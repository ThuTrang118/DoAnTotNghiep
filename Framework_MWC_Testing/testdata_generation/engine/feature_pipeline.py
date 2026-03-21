from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from testdata_generation.engine.feature_mappers import GROUP_TO_FINAL_MAPPERS


@dataclass
class GenerationResult:
    ok: bool
    feature: str
    raw_text_path: Optional[Path]
    raw_path: Optional[Path]
    processed_paths: Dict[str, Path]
    rows: List[Dict[str, Any]]
    warnings: List[str]
    errors: List[str]


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def build_prompt_from_files(project_root: Path, relative_paths: List[str]) -> str:
    parts: List[str] = []

    for rel in relative_paths:
        p = project_root / rel
        if not p.exists():
            continue
        parts.append(p.read_text(encoding="utf-8").strip())

    final_prompt = "\n\n".join([p for p in parts if p])
    return final_prompt.replace("{app_context}", "")


def print_result_summary(title: str, feature: str, result: Any) -> None:
    print("=" * 60)
    print(title)
    print(f"Feature: {feature}")
    print(f"OK: {result.ok}")
    print(f"Raw text: {result.raw_text_path}")
    print(f"Raw json:  {result.raw_path}")

    if result.processed_paths:
        print("Processed files:")
        for k, v in sorted(result.processed_paths.items()):
            print(f"  - {k}: {v}")
    else:
        print("Processed files: (none)")

    if result.warnings:
        print("Warnings:")
        for w in result.warnings:
            print(f"  - {w}")

    if result.errors:
        print("Errors:")
        for e in result.errors:
            print(f"  - {e}")

    print(f"Total rows: {len(result.rows)}")


def print_plan_summary_from_payload(feature: str, payload: Dict[str, Any]) -> None:
    plan = payload.get("plan", {}) if isinstance(payload, dict) else {}

    dt = plan.get("DecisionTable", "?")
    ep = plan.get("EquivalencePartitioning", "?")
    bva = plan.get("BoundaryValueAnalysis", "?")
    final_n = plan.get("FinalOptimized", "?")

    print()
    print("=" * 56)
    print("TESTCASE COVERAGE PLAN")
    print(f"Feature                : {feature}")
    print("-" * 56)
    print(f"Decision Table         : {dt}")
    print(f"Equivalence Partition  : {ep}")
    print(f"Boundary Value Analysis: {bva}")
    print("-" * 56)
    print(f"Final Optimized        : {final_n}")
    print("=" * 56)
    print()


def write_final_outputs(
    gen: Any,
    feature: str,
    payload: Dict[str, Any],
    formats: Optional[List[str]],
    want_export: bool,
    pre_warnings: Optional[List[str]] = None,
) -> GenerationResult:
    rows = payload.get("items", []) if isinstance(payload, dict) else []
    validator_result = gen.validator.validate(feature=feature, data={"items": rows})

    warnings = list(pre_warnings or []) + list(validator_result.warnings or [])
    errors = list(validator_result.errors or [])

    cleaned_payload = (
        validator_result.data
        if isinstance(validator_result.data, dict)
        else {"items": rows}
    )
    cleaned_rows = cleaned_payload.get("items", rows)

    raw_text = json.dumps(cleaned_payload, ensure_ascii=False, indent=2)
    raw_text_path = gen.writer.write_raw_text(feature, raw_text)
    raw_json_path = gen.writer.write_raw_json(feature, cleaned_payload)

    processed_paths: Dict[str, Path] = {}
    if want_export:
        processed_paths = gen.writer.write_formats(
            feature=feature,
            rows=cleaned_rows,
            formats=None if formats is None else list(formats),
            yaml_ext="yaml",
        )

    ok = len(cleaned_rows) > 0 and len(errors) == 0

    return GenerationResult(
        ok=ok,
        feature=feature,
        raw_text_path=raw_text_path,
        raw_path=raw_json_path,
        processed_paths=processed_paths,
        rows=cleaned_rows,
        warnings=warnings,
        errors=errors,
    )


def _normalize_text_for_compare(text: str) -> str:
    return (text or "").strip().replace("\r\n", "\n")


def _validate_plan_rules(plan: Dict[str, int]) -> None:
    dt = int(plan.get("DecisionTable", 0))
    ep = int(plan.get("EquivalencePartitioning", 0))
    bva = int(plan.get("BoundaryValueAnalysis", 0))
    final_n = int(plan.get("FinalOptimized", 0))

    values = {
        "DecisionTable": dt,
        "EquivalencePartitioning": ep,
        "BoundaryValueAnalysis": bva,
        "FinalOptimized": final_n,
    }

    negatives = [k for k, v in values.items() if v < 0]
    if negatives:
        raise ValueError(f"Invalid plan: negative values are not allowed: {negatives}")

    if final_n <= 0:
        raise ValueError("Invalid plan: FinalOptimized must be > 0")

    if final_n < dt:
        raise ValueError(
            f"Invalid plan: FinalOptimized={final_n} must be >= DecisionTable={dt}"
        )

    if dt < 7:
        raise ValueError(f"Invalid plan: DecisionTable={dt} must be >= 7 for login")


def _parse_plan_payload(gen: Any, raw_text: str) -> Tuple[Dict[str, Any], List[str]]:
    parsed = gen.parser.parse_json(raw_text)
    if not parsed.ok or parsed.data is None:
        raise ValueError(parsed.error or "Cannot parse JSON plan from LLM output")

    warnings: List[str] = []
    raw_norm = _normalize_text_for_compare(raw_text)
    cleaned_norm = _normalize_text_for_compare(parsed.cleaned_text)

    if cleaned_norm and cleaned_norm != raw_norm:
        warnings.append(
            "Plan step output was repaired/salvaged by parser; inspect login_plan_raw.txt carefully."
        )

    payload = parsed.data
    if not isinstance(payload, dict):
        raise ValueError("Plan payload must be a JSON object")

    plan = payload.get("plan")
    if not isinstance(plan, dict):
        raise ValueError("Plan payload must contain key 'plan' as an object")

    required_keys = {
        "DecisionTable",
        "EquivalencePartitioning",
        "BoundaryValueAnalysis",
        "FinalOptimized",
    }
    missing = [k for k in required_keys if k not in plan]
    if missing:
        raise ValueError(f"Plan payload missing keys: {missing}")

    try:
        cleaned_plan = {
            "DecisionTable": int(plan.get("DecisionTable", 0)),
            "EquivalencePartitioning": int(plan.get("EquivalencePartitioning", 0)),
            "BoundaryValueAnalysis": int(plan.get("BoundaryValueAnalysis", 0)),
            "FinalOptimized": int(plan.get("FinalOptimized", 0)),
        }
    except Exception as e:
        raise ValueError(f"Plan payload contains non-integer values: {e}") from e

    _validate_plan_rules(cleaned_plan)
    return {"plan": cleaned_plan}, warnings


def _parse_items_payload(gen: Any, raw_text: str) -> Tuple[Dict[str, Any], List[str]]:
    parsed = gen.parser.parse_json(raw_text)
    if not parsed.ok or parsed.data is None:
        raise ValueError(parsed.error or "Cannot parse JSON items from LLM output")

    warnings: List[str] = []
    raw_norm = _normalize_text_for_compare(raw_text)
    cleaned_norm = _normalize_text_for_compare(parsed.cleaned_text)

    if cleaned_norm and cleaned_norm != raw_norm:
        warnings.append(
            "Items step output was repaired/salvaged by parser; raw output may be truncated or malformed."
        )

    payload = parsed.data
    if not isinstance(payload, dict):
        raise ValueError("Items payload must be a JSON object")

    items = payload.get("items")
    if not isinstance(items, list):
        raise ValueError("Items payload must contain key 'items' as a list")

    rows: List[Dict[str, Any]] = [it for it in items if isinstance(it, dict)]

    non_dict_count = len(items) - len(rows)
    if non_dict_count > 0:
        warnings.append(
            f"Items payload contains {non_dict_count} non-object element(s); they will be dropped by validation."
        )

    return {"items": rows}, warnings


def _build_items_prompt_with_locked_plan(base_prompt: str, plan_payload: Dict[str, Any]) -> str:
    plan_json = json.dumps(plan_payload, ensure_ascii=False, indent=2)
    placeholder = "{locked_plan_json}"

    if placeholder in base_prompt:
        return base_prompt.replace(placeholder, plan_json)

    return (
        base_prompt.strip()
        + "\n\n==================================================\n"
        + "LOCKED PLAN JSON\n"
        + "==================================================\n\n"
        + plan_json
        + "\n"
    )


def _build_retry_items_prompt(
    base_prompt: str,
    plan_payload: Dict[str, Any],
    previous_items_payload: Dict[str, Any],
    reason: str,
) -> str:
    plan_json = json.dumps(plan_payload, ensure_ascii=False, indent=2)
    prev_json = json.dumps(previous_items_payload, ensure_ascii=False, indent=2)

    return (
        base_prompt.strip()
        + "\n\n==================================================\n"
        + "LOCKED PLAN JSON\n"
        + "==================================================\n\n"
        + plan_json
        + "\n\n==================================================\n"
        + "PREVIOUS INVALID OUTPUT\n"
        + "==================================================\n\n"
        + prev_json
        + "\n\n==================================================\n"
        + "CORRECTION INSTRUCTION\n"
        + "==================================================\n"
        + f"- Output trước bị lỗi vì: {reason}\n"
        + "- Hãy tạo LẠI TOÀN BỘ mảng items từ đầu.\n"
        + "- KHÔNG được chỉ bổ sung thêm item.\n"
        + "- KHÔNG được lặp lại item cũ nếu không tăng coverage.\n"
        + "- Số phần tử trong items phải bằng đúng plan.FinalOptimized.\n"
        + "- Ưu tiên giữ đủ 7 nhóm Decision Table lõi trước, sau đó đến EP, sau đó đến BVA.\n"
        + "- Nếu plan khó materialize, phải chọn các boundary rõ ràng nhất thay vì tạo nhóm mơ hồ.\n"
        + "- Chỉ trả về JSON object có key duy nhất là items.\n"
    )


def _validate_items_against_plan(
    gen: Any,
    items_payload: Dict[str, Any],
    expected_final: int,
) -> Tuple[List[Dict[str, Any]], List[str], List[str]]:
    validator_result = gen.validator.validate(feature="login_groups", data=items_payload)
    groups_rows = (
        validator_result.data.get("items", [])
        if isinstance(validator_result.data, dict)
        else []
    )

    warnings = list(validator_result.warnings or [])
    errors = list(validator_result.errors or [])

    actual_final = len(groups_rows)
    if actual_final < expected_final:
        errors.append(
            f"Plan/items mismatch: plan.FinalOptimized={expected_final} but actual items={actual_final}"
        )
        warnings.append(
            "Actual items < plan.FinalOptimized. This often means the LLM output was truncated, malformed, or under-generated."
        )
    elif actual_final > expected_final:
        errors.append(
            f"Plan/items mismatch: plan.FinalOptimized={expected_final} but actual items={actual_final}"
        )
        warnings.append(
            "Actual items > plan.FinalOptimized. The LLM ignored the locked plan."
        )

    return groups_rows, warnings, errors


def run_single_step(
    gen: Any,
    root: Path,
    feature: str,
    prompt_file: str,
    formats: Optional[List[str]],
    want_export: bool,
) -> GenerationResult:
    prompt = build_prompt_from_files(
        root,
        [
            "testdata_generation/engine/blackbox_techniques.txt",
            f"testdata_generation/input/{prompt_file}",
        ],
    )

    return gen.generate(
        feature=feature,
        prompt=prompt,
        system=None,
        formats=(formats if want_export else []),
        yaml_ext="yaml",
        llm_kwargs={"json_mode": True},
        save_raw_json=True,
    )


def run_two_step(
    gen: Any,
    root: Path,
    final_feature: str,
    groups_feature: str,
    prompt_file: str,
    mapper_name: str,
    formats: Optional[List[str]],
    want_export: bool,
) -> GenerationResult:
    prompt = build_prompt_from_files(
        root,
        [
            "testdata_generation/engine/blackbox_techniques.txt",
            f"testdata_generation/input/{prompt_file}",
        ],
    )

    groups_result = gen.generate(
        feature=groups_feature,
        prompt=prompt,
        system=None,
        formats=[],
        yaml_ext="yaml",
        llm_kwargs={"json_mode": True},
        save_raw_json=False,
    )

    print_result_summary(f"STEP 1 - {groups_feature.upper()}", groups_feature, groups_result)

    if not groups_result.ok:
        return groups_result

    mapper = GROUP_TO_FINAL_MAPPERS.get(mapper_name)
    if mapper is None:
        return GenerationResult(
            ok=False,
            feature=final_feature,
            raw_text_path=None,
            raw_path=None,
            processed_paths={},
            rows=[],
            warnings=[],
            errors=[f"Mapper not found for feature: {mapper_name}"],
        )

    mapped_payload, pre_warnings = mapper(groups_result.rows)

    return write_final_outputs(
        gen=gen,
        feature=final_feature,
        payload=mapped_payload,
        formats=formats,
        want_export=want_export,
        pre_warnings=pre_warnings,
    )


def run_login_plan_first(
    gen: Any,
    root: Path,
    formats: Optional[List[str]],
    want_export: bool,
    plan_prompt_file: str,
    items_prompt_file: str,
) -> GenerationResult:
    print("[LOGIN] Plan-first mode: generate plan first, then generate items from locked plan...")

    # ==================================================
    # STEP 1 - PLAN ONLY
    # Chỉ lưu TXT: login_plan_raw.txt
    # ==================================================
    plan_prompt = build_prompt_from_files(
        root,
        [
            "testdata_generation/engine/blackbox_techniques.txt",
            f"testdata_generation/input/{plan_prompt_file}",
        ],
    )

    try:
        raw_plan_text = gen.client.generate_text(
            prompt=plan_prompt,
            system=None,
            json_mode=True,
        )
    except Exception as e:
        return GenerationResult(
            ok=False,
            feature="login_plan",
            raw_text_path=None,
            raw_path=None,
            processed_paths={},
            rows=[],
            warnings=[],
            errors=[f"LLM call failed at plan step: {e}"],
        )

    plan_raw_text_path = gen.writer.write_raw_text("login_plan", raw_plan_text)

    try:
        plan_payload, plan_parse_warnings = _parse_plan_payload(gen, raw_plan_text)
    except Exception as e:
        return GenerationResult(
            ok=False,
            feature="login_plan",
            raw_text_path=plan_raw_text_path,
            raw_path=None,
            processed_paths={},
            rows=[],
            warnings=[],
            errors=[str(e)],
        )

    plan_result = GenerationResult(
        ok=True,
        feature="login_plan",
        raw_text_path=plan_raw_text_path,
        raw_path=None,
        processed_paths={},
        rows=[],
        warnings=plan_parse_warnings,
        errors=[],
    )

    print_result_summary("STEP 1 - LOGIN PLAN", "login_plan", plan_result)
    print_plan_summary_from_payload("login", plan_payload)

    # ==================================================
    # STEP 2 - ITEMS FROM LOCKED PLAN
    # Chỉ lưu TXT: login_items_raw.txt, login_items_retry_raw.txt
    # ==================================================
    items_prompt_base = build_prompt_from_files(
        root,
        [
            "testdata_generation/engine/blackbox_techniques.txt",
            f"testdata_generation/input/{items_prompt_file}",
        ],
    )
    items_prompt = _build_items_prompt_with_locked_plan(items_prompt_base, plan_payload)

    def call_items(prompt_text: str, raw_name: str) -> Tuple[str, Path]:
        raw_text = gen.client.generate_text(
            prompt=prompt_text,
            system=None,
            json_mode=True,
        )
        raw_path = gen.writer.write_raw_text(raw_name, raw_text)
        return raw_text, raw_path

    try:
        raw_items_text, items_raw_text_path = call_items(items_prompt, "login_items")
    except Exception as e:
        return GenerationResult(
            ok=False,
            feature="login",
            raw_text_path=plan_raw_text_path,
            raw_path=None,
            processed_paths={},
            rows=[],
            warnings=[
                f"Plan raw text saved at: {plan_raw_text_path}",
            ] + plan_parse_warnings,
            errors=[f"LLM call failed at items step: {e}"],
        )

    try:
        items_payload, items_parse_warnings = _parse_items_payload(gen, raw_items_text)
    except Exception as e:
        return GenerationResult(
            ok=False,
            feature="login",
            raw_text_path=items_raw_text_path,
            raw_path=None,
            processed_paths={},
            rows=[],
            warnings=[
                f"Plan raw text saved at: {plan_raw_text_path}",
                f"Items raw text saved at: {items_raw_text_path}",
            ] + plan_parse_warnings,
            errors=[str(e)],
        )

    expected_final = int(plan_payload.get("plan", {}).get("FinalOptimized", 0))
    groups_rows, v_warnings, v_errors = _validate_items_against_plan(
        gen=gen,
        items_payload=items_payload,
        expected_final=expected_final,
    )

    warnings = list(plan_parse_warnings) + list(items_parse_warnings) + list(v_warnings)
    errors = list(v_errors)

    # Retry 1 lần nếu mismatch
    if errors:
        retry_reason = "; ".join(errors)
        retry_prompt = _build_retry_items_prompt(
            base_prompt=items_prompt_base,
            plan_payload=plan_payload,
            previous_items_payload=items_payload,
            reason=retry_reason,
        )

        try:
            retry_raw_text, retry_raw_text_path = call_items(retry_prompt, "login_items_retry")
            retry_payload, retry_parse_warnings = _parse_items_payload(gen, retry_raw_text)

            retry_groups_rows, retry_v_warnings, retry_v_errors = _validate_items_against_plan(
                gen=gen,
                items_payload=retry_payload,
                expected_final=expected_final,
            )

            if not retry_v_errors:
                mapper = GROUP_TO_FINAL_MAPPERS.get("login")
                if mapper is None:
                    return GenerationResult(
                        ok=False,
                        feature="login",
                        raw_text_path=retry_raw_text_path,
                        raw_path=None,
                        processed_paths={},
                        rows=[],
                        warnings=warnings + retry_parse_warnings + retry_v_warnings,
                        errors=["Mapper not found for feature: login"],
                    )

                mapped_payload, pre_warnings = mapper(retry_groups_rows)
                return write_final_outputs(
                    gen=gen,
                    feature="login",
                    payload=mapped_payload,
                    formats=formats,
                    want_export=want_export,
                    pre_warnings=pre_warnings
                    + warnings
                    + retry_parse_warnings
                    + retry_v_warnings
                    + [
                        f"Plan raw text saved at: {plan_raw_text_path}",
                        f"Items raw text saved at: {items_raw_text_path}",
                        f"Retry items raw text saved at: {retry_raw_text_path}",
                    ],
                )

            warnings.extend(
                retry_parse_warnings
                + retry_v_warnings
                + [
                    "Retry at items step was attempted but still invalid.",
                    f"Retry items raw text saved at: {retry_raw_text_path}",
                ]
            )
            errors = retry_v_errors

        except Exception as retry_exc:
            warnings.append(f"Retry at items step failed: {retry_exc}")

    if errors:
        return GenerationResult(
            ok=False,
            feature="login",
            raw_text_path=items_raw_text_path,
            raw_path=None,
            processed_paths={},
            rows=groups_rows,
            warnings=[
                f"Plan raw text saved at: {plan_raw_text_path}",
                f"Items raw text saved at: {items_raw_text_path}",
            ] + warnings,
            errors=errors,
        )

    mapper = GROUP_TO_FINAL_MAPPERS.get("login")
    if mapper is None:
        return GenerationResult(
            ok=False,
            feature="login",
            raw_text_path=items_raw_text_path,
            raw_path=None,
            processed_paths={},
            rows=[],
            warnings=[
                f"Plan raw text saved at: {plan_raw_text_path}",
                f"Items raw text saved at: {items_raw_text_path}",
            ] + warnings,
            errors=["Mapper not found for feature: login"],
        )

    mapped_payload, pre_warnings = mapper(groups_rows)
    final_result = write_final_outputs(
        gen=gen,
        feature="login",
        payload=mapped_payload,
        formats=formats,
        want_export=want_export,
        pre_warnings=pre_warnings
        + warnings
        + [
            f"Plan raw text saved at: {plan_raw_text_path}",
            f"Items raw text saved at: {items_raw_text_path}",
        ],
    )

    return final_result