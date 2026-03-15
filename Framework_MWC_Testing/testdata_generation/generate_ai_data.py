from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from testdata_generation.engine.generator import AITestDataGenerator, GenerationResult
from testdata_generation.engine.llm_client import OllamaClient

# =========================
# LOGIN RULES
# =========================
LOGIN_SEED_USERNAME = "AnhDuong11"
LOGIN_SEED_PASSWORD = "anhduong@123"
LOGIN_EXPECTED_MISSING = "Vui lòng điền vào trường này"
LOGIN_EXPECTED_WRONG = "Tên đăng nhập hoặc mật khẩu không đúng!"

# =========================
# REGISTER RULES
# =========================
REGISTER_EXISTING_USERNAME = "AnhDuong11"
REGISTER_EXPECTED_REQUIRED = "Vui lòng điền vào trường này."
REGISTER_EXPECTED_PHONE = "Số điện thoại không đúng định dạng!"
REGISTER_EXPECTED_PASSWORD = "Mật khẩu phải lớn hơn 8 ký tự và nhỏ hơn 20 ký tự!"
REGISTER_EXPECTED_CONFIRM = "Mật khẩu không giống nhau"
REGISTER_EXPECTED_DUPLICATE = "Tài khoản đã tồn tại trong hệ thống"


def load_app_config(project_root: Path) -> Dict[str, Any]:
    p = project_root / "app_config.yaml"
    if not p.exists():
        return {}
    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}


def parse_formats(value: Optional[str]) -> Tuple[Optional[List[str]], bool]:
    """
    - omit --formats => ONLY RAW
    - --formats all => export ALL
    - --formats csv,json => export those
    """
    if value is None:
        return ([], False)

    v = value.strip().lower()
    if not v:
        return ([], False)

    if v == "all":
        return (None, True)

    parts = [x.strip().lower() for x in v.split(",") if x.strip()]
    return (parts or [], True)


def read_text_file(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Prompt file not found: {path}")
    return path.read_text(encoding="utf-8").strip()


def build_prompt_from_files(project_root: Path, relative_files: List[str]) -> str:
    parts: List[str] = []
    for rel in relative_files:
        parts.append(read_text_file(project_root / rel))

    final_prompt = "\n\n".join(parts)
    return final_prompt.replace("{app_context}", "")


def build_prompt(project_root: Path, feature: str) -> str:
    return build_prompt_from_files(
        project_root,
        [
            "testdata_generation/engine/blackbox_techniques.txt",
            f"testdata_generation/input/{feature}.txt",
        ],
    )


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


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


def generate_single_step(
    gen: AITestDataGenerator,
    root: Path,
    feature: str,
    formats: Optional[List[str]],
    want_export: bool,
) -> Any:
    prompt = build_prompt(root, feature)

    return gen.generate(
        feature=feature,
        prompt=prompt,
        system=None,
        formats=(formats if want_export else []),
        yaml_ext="yaml",
        llm_kwargs={"json_mode": True},
        save_raw_json=True,
    )


def _safe_str(value: Any) -> str:
    return "" if value is None else str(value)


# =========================================================
# LOGIN: groups -> final
# =========================================================
def _group_id_to_login_testcase(group_id: str, index: int) -> str:
    gid = (group_id or "").strip().upper()
    if gid.startswith("LGG") and gid[3:].isdigit():
        return f"LG{int(gid[3:]):02d}"
    return f"LG{index:02d}"


def _infer_login_expected(username: str, password: str) -> str:
    if username == "" or password == "":
        return LOGIN_EXPECTED_MISSING
    if username == LOGIN_SEED_USERNAME and password == LOGIN_SEED_PASSWORD:
        return LOGIN_SEED_USERNAME
    return LOGIN_EXPECTED_WRONG


def build_login_payload_from_groups(groups_rows: List[Dict[str, Any]]) -> Tuple[Dict[str, Any], List[str]]:
    items: List[Dict[str, str]] = []
    warnings: List[str] = []

    for idx, row in enumerate(groups_rows, start=1):
        if not isinstance(row, dict):
            warnings.append(f"Skipped non-object login group at source index {idx}")
            continue

        username = _safe_str(row.get("UsernamePattern", ""))
        password = _safe_str(row.get("PasswordPattern", ""))
        expected_old = _safe_str(row.get("Expected", ""))
        expected_new = _infer_login_expected(username, password)

        if expected_old != expected_new:
            warnings.append(
                f"Adjusted login Expected from '{expected_old}' to '{expected_new}' for source index {idx}"
            )

        items.append(
            {
                "Testcase": _group_id_to_login_testcase(_safe_str(row.get("GroupID", "")), idx),
                "Username": username,
                "Password": password,
                "Expected": expected_new,
            }
        )

    for idx, item in enumerate(items, start=1):
        item["Testcase"] = f"LG{idx:02d}"

    return {"items": items}, warnings


def write_login_outputs_from_groups(
    gen: AITestDataGenerator,
    groups_rows: List[Dict[str, Any]],
    formats: Optional[List[str]],
    want_export: bool,
) -> GenerationResult:
    final_payload, pre_warnings = build_login_payload_from_groups(groups_rows)

    raw_text = json.dumps(final_payload, ensure_ascii=False, indent=2)
    raw_text_path = gen.writer.write_raw_text("login", raw_text)
    raw_json_path = gen.writer.write_raw_json("login", final_payload)

    rows = final_payload.get("items", [])
    v = gen.validator.validate(feature="login", data={"items": rows})

    warnings = list(pre_warnings) + list(v.warnings or [])
    errors = list(v.errors or [])
    cleaned_payload = v.data if isinstance(v.data, dict) else {"items": rows}
    cleaned_rows = cleaned_payload.get("items", rows)

    processed_paths: Dict[str, Path] = {}
    if want_export:
        processed_paths = gen.writer.write_formats(
            feature="login",
            rows=cleaned_rows,
            formats=None if formats is None else list(formats),
            yaml_ext="yaml",
        )

    ok = len(cleaned_rows) > 0 and len(errors) == 0

    return GenerationResult(
        ok=ok,
        feature="login",
        raw_text_path=raw_text_path,
        raw_path=raw_json_path,
        processed_paths=processed_paths,
        rows=cleaned_rows,
        warnings=warnings,
        errors=errors,
    )


def generate_login_single_prompt(
    gen: AITestDataGenerator,
    root: Path,
    formats: Optional[List[str]],
    want_export: bool,
) -> Any:
    print("[LOGIN] Single-prompt mode: generate behavior groups, then convert to final login data...")

    prompt = build_prompt_from_files(
        root,
        [
            "testdata_generation/engine/blackbox_techniques.txt",
            "testdata_generation/input/login.txt",
        ],
    )

    groups_result = gen.generate(
        feature="login_groups",
        prompt=prompt,
        system=None,
        formats=[],
        yaml_ext="yaml",
        llm_kwargs={"json_mode": True},
        save_raw_json=False,  # CHỈ sinh login_groups_raw.txt
    )

    print_result_summary("STEP 1 - LOGIN CLASSIFIED GROUPS", "login_groups", groups_result)

    if not groups_result.ok:
        return groups_result

    return write_login_outputs_from_groups(
        gen=gen,
        groups_rows=groups_result.rows,
        formats=formats,
        want_export=want_export,
    )


# =========================================================
# REGISTER: groups -> final
# =========================================================
def _group_id_to_register_testcase(group_id: str, index: int) -> str:
    gid = (group_id or "").strip().upper()
    if gid.startswith("RGG") and gid[3:].isdigit():
        return f"DK{int(gid[3:]):02d}"
    return f"DK{index:02d}"


def _is_phone_invalid(phone: str) -> bool:
    if phone == "":
        return False
    if len(phone) != 10:
        return True
    if not phone.startswith("0"):
        return True
    if not phone.isdigit():
        return True
    if " " in phone:
        return True
    return False


def _is_password_length_invalid(password: str) -> bool:
    if password == "":
        return False
    return not (len(password) > 8 and len(password) < 20)


def _infer_register_expected(username: str, phone: str, password: str, confirm: str) -> str:
    if username == "" or phone == "" or password == "" or confirm == "":
        return REGISTER_EXPECTED_REQUIRED
    if username == REGISTER_EXISTING_USERNAME:
        return REGISTER_EXPECTED_DUPLICATE
    if _is_phone_invalid(phone):
        return REGISTER_EXPECTED_PHONE
    if _is_password_length_invalid(password):
        return REGISTER_EXPECTED_PASSWORD
    if confirm != password:
        return REGISTER_EXPECTED_CONFIRM
    return username


def build_register_payload_from_groups(groups_rows: List[Dict[str, Any]]) -> Tuple[Dict[str, Any], List[str]]:
    items: List[Dict[str, str]] = []
    warnings: List[str] = []

    for idx, row in enumerate(groups_rows, start=1):
        if not isinstance(row, dict):
            warnings.append(f"Skipped non-object register group at source index {idx}")
            continue

        username = _safe_str(row.get("UsernamePattern", ""))
        phone = _safe_str(row.get("PhonePattern", ""))
        password = _safe_str(row.get("PasswordPattern", ""))
        confirm = _safe_str(row.get("ConfirmPasswordPattern", ""))
        expected_old = _safe_str(row.get("Expected", ""))
        expected_new = _infer_register_expected(username, phone, password, confirm)

        if expected_old != expected_new:
            warnings.append(
                f"Adjusted register Expected from '{expected_old}' to '{expected_new}' for source index {idx}"
            )

        items.append(
            {
                "Testcase": _group_id_to_register_testcase(_safe_str(row.get("GroupID", "")), idx),
                "Username": username,
                "Phone": phone,
                "Password": password,
                "ConfirmPassword": confirm,
                "Expected": expected_new,
            }
        )

    for idx, item in enumerate(items, start=1):
        item["Testcase"] = f"DK{idx:02d}"

    return {"items": items}, warnings


def write_register_outputs_from_groups(
    gen: AITestDataGenerator,
    groups_rows: List[Dict[str, Any]],
    formats: Optional[List[str]],
    want_export: bool,
) -> GenerationResult:
    final_payload, pre_warnings = build_register_payload_from_groups(groups_rows)

    raw_text = json.dumps(final_payload, ensure_ascii=False, indent=2)
    raw_text_path = gen.writer.write_raw_text("register", raw_text)
    raw_json_path = gen.writer.write_raw_json("register", final_payload)

    rows = final_payload.get("items", [])
    v = gen.validator.validate(feature="register", data={"items": rows})

    warnings = list(pre_warnings) + list(v.warnings or [])
    errors = list(v.errors or [])
    cleaned_payload = v.data if isinstance(v.data, dict) else {"items": rows}
    cleaned_rows = cleaned_payload.get("items", rows)

    processed_paths: Dict[str, Path] = {}
    if want_export:
        processed_paths = gen.writer.write_formats(
            feature="register",
            rows=cleaned_rows,
            formats=None if formats is None else list(formats),
            yaml_ext="yaml",
        )

    ok = len(cleaned_rows) > 0 and len(errors) == 0

    return GenerationResult(
        ok=ok,
        feature="register",
        raw_text_path=raw_text_path,
        raw_path=raw_json_path,
        processed_paths=processed_paths,
        rows=cleaned_rows,
        warnings=warnings,
        errors=errors,
    )


def generate_register_single_prompt(
    gen: AITestDataGenerator,
    root: Path,
    formats: Optional[List[str]],
    want_export: bool,
) -> Any:
    print("[REGISTER] Single-prompt mode: generate behavior groups, then convert to final register data...")

    prompt = build_prompt_from_files(
        root,
        [
            "testdata_generation/engine/blackbox_techniques.txt",
            "testdata_generation/input/register.txt",
        ],
    )

    groups_result = gen.generate(
        feature="register_groups",
        prompt=prompt,
        system=None,
        formats=[],
        yaml_ext="yaml",
        llm_kwargs={"json_mode": True},
        save_raw_json=False,  # CHỈ sinh register_groups_raw.txt
    )

    print_result_summary("STEP 1 - REGISTER CLASSIFIED GROUPS", "register_groups", groups_result)

    if not groups_result.ok:
        return groups_result

    return write_register_outputs_from_groups(
        gen=gen,
        groups_rows=groups_result.rows,
        formats=formats,
        want_export=want_export,
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--feature", required=True)
    ap.add_argument("--formats", default=None, help="all OR csv,json,xlsx... (omit => ONLY RAW)")
    ap.add_argument("--base-url", default=None)
    ap.add_argument("--model", default=None)
    ap.add_argument("--timeout-sec", type=int, default=None)
    ap.add_argument("--endpoint-mode", default=None, choices=["auto", "generate", "chat"])
    ap.add_argument("--temperature", type=float, default=None)
    ap.add_argument("--top-p", type=float, default=None)
    ap.add_argument("--num-predict", type=int, default=None)
    ap.add_argument("--seed", type=int, default=None)

    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    cfg = load_app_config(root)

    ai_cfg = cfg.get("ai", {}) if isinstance(cfg.get("ai", {}), dict) else {}
    ollama_cfg = ai_cfg.get("ollama", {}) if isinstance(ai_cfg.get("ollama", {}), dict) else {}

    base_url = args.base_url or ollama_cfg.get("base_url") or "http://localhost:11434"
    model = args.model or ollama_cfg.get("model") or "deepseek-r1:8b"
    timeout_sec = args.timeout_sec if args.timeout_sec is not None else int(ollama_cfg.get("timeout_sec", 900))
    endpoint_mode = args.endpoint_mode or ollama_cfg.get("endpoint_mode") or "auto"
    temperature = args.temperature if args.temperature is not None else float(ollama_cfg.get("temperature", 0.2))
    top_p = args.top_p if args.top_p is not None else float(ollama_cfg.get("top_p", 0.8))
    num_predict = args.num_predict if args.num_predict is not None else int(ollama_cfg.get("num_predict", 2000))
    seed = args.seed if args.seed is not None else ollama_cfg.get("seed", None)

    client = OllamaClient(
        base_url=str(base_url),
        model=str(model),
        timeout_sec=int(timeout_sec),
        endpoint_mode=str(endpoint_mode),
        temperature=float(temperature),
        top_p=float(top_p),
        num_predict=int(num_predict),
        seed=None if seed is None else int(seed),
        json_mode=True,
    )

    feature = args.feature.strip().lower()
    formats, want_export = parse_formats(args.formats)

    raw_dir = root / "testdata_generation" / "output"
    processed_dir = root / "data" / "ai_processed"

    ensure_dir(raw_dir)
    ensure_dir(processed_dir)

    gen = AITestDataGenerator(
        client=client,
        project_root=root,
        raw_evidence_dir=raw_dir,
        processed_dir=processed_dir,
    )

    if feature == "login":
        result = generate_login_single_prompt(
            gen=gen,
            root=root,
            formats=formats,
            want_export=want_export,
        )
    elif feature == "register":
        result = generate_register_single_prompt(
            gen=gen,
            root=root,
            formats=formats,
            want_export=want_export,
        )
    else:
        result = generate_single_step(
            gen=gen,
            root=root,
            feature=feature,
            formats=formats,
            want_export=want_export,
        )

    print_result_summary("FINAL RESULT", feature, result)
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())