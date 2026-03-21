from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from testdata_generation.engine.feature_pipeline import (
    ensure_dir,
    print_result_summary,
    run_login_plan_first,
    run_single_step,
    run_two_step,
)
from testdata_generation.engine.generator import AITestDataGenerator
from testdata_generation.engine.llm_client import OllamaClient


PIPELINE_CONFIG: Dict[str, Dict[str, Any]] = {
    "login": {
        "mode": "plan_first",
        "plan_prompt_file": "login_plan.txt",
        "items_prompt_file": "login_items_from_plan.txt",
    },
    "register": {
        "mode": "two_step",
        "groups_feature": "register_groups",
        "mapper": "register",
        "prompt_file": "register.txt",
    },
    "search": {
        "mode": "single_step",
        "prompt_file": "search.txt",
    },
    "order": {
        "mode": "single_step",
        "prompt_file": "order.txt",
    },
    "profile": {
        "mode": "single_step",
        "prompt_file": "profile.txt",
    },
}


def load_app_config(project_root: Path) -> Dict[str, Any]:
    config_path = project_root / "app_config.yaml"
    if not config_path.exists():
        return {}

    with config_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    return data if isinstance(data, dict) else {}


def parse_formats(formats_arg: Optional[str]) -> Tuple[Optional[List[str]], bool]:
    if formats_arg is None:
        return None, False

    text = str(formats_arg).strip()
    if not text:
        return None, False

    if text.lower() == "all":
        return None, True

    parts = [x.strip().lower() for x in text.split(",") if x.strip()]
    return parts, True


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

    feature_cfg = PIPELINE_CONFIG.get(feature)
    if not feature_cfg:
        supported = ", ".join(sorted(PIPELINE_CONFIG.keys()))
        raise SystemExit(f"Unsupported feature: {feature}. Supported: {supported}")

    mode = str(feature_cfg.get("mode", "single_step")).strip().lower()

    if mode == "plan_first":
        result = run_login_plan_first(
            gen=gen,
            root=root,
            formats=formats,
            want_export=want_export,
            plan_prompt_file=str(feature_cfg["plan_prompt_file"]),
            items_prompt_file=str(feature_cfg["items_prompt_file"]),
        )
    elif mode == "two_step":
        result = run_two_step(
            gen=gen,
            root=root,
            final_feature=feature,
            groups_feature=str(feature_cfg["groups_feature"]),
            prompt_file=str(feature_cfg["prompt_file"]),
            mapper_name=str(feature_cfg["mapper"]),
            formats=formats,
            want_export=want_export,
        )
    else:
        result = run_single_step(
            gen=gen,
            root=root,
            feature=feature,
            prompt_file=str(feature_cfg["prompt_file"]),
            formats=formats,
            want_export=want_export,
        )

    print_result_summary("FINAL RESULT", feature, result)
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())