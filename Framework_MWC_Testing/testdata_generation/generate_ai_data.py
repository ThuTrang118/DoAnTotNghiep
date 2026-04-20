from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

import yaml

from testdata_generation.engine.generation_pipeline import GenerationPipeline
from testdata_generation.engine.llm_client import OllamaLLMClient
from testdata_generation.engine.prompt_loader import PromptLoader
from testdata_generation.engine.feature_item_schema import normalize_feature_name


BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
CONFIG_PATH = PROJECT_ROOT / "app_config.yaml"

SUPPORTED_FORMATS = ["csv", "json", "xlsx", "xls", "yaml", "yml", "xml", "db"]


def _log(message: str) -> None:
    now = time.strftime("%H:%M:%S")
    print(f"[{now}] {message}", flush=True)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m testdata_generation.generate_ai_data",
        description="Generate AI test data with 2-step pipeline: Step1 coverage (EP+BVA) -> Step2 final testcases (Decision Table).",
    )
    parser.add_argument(
        "--feature",
        required=True,
        help="Feature name. Example: login, register, search",
    )
    parser.add_argument(
        "--formats",
        nargs="+",
        default=["json"],
        help="Example: --formats json csv xlsx OR --formats all",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Reduce pipeline logs.",
    )
    return parser.parse_args()


def _normalize_formats(values: List[str]) -> List[str]:
    raw = [str(v).strip().lower() for v in values if str(v).strip()]
    if not raw:
        return ["json"]

    if "all" in raw:
        return list(SUPPORTED_FORMATS)

    seen: List[str] = []
    for fmt in raw:
        if fmt not in SUPPORTED_FORMATS:
            raise ValueError(
                f"Unsupported format: '{fmt}'. Supported={SUPPORTED_FORMATS} or 'all'."
            )
        if fmt not in seen:
            seen.append(fmt)
    return seen


def _validate_feature_arg(feature: str) -> str:
    if not isinstance(feature, str) or not feature.strip():
        raise ValueError("Feature must not be empty.")
    return normalize_feature_name(feature)


def _load_config() -> Dict[str, Any]:
    if not CONFIG_PATH.exists():
        return {}

    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
        return data if isinstance(data, dict) else {}


def _get_ollama_cfg(config: Dict[str, Any]) -> Dict[str, Any]:
    return (((config.get("ai") or {}).get("ollama")) or {})


def _build_llm_client(config: Dict[str, Any]) -> OllamaLLMClient:
    ollama_cfg = _get_ollama_cfg(config)

    base_url = str(ollama_cfg.get("base_url", "http://127.0.0.1:11434")).strip()
    model = str(ollama_cfg.get("model", "qwen2.5:7b-instruct")).strip()
    endpoint_mode = str(ollama_cfg.get("endpoint_mode", "generate")).strip()
    timeout_sec = int(ollama_cfg.get("timeout_sec", 1200))
    temperature = float(ollama_cfg.get("temperature", 0.0))
    top_p = float(ollama_cfg.get("top_p", 0.8))
    num_predict = int(ollama_cfg.get("num_predict", 1200))
    seed = ollama_cfg.get("seed", 42)

    if timeout_sec <= 0:
        raise ValueError("Config error: timeout_sec must be > 0.")
    if num_predict <= 0:
        raise ValueError("Config error: num_predict must be > 0.")

    return OllamaLLMClient(
        base_url=base_url,
        model=model,
        endpoint_mode=endpoint_mode,
        timeout_sec=timeout_sec,
        temperature=temperature,
        top_p=top_p,
        num_predict=num_predict,
        seed=seed,
    )


def _log_runtime_config(config: Dict[str, Any]) -> None:
    ollama_cfg = _get_ollama_cfg(config)

    _log("Thông tin model:")
    _log(f" - base_url      = {ollama_cfg.get('base_url', 'http://127.0.0.1:11434')}")
    _log(f" - model         = {ollama_cfg.get('model', 'qwen2.5:7b-instruct')}")
    _log(f" - endpoint_mode = {ollama_cfg.get('endpoint_mode', 'generate')}")
    _log(f" - timeout_sec   = {int(ollama_cfg.get('timeout_sec', 1200))}")
    _log(f" - temperature   = {float(ollama_cfg.get('temperature', 0.0))}")
    _log(f" - top_p         = {float(ollama_cfg.get('top_p', 0.8))}")
    _log(f" - num_predict   = {int(ollama_cfg.get('num_predict', 1200))}")
    _log(f" - seed          = {ollama_cfg.get('seed', 42)}")


def _validate_prompt_sources(feature: str) -> None:
    loader = PromptLoader(input_dir=BASE_DIR / "input")
    sources = loader.validate_required_sources(feature)

    _log("Kiểm tra nguồn prompt/spec:")
    for key, path in sources.items():
        _log(f" - {key} = {path}")


def main() -> int:
    total_start = time.perf_counter()
    args = _parse_args()

    _log("Khởi động lệnh generate_ai_data ...")

    try:
        feature = _validate_feature_arg(args.feature)
    except ValueError as exc:
        _log(str(exc))
        return 2

    _log(f"Feature yêu cầu: {feature}")

    try:
        formats = _normalize_formats(args.formats)
    except ValueError as exc:
        _log(str(exc))
        return 2

    _log(f"Formats: {formats}")
    _log(f"Đọc config từ: {CONFIG_PATH}")

    try:
        config = _load_config()
        llm_client = _build_llm_client(config)
        _log_runtime_config(config)
    except Exception as exc:
        _log(f"Config/LLM init failed: {exc}")
        return 2

    try:
        _validate_prompt_sources(feature)
    except FileNotFoundError as exc:
        _log(f"Required input file not found: {exc}")
        return 2
    except Exception as exc:
        _log(f"Prompt source validation failed: {exc}")
        return 2

    pipeline = GenerationPipeline(
        llm_client=llm_client,
        base_dir=BASE_DIR,
        verbose=not args.quiet,
    )

    try:
        final_json_path, processed_files = pipeline.generate(feature, formats)
    except FileNotFoundError as exc:
        _log(f"Required input file not found: {exc}")
        return 2
    except ValueError as exc:
        _log(f"Validation failed: {exc}")
        return 1
    except Exception as exc:
        _log(f"Generation failed: {exc}")
        return 1

    total_elapsed = time.perf_counter() - total_start

    print("\n==================================================", flush=True)
    print("GENERATE TEST DATA SUCCESS", flush=True)
    print(f"Feature: {feature}", flush=True)
    print("Final JSON:", final_json_path, flush=True)
    print("Processed files:", flush=True)
    for f in processed_files:
        print(" -", f, flush=True)
    print(f"TOTAL ELAPSED: {total_elapsed:.2f}s", flush=True)
    print("==================================================", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())