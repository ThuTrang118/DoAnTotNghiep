from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

import yaml

from testdata_generation.engine.llm_client import OllamaLLMClient
from testdata_generation.engine.generation_pipeline import GenerationPipeline


BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
CONFIG_PATH = PROJECT_ROOT / "app_config.yaml"

SUPPORTED_FORMATS = ["csv", "json", "xlsx", "xls", "yaml", "yml", "xml", "db"]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m testdata_generation.generate_ai_data",
        description="Generate AI test data with single-step prompt pipeline.",
    )
    parser.add_argument("--feature", required=True, help="Example: login")
    parser.add_argument(
        "--formats",
        nargs="+",
        default=["json"],
        help="Example: --formats json csv xlsx OR --formats all",
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


def _load_config() -> dict:
    if not CONFIG_PATH.exists():
        return {}

    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
        return data if isinstance(data, dict) else {}


def _build_llm_client(config: dict) -> OllamaLLMClient:
    ollama_cfg = (((config.get("ai") or {}).get("ollama")) or {})

    base_url = ollama_cfg.get("base_url", "http://localhost:11434")
    model = ollama_cfg.get("model", "qwen2.5:3b-instruct")
    endpoint_mode = ollama_cfg.get("endpoint_mode", "generate")
    timeout_sec = int(ollama_cfg.get("timeout_sec", 1200))
    temperature = float(ollama_cfg.get("temperature", 0.1))
    top_p = float(ollama_cfg.get("top_p", 0.8))
    num_predict = int(ollama_cfg.get("num_predict", 1200))
    seed = ollama_cfg.get("seed", 42)

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


def main() -> int:
    args = _parse_args()
    feature = args.feature.strip().lower()

    try:
        formats = _normalize_formats(args.formats)
    except ValueError as exc:
        print(str(exc))
        return 2

    config = _load_config()
    llm_client = _build_llm_client(config)
    pipeline = GenerationPipeline(llm_client, BASE_DIR)

    try:
        raw_json_path, processed_files = pipeline.generate(feature, formats)
    except FileNotFoundError as exc:
        print(f"Prompt file not found: {exc}")
        return 2
    except Exception as exc:
        print(f"Generation failed: {exc}")
        return 1

    print("\n==================================================")
    print("GENERATE TEST DATA SUCCESS")
    print("Raw JSON:", raw_json_path)
    print("Processed files:")
    for f in processed_files:
        print(" -", f)
    print("==================================================")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())