from __future__ import annotations
import argparse
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import yaml
from testdata_generation.engine.generator import AITestDataGenerator
from testdata_generation.engine.llm_client import OllamaClient

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
    Returns: (formats, want_export)
      formats=None means ALL (pass through)
      formats=[] means none
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

def read_prompt_file(project_root: Path, feature: str) -> str:
    p = project_root / "testdata_generation" / "input" / f"{feature}.txt"
    if not p.exists():
        raise FileNotFoundError(f"Prompt file not found: {p}")
    return p.read_text(encoding="utf-8")

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
    model = args.model or ollama_cfg.get("model") or "qwen2.5-coder:3b-instruct"
    timeout_sec = args.timeout_sec if args.timeout_sec is not None else int(ollama_cfg.get("timeout_sec", 300))
    endpoint_mode = args.endpoint_mode or ollama_cfg.get("endpoint_mode") or "auto"
    temperature = args.temperature if args.temperature is not None else float(ollama_cfg.get("temperature", 0.2))
    top_p = args.top_p if args.top_p is not None else float(ollama_cfg.get("top_p", 0.9))
    num_predict = args.num_predict if args.num_predict is not None else int(ollama_cfg.get("num_predict", 600))
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
    )

    feature = args.feature.strip().lower()

    # Prompt now contains seed directly (NO LoginSeedAccounts.json)
    prompt = read_prompt_file(root, feature)

    # Backward-compatible: if old prompt still contains {app_context}, replace with empty string
    # so you don't have to edit prompt immediately.
    if "{app_context}" in prompt:
        prompt = prompt.replace("{app_context}", "")

    formats, want_export = parse_formats(args.formats)

    # RAW
    raw_dir = root / "testdata_generation" / "output"

    # PROCESSED
    processed_dir = root / "data" / "ai_processed"

    gen = AITestDataGenerator(
        client=client,
        project_root=root,
        raw_evidence_dir=raw_dir,
        processed_dir=processed_dir,
    )

    result = gen.generate(
        feature=feature,
        prompt=prompt,
        system=None,
        formats=(formats if want_export else []),
        yaml_ext="yaml",
        llm_kwargs={},
    )

    print("AI generation completed.")
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
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())