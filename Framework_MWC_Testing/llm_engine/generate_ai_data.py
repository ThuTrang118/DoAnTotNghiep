from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import List, Optional, Dict, Any

from llm_engine.engine.llm_client import OllamaClient
from llm_engine.engine.generator import AITestDataGenerator


# -------------------------
# Helpers
# -------------------------
def project_root() -> Path:
    # llm_engine/generate_ai_data.py -> <root>
    return Path(__file__).resolve().parents[1]


def read_text(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    return path.read_text(encoding="utf-8").strip()


def load_login_seed_context(root: Path) -> str:
    """
    Return:
      SEED_VALID_ACCOUNT Username=...; Password=...
    or "" if not found.
    """
    p = root / "data" / "manual" / "LoginSeedAccounts.json"
    if not p.exists():
        return ""

    data = json.loads(p.read_text(encoding="utf-8"))
    accounts = data.get("valid_accounts", [])
    if not accounts:
        return ""

    acc = accounts[0]
    u = (acc.get("Username") or "").strip()
    pw = (acc.get("Password") or "").strip()
    if not u or not pw:
        return ""

    return f"SEED_VALID_ACCOUNT Username={u}; Password={pw}"


def parse_formats(value: str) -> Optional[List[str]]:
    if not value:
        return None
    v = value.strip().lower()
    if v == "all":
        return None
    return [x.strip().lower() for x in v.split(",") if x.strip()]


def build_feature_prompt(root: Path, feature: str, context: str) -> str:
    """
    Build FINAL prompt = base_prompt.txt + feature.txt (with Context injected)
    """
    prompts_dir = root / "llm_engine" / "prompts"
    base_prompt = read_text(prompts_dir / "base_prompt.txt")

    feature_path = prompts_dir / f"{feature}.txt"
    feature_template = read_text(feature_path)

    # login.txt của bạn có placeholder {app_context}【:contentReference[oaicite:0]{index=0}】
    feature_prompt = feature_template.replace("{app_context}", context or "")

    final_prompt = f"{base_prompt}\n\n{feature_prompt}".strip()
    return final_prompt


# -------------------------
# CLI
# -------------------------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Generate AI test data using llm_engine (Ollama).")

    p.add_argument(
        "--feature",
        required=True,
        choices=["login", "register", "search", "order"],
        help="Feature to generate data for",
    )
    p.add_argument(
        "--formats",
        default="all",
        help="all | csv,json,xlsx,xls,xml,yaml,yml,db",
    )

    # Optional extra context appended (besides seed)
    p.add_argument("--extra-context", default="", help="Extra context lines (optional).")

    # Ollama settings
    p.add_argument("--base-url", default="http://localhost:11434")
    p.add_argument("--model", default="mistral:latest")
    p.add_argument("--timeout-sec", type=int, default=300)
    p.add_argument("--endpoint-mode", default="auto", choices=["auto", "generate", "chat"])
    p.add_argument("--temperature", type=float, default=0.2)
    p.add_argument("--top-p", type=float, default=0.9)
    p.add_argument("--num-predict", type=int, default=2500)
    p.add_argument("--seed", type=int, default=42)

    p.add_argument("--healthcheck", action="store_true")
    return p


def main() -> int:
    args = build_parser().parse_args()
    root = project_root()

    # -------------------------
    # Build context
    # -------------------------
    context_lines: List[str] = []
    if args.feature == "login":
        seed_line = load_login_seed_context(root)
        if seed_line:
            context_lines.append(seed_line)

    extra = (args.extra_context or "").strip()
    if extra:
        context_lines.append(extra)

    context = "\n".join(context_lines).strip()

    # -------------------------
    # Build prompt
    # -------------------------
    prompt = build_feature_prompt(root, args.feature, context)

    # -------------------------
    # Init client
    # -------------------------
    client = OllamaClient(
        base_url=args.base_url,
        model=args.model,
        timeout_sec=args.timeout_sec,
        endpoint_mode=args.endpoint_mode,
        temperature=args.temperature,
        top_p=args.top_p,
        num_predict=args.num_predict,
        seed=args.seed,
    )

    if args.healthcheck:
        print("Healthcheck:", client.healthcheck())
        return 0

    # -------------------------
    # Run pipeline (generator expects prompt)
    # -------------------------
    formats = parse_formats(args.formats)
    generator = AITestDataGenerator(client=client)

    result = generator.generate(
        feature=args.feature,
        prompt=prompt,
        system=None,        # base_prompt đã nằm trong prompt rồi
        formats=formats,
        yaml_ext="yaml",
        llm_kwargs={},      # có thể truyền temperature/top_p ở client rồi
    )

    # -------------------------
    # Print summary
    # -------------------------
    print("AI generation completed.")
    print(f"Feature: {result.feature}")
    print(f"OK: {result.ok}")

    if result.raw_text_path:
        print(f"Raw text: {result.raw_text_path}")
    if result.raw_path:
        print(f"Raw json:  {result.raw_path}")

    if result.processed_paths:
        print("Processed files:")
        for k, v in result.processed_paths.items():
            print(f"  - {k}: {v}")

    if result.errors:
        print("Errors:")
        for e in result.errors:
            print(f"  - {e}")

    print(f"Total rows: {len(result.rows or [])}")
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
