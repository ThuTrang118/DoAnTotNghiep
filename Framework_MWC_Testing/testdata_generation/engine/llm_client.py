from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests


@dataclass
class LLMResponse:
    """
    Gói kết quả trả về từ LLM nếu sau này cần mở rộng.
    Hiện tại pipeline chủ yếu dùng raw_text.
    """
    raw_text: str
    status_code: int
    endpoint_mode: str


class OllamaLLMClient:
    """
    Client dùng chung cho Ollama, tương thích với pipeline mới.

    Hỗ trợ:
    - /api/generate
    - /api/chat
    - endpoint_mode = generate | chat | auto

    API chính:
    - generate(prompt, **kwargs) -> str
    - generate_text(prompt, **kwargs) -> str
    - __call__(prompt, **kwargs) -> str
    """

    def __init__(
        self,
        base_url: str = "http://127.0.0.1:11434",
        model: str = "qwen3:14b",
        endpoint_mode: str = "generate",
        timeout_sec: int = 180,
        temperature: float = 0.1,
        top_p: float = 0.9,
        num_predict: int = 1200,
        json_mode: bool = False,
        seed: Optional[int] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.endpoint_mode = (endpoint_mode or "generate").strip().lower()
        self.timeout_sec = int(timeout_sec)
        self.temperature = float(temperature)
        self.top_p = float(top_p)
        self.num_predict = int(num_predict)
        self.json_mode = bool(json_mode)
        self.seed = seed

        if self.endpoint_mode not in {"generate", "chat", "auto"}:
            raise ValueError(
                "endpoint_mode must be one of: generate, chat, auto"
            )

    # =========================================================
    # Public API
    # =========================================================
    def __call__(self, prompt: str, **kwargs: Any) -> str:
        return self.generate(prompt, **kwargs)

    def generate_text(self, prompt: str, **kwargs: Any) -> str:
        """
        Alias tương thích code cũ.
        """
        return self.generate(prompt, **kwargs)

    def generate(self, prompt: str, **kwargs: Any) -> str:
        """
        API chính cho pipeline mới.
        """
        mode = str(kwargs.pop("endpoint_mode", self.endpoint_mode)).strip().lower()

        if not isinstance(prompt, str) or not prompt.strip():
            raise ValueError("Prompt must be a non-empty string.")

        if mode == "auto":
            try:
                return self._generate_via_generate(prompt, **kwargs)
            except Exception:
                return self._generate_via_chat(prompt, **kwargs)

        if mode == "generate":
            return self._generate_via_generate(prompt, **kwargs)

        if mode == "chat":
            return self._generate_via_chat(prompt, **kwargs)

        raise ValueError(
            f"Unsupported endpoint_mode='{mode}'. "
            "Expected generate, chat, or auto."
        )

    def healthcheck(self) -> Dict[str, Any]:
        """
        Kiểm tra Ollama có sống không.
        """
        url = f"{self.base_url}/api/tags"
        resp = requests.get(url, timeout=self.timeout_sec)
        resp.raise_for_status()
        return resp.json()

    # =========================================================
    # Internal helpers
    # =========================================================
    def _effective_options(self, **kwargs: Any) -> Dict[str, Any]:
        temperature = kwargs.pop("temperature", self.temperature)
        top_p = kwargs.pop("top_p", self.top_p)
        num_predict = kwargs.pop("num_predict", self.num_predict)
        seed = kwargs.pop("seed", self.seed)

        options: Dict[str, Any] = {
            "temperature": temperature,
            "top_p": top_p,
            "num_predict": num_predict,
        }

        if seed is not None:
            options["seed"] = seed

        extra_options = kwargs.pop("options", None)
        if isinstance(extra_options, dict):
            options.update(extra_options)

        return options

    def _effective_json_mode(self, **kwargs: Any) -> bool:
        return bool(kwargs.pop("json_mode", self.json_mode))

    def _effective_timeout(self, **kwargs: Any) -> int:
        timeout_sec = int(kwargs.pop("timeout_sec", self.timeout_sec))
        if timeout_sec <= 0:
            raise ValueError("timeout_sec must be > 0.")
        return timeout_sec

    def _effective_model(self, **kwargs: Any) -> str:
        model = str(kwargs.pop("model", self.model)).strip()
        if not model:
            raise ValueError("model must be non-empty.")
        return model

    @staticmethod
    def _safe_json_dumps(data: Any) -> str:
        try:
            return json.dumps(data, ensure_ascii=False)
        except Exception:
            return str(data)

    @staticmethod
    def _truncate(text: str, limit: int = 500) -> str:
        text = str(text or "")
        if len(text) <= limit:
            return text
        return text[:limit] + "...<truncated>"

    def _raise_if_empty_text(
        self,
        text: Any,
        endpoint_name: str,
        status_code: int,
        response_body: Any,
    ) -> None:
        if text is None or not str(text).strip():
            body_preview = self._truncate(self._safe_json_dumps(response_body))
            raise RuntimeError(
                f"Ollama returned empty text from {endpoint_name} "
                f"(status_code={status_code}). Response body: {body_preview}"
            )

    def _generate_via_generate(self, prompt: str, **kwargs: Any) -> str:
        url = f"{self.base_url}/api/generate"
        payload = self._build_generate_payload(prompt, **kwargs)
        timeout_sec = self._effective_timeout(**kwargs)

        resp = requests.post(url, json=payload, timeout=timeout_sec)
        resp.raise_for_status()

        data = resp.json()

        text = data.get("response", "")
        self._raise_if_empty_text(
            text=text,
            endpoint_name="/api/generate",
            status_code=resp.status_code,
            response_body=data,
        )

        return str(text)

    def _generate_via_chat(self, prompt: str, **kwargs: Any) -> str:
        url = f"{self.base_url}/api/chat"
        payload = self._build_chat_payload(prompt, **kwargs)
        timeout_sec = self._effective_timeout(**kwargs)

        resp = requests.post(url, json=payload, timeout=timeout_sec)
        resp.raise_for_status()

        data = resp.json()

        message = data.get("message", {}) or {}
        text = message.get("content", "")
        self._raise_if_empty_text(
            text=text,
            endpoint_name="/api/chat",
            status_code=resp.status_code,
            response_body=data,
        )

        return str(text)

    def _build_generate_payload(self, prompt: str, **kwargs: Any) -> Dict[str, Any]:
        model = self._effective_model(**kwargs)
        json_mode = self._effective_json_mode(**kwargs)
        options = self._effective_options(**kwargs)

        payload: Dict[str, Any] = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": options,
        }

        if json_mode:
            payload["format"] = "json"

        return payload

    def _build_chat_payload(self, prompt: str, **kwargs: Any) -> Dict[str, Any]:
        model = self._effective_model(**kwargs)
        json_mode = self._effective_json_mode(**kwargs)
        options = self._effective_options(**kwargs)

        payload: Dict[str, Any] = {
            "model": model,
            "messages": [
                {
                    "role": "user",
                    "content": prompt,
                }
            ],
            "stream": False,
            "options": options,
        }

        if json_mode:
            payload["format"] = "json"

        return payload


# =========================================================
# Backward compatibility
# =========================================================
class OllamaClient(OllamaLLMClient):
    """
    Alias tương thích ngược với code cũ.
    """
    pass