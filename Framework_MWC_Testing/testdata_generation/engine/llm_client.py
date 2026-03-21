from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Tuple, Union
import requests


TimeoutType = Union[int, float, Tuple[float, float]]  # (connect, read)


class BaseLLMClient(ABC):
    @abstractmethod
    def generate_text(self, prompt: str, system: Optional[str] = None, **kwargs) -> str:
        raise NotImplementedError

    @abstractmethod
    def healthcheck(self) -> Dict[str, Any]:
        raise NotImplementedError


class OllamaClient(BaseLLMClient):
    def __init__(
        self,
        base_url: Optional[str] = None,
        model: Optional[str] = None,
        timeout_sec: int = 900,
        endpoint_mode: str = "auto",
        temperature: float = 0.0,
        top_p: float = 0.9,
        num_predict: int = 2000,
        seed: Optional[int] = 42,
        connect_timeout_sec: int = 10,
        json_mode: bool = True,
    ):
        self.base_url = (base_url or "http://localhost:11434").rstrip("/")
        self.model = model or "qwen2.5:7b-instruct"
        self.timeout_sec = int(timeout_sec)
        self.connect_timeout_sec = int(connect_timeout_sec)
        self.endpoint_mode = (endpoint_mode or "auto").lower()
        self.temperature = float(temperature)
        self.top_p = float(top_p)
        self.num_predict = int(num_predict)
        self.seed = seed
        self.json_mode = bool(json_mode)

    def _timeout(self) -> TimeoutType:
        return (float(self.connect_timeout_sec), float(self.timeout_sec))

    def _clean_response_text(self, text: str) -> str:
        """
        Dọn các trường hợp model vẫn lỡ trả về code fence hoặc chữ 'json'
        trước khi parser xử lý.
        """
        text = (text or "").strip()

        if text.startswith("```"):
            lines = text.splitlines()
            if lines and lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            text = "\n".join(lines).strip()

        if text.lower().startswith("json\n"):
            text = text[5:].strip()

        return text

    def _use_json_mode(self, **kwargs) -> bool:
        return bool(kwargs.get("json_mode", self.json_mode))

    def healthcheck(self) -> Dict[str, Any]:
        url = f"{self.base_url}/api/tags"
        r = requests.get(url, timeout=self._timeout())
        r.raise_for_status()
        return {"ok": True, "provider": "ollama", "models": r.json().get("models", [])}

    def generate_text(self, prompt: str, system: Optional[str] = None, **kwargs) -> str:
        mode = (kwargs.get("endpoint_mode") or self.endpoint_mode or "auto").lower()

        if mode == "generate":
            return self._generate(prompt=prompt, system=system, **kwargs)
        if mode == "chat":
            return self._chat(prompt=prompt, system=system, **kwargs)

        try:
            return self._generate(prompt=prompt, system=system, **kwargs)
        except Exception:
            return self._chat(prompt=prompt, system=system, **kwargs)

    def _generate(self, prompt: str, system: Optional[str], **kwargs) -> str:
        url = f"{self.base_url}/api/generate"

        payload: Dict[str, Any] = {
            "model": kwargs.get("model", self.model),
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": kwargs.get("temperature", self.temperature),
                "top_p": kwargs.get("top_p", self.top_p),
                "num_predict": kwargs.get("num_predict", self.num_predict),
            },
        }

        seed = kwargs.get("seed", self.seed)
        if seed is not None:
            payload["options"]["seed"] = seed

        if system:
            payload["system"] = system

        if self._use_json_mode(**kwargs):
            payload["format"] = "json"

        r = requests.post(url, json=payload, timeout=self._timeout())
        r.raise_for_status()

        text = r.json().get("response", "")
        return self._clean_response_text(text)

    def _chat(self, prompt: str, system: Optional[str], **kwargs) -> str:
        url = f"{self.base_url}/api/chat"

        messages: List[Dict[str, str]] = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        payload: Dict[str, Any] = {
            "model": kwargs.get("model", self.model),
            "messages": messages,
            "stream": False,
            "options": {
                "temperature": kwargs.get("temperature", self.temperature),
                "top_p": kwargs.get("top_p", self.top_p),
                "num_predict": kwargs.get("num_predict", self.num_predict),
            },
        }

        seed = kwargs.get("seed", self.seed)
        if seed is not None:
            payload["options"]["seed"] = seed

        if self._use_json_mode(**kwargs):
            payload["format"] = "json"

        r = requests.post(url, json=payload, timeout=self._timeout())
        r.raise_for_status()

        data = r.json()
        text = (data.get("message") or {}).get("content", "")
        return self._clean_response_text(text)