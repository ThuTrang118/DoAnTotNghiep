# llm_engine/engine/llm_client.py
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
import requests


class BaseLLMClient(ABC):
    @abstractmethod
    def generate_text(self, prompt: str, system: Optional[str] = None, **kwargs) -> str:
        raise NotImplementedError

    @abstractmethod
    def healthcheck(self) -> Dict[str, Any]:
        raise NotImplementedError


class OllamaClient(BaseLLMClient):
    """
    Ollama local REST API:
      - GET  /api/tags
      - POST /api/generate
      - POST /api/chat
    endpoint_mode: auto | generate | chat
    """

    def __init__(
        self,
        base_url: str = "http://localhost:11434",
        model: str = "mistral:latest",
        timeout_sec: int = 300,
        endpoint_mode: str = "auto",
        temperature: float = 0.2,
        top_p: float = 0.9,
        num_predict: int = 800,
        seed: Optional[int] = 42,
    ):
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout_sec = int(timeout_sec)
        self.endpoint_mode = (endpoint_mode or "auto").lower()
        self.temperature = float(temperature)
        self.top_p = float(top_p)
        self.num_predict = int(num_predict)
        self.seed = seed

    def healthcheck(self) -> Dict[str, Any]:
        url = f"{self.base_url}/api/tags"
        r = requests.get(url, timeout=self.timeout_sec)
        r.raise_for_status()
        return {"ok": True, "provider": "ollama", "models": r.json().get("models", [])}

    def generate_text(self, prompt: str, system: Optional[str] = None, **kwargs) -> str:
        mode = (kwargs.get("endpoint_mode") or self.endpoint_mode or "auto").lower()

        if mode == "generate":
            return self._generate(prompt=prompt, system=system, **kwargs)
        if mode == "chat":
            return self._chat(prompt=prompt, system=system, **kwargs)

        # auto: ưu tiên generate (đơn giản) rồi fallback chat
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

        r = requests.post(url, json=payload, timeout=self.timeout_sec)
        r.raise_for_status()
        return r.json().get("response", "")

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

        r = requests.post(url, json=payload, timeout=self.timeout_sec)
        r.raise_for_status()
        data = r.json()
        return (data.get("message") or {}).get("content", "")
