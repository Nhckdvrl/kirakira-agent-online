"""Small OpenAI-compatible embedding client."""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import List


class EmbeddingClient:
    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        model: str,
        timeout: int = 30,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model = model
        self.timeout = timeout

    def embed(self, text: str) -> List[float]:
        payload = json.dumps({"model": self.model, "input": text}).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = "Bearer %s" % self.api_key
        request = urllib.request.Request(
            self._url(), data=payload, headers=headers, method="POST"
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                body = json.loads(response.read().decode("utf-8"))
        except (urllib.error.HTTPError, urllib.error.URLError, ValueError) as exc:
            raise RuntimeError("Embedding request failed: %s" % exc) from exc
        data = body.get("data") or []
        vector = data[0].get("embedding") if data and isinstance(data[0], dict) else None
        if not isinstance(vector, list) or not vector:
            raise RuntimeError("Embedding response has no vector")
        return [float(value) for value in vector]

    def _url(self) -> str:
        if self.base_url.endswith("/embeddings"):
            return self.base_url
        if self.base_url.endswith("/v1"):
            return self.base_url + "/embeddings"
        return self.base_url + "/v1/embeddings"
