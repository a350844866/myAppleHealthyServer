from __future__ import annotations

import threading
import time
from typing import Any


class TTLCache:
    def __init__(self, default_ttl_seconds: int):
        self.default_ttl_seconds = default_ttl_seconds
        self._values: dict[str, tuple[float, Any]] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Any | None:
        now = time.time()
        with self._lock:
            entry = self._values.get(key)
            if not entry:
                return None
            expires_at, value = entry
            if expires_at < now:
                self._values.pop(key, None)
                return None
            return value

    def set(self, key: str, value: Any, ttl_seconds: int | None = None) -> Any:
        ttl = ttl_seconds if ttl_seconds is not None else self.default_ttl_seconds
        expires_at = time.time() + max(ttl, 0)
        with self._lock:
            self._values[key] = (expires_at, value)
        return value

    def delete(self, key: str) -> None:
        with self._lock:
            self._values.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._values.clear()


dashboard_home_cache = TTLCache(default_ttl_seconds=30)
record_types_cache = TTLCache(default_ttl_seconds=300)
overview_cache = TTLCache(default_ttl_seconds=30)
