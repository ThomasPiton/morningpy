import json
from pathlib import Path
from typing import Optional, Any, Dict


class Cache:
    """
    A simple persistent JSON-based cache for storing authentication values
    (apikey, maas_token, waf_token, etc.).

    Features:
    - Automatic loading on initialization.
    - Automatic persistence on update.
    - Atomic writes (prevents corruption if process is interrupted).
    - Explicit methods for get/set/clear.
    """

    def __init__(self, cache_filename: str = "cache.json"):
        """
        Initialize the cache inside ~/.morningpy/ (user home directory).

        This ensures the cache is writable on all platforms, including
        read-only pip/conda installs on Linux and macOS.
        """
        self.data_dir = Path.home() / ".morningpy"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.cache_path = self.data_dir / cache_filename
        self._cache: dict[str, Any] = self._load_cache()

    def _load_cache(self) -> Dict[str, Any]:
        """Load cache from disk. Return an empty dict on failure."""
        if not self.cache_path.exists():
            return {}

        try:
            with self.cache_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
        except (json.JSONDecodeError, OSError):
            return {}

    def _save_cache(self) -> None:
        """
        Save the cache atomically to avoid corruption.
        (Write to temp file → rename.)
        """
        tmp_path = self.cache_path.with_suffix(".tmp")

        try:
            with tmp_path.open("w", encoding="utf-8") as f:
                json.dump(self._cache, f, indent=2)

            tmp_path.replace(self.cache_path)  # atomic operation
        finally:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)

    def get(self, key: str) -> Optional[Any]:
        """Retrieve a cached value by key."""
        return self._cache.get(key)

    def set(self, key: str, value: Any) -> None:
        """
        Store or update a key-value pair in the cache.
        
        Empty or None values are ignored (to avoid polluting the cache).
        """
        if value is None:
            return

        self._cache[key] = value
        self._save_cache()

    def delete(self, key: str) -> None:
        """Delete a single key from the cache."""
        if key in self._cache:
            del self._cache[key]
            self._save_cache()

    def clear(self) -> None:
        """Clear the entire cache."""
        self._cache = {}
        self._save_cache()

    def keys(self) -> list[str]:
        """Return a list of stored keys."""
        return list(self._cache.keys())

    def as_dict(self) -> Dict[str, Any]:
        """Return the whole cache (read-only)."""
        return dict(self._cache)
