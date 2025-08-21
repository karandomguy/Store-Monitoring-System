import json
import redis
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from app.core.config import settings


class CacheManager:
    """Redis-based cache manager for store monitoring system."""

    def __init__(self):
        self.redis_client = redis.from_url(
            settings.redis_url,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True
        )
        self.default_ttl = settings.cache_ttl_seconds
        self.prefix = "store_monitoring"

    # Cache Key
    def _make_key(self, key_type: str, identifier: str = "") -> str:
        if identifier:
            return f"{self.prefix}:{key_type}:{identifier}"
        return f"{self.prefix}:{key_type}"
    
    # Check Accessiblity
    def health_check(self) -> bool:
        try:
            self.redis_client.ping()
            return True
        except Exception:
            return False
    
    # Store metrics caching
    def get_store_metrics(self, store_id: str) -> Optional[Dict[str, Any]]:
        try:
            key = self._make_key("metrics", store_id)
            data = self.redis_client.get(key)
            return json.loads(data) if data else None
        except Exception:
            return None
    
    def set_store_metrics(self, store_id: str, metrics: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        try:
            key = self._make_key("metrics", store_id)
            value = json.dumps(metrics)
            self.redis_client.setex(key, ttl or self.default_ttl, value)
            return True
        except Exception:
            return False
    
    def invalidate_store_cache(self, store_id: str):
        try:
            key = self._make_key("metrics", store_id)
            self.redis_client.delete(key)
        except Exception:
            pass
    
    def invalidate_multiple_stores(self, store_ids: List[str]):
        try:
            keys = [self._make_key("metrics", store_id) for store_id in store_ids]
            if keys:
                self.redis_client.delete(*keys)
        except Exception:
            pass
    
    # System stats caching
    def get_system_stats(self) -> Optional[Dict[str, Any]]:
        try:
            key = self._make_key("system", "stats")
            data = self.redis_client.get(key)
            return json.loads(data) if data else None
        except Exception:
            return None
    
    def set_system_stats(self, stats: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        try:
            key = self._make_key("system", "stats")
            value = json.dumps(stats)
            self.redis_client.setex(key, ttl or self.default_ttl, value)
            return True
        except Exception:
            return False
    
    # Report status caching
    def get_report_status(self, report_id: str) -> Optional[Dict[str, Any]]:
        try:
            key = self._make_key("report", report_id)
            data = self.redis_client.get(key)
            return json.loads(data) if data else None
        except Exception:
            return None
    
    def set_report_status(self, report_id: str, status: Dict[str, Any], ttl: int = 3600) -> bool:
        try:
            key = self._make_key("report", report_id)
            value = json.dumps(status)
            self.redis_client.setex(key, ttl, value)
            return True
        except Exception:
            return False


# Global cache instance
cache = CacheManager()