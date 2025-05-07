import os
import logging
from threading import Lock
import redis.asyncio as redis # async version

class RedisClient:
    _instance = None
    _lock = Lock()  # Thread-safe singleton

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    async def initialize(self):
        if not self._initialized:
            self.redis_url = os.environ.get("REDIS_URL")
            self.redis_client = None
            await self._connect()
            self._initialized = True

    async def _connect(self):
        if not self.redis_client:
            try:
                self.redis_client = redis.from_url(self.redis_url, decode_responses=True)
                await self.redis_client.ping()
                logging.info("[RedisClient] Connected to Redis.")
            except redis.RedisError as e:
                logging.error(f"[RedisClient] Redis connection failed: {e}", exc_info=True)
                raise

    async def store_upload_info(self, filename: str, info: dict, ttl_seconds: int = None):
        key = f"upload:{filename}"
        try:
            await self.redis_client.hset(key, mapping={k: str(v) for k, v in info.items()})
            if ttl_seconds:
                await self.redis_client.expire(key, ttl_seconds)
            logging.info(f"[RedisClient] Stored upload info: {key}")
        except redis.RedisError as e:
            logging.error(f"[RedisClient] Failed to store HSET: {e}", exc_info=True)

    async def get_upload_info(self, filename: str):
        key = f"upload:{filename}"
        try:
            data = await self.redis_client.hgetall(key)
            if data:
                logging.info(f"[RedisClient] Retrieved data for: {key}")
                return data
            else:
                logging.info(f"[RedisClient] No data found for: {key}")
                return None
        except redis.RedisError as e:
            logging.error(f"[RedisClient] Failed to get data: {e}", exc_info=True)
            return None

    async def delete_upload_info_hash_set(self, filename: str) -> bool:
        key = f"upload:{filename}"
        try:
            result = await self.redis_client.delete(key)
            if result == 1:
                logging.info(f"[RedisClient] Deleted hash_key: '{key}'")
                return True
            else:
                logging.info(f"[RedisClient] hash_key: '{key}' not found to delete")
                return False
        except redis.RedisError as e:
            logging.error(f"[RedisClient] Failed to delete hash_key '{key}': {e}", exc_info=True)
            return False

    async def store_user_info(self, username: str, info: dict, ttl_seconds: int = None):
        key = f"userinfo:{username}"
        try:
            await self.redis_client.hset(key, mapping={k: str(v) for k, v in info.items()})
            if ttl_seconds:
                await self.redis_client.expire(key, ttl_seconds)
            logging.info(f"[RedisClient] Stored user info: {key}")
        except redis.RedisError as e:
            logging.error(f"[RedisClient] Failed to store user info: {e}", exc_info=True)

    async def get_user_info(self, username: str):
        key = f"userinfo:{username}"
        try:
            data = await self.redis_client.hgetall(key)
            if data:
                logging.info(f"[RedisClient] Retrieved data for: {key}")
                return data
            else:
                logging.info(f"[RedisClient] No data found for: {key}")
                return None
        except redis.RedisError as e:
            logging.error(f"[RedisClient] Failed to get user info: {e}", exc_info=True)
            return None

    async def delete_user_info_hash_set(self, username: str) -> bool:
        key = f"userinfo:{username}"
        try:
            result = await self.redis_client.delete(key)
            if result == 1:
                logging.info(f"[RedisClient] Deleted hash_key: '{key}'")
                return True
            else:
                logging.info(f"[RedisClient] hash_key: '{key}' not found to delete")
                return False
        except redis.RedisError as e:
            logging.error(f"[RedisClient] Failed to delete hash_key '{key}': {e}", exc_info=True)
            return False
