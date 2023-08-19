import redis.asyncio as redis
from .conf import REDIS_HOST, REDIS_PORT, KEY_PREFIX


class RedisConsumer:
    def __init__(self, redis_host=REDIS_HOST, redis_port=REDIS_PORT, key_prefix=KEY_PREFIX):
        self.redis_host = REDIS_HOST
        self.redis_port = REDIS_PORT
        self.key_prefix = KEY_PREFIX

        self.client = redis.Redis(host=self.REDIS_HOST, port=self.REDIS_PORT)

    async def run(self):
        pass
