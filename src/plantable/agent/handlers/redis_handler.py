import asyncio
import json
import logging
import time
import traceback
from typing import Callable, List

import redis.asyncio as redis

logger = logging.getLogger(__name__)


################################################################
# RedisStreamAdder
################################################################
class RedisStreamAdder:
    def __init__(
        self,
        host: str = "localhost",
        port: str = 6379,
        key_prefix: str = "fasto",
        maxlen: int = None,
        approximate: bool = False,
    ):
        # properties
        self.host = host
        self.port = port
        self.key_prefix = key_prefix
        self.maxlen = maxlen
        self.approximate = approximate

        # redis client
        self.redis_client = redis.Redis(host=self.host, port=self.port)

    async def __call__(self, key, msg):
        try:
            pipe = self.redis_client.pipeline()
            pipe.xadd("|".join([self.key_prefix, key]), msg)
            await pipe.execute()
        except Exception as ex:
            logger.warn(f"RedisStreamAdder {ex}")
            traceback.print_exc()
