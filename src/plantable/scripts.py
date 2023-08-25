import logging

import click
import uvicorn
import uvloop
from click_loglevel import LogLevel

logger = logging.getLogger(__file__)


@click.group()
def plantable():
    pass


@plantable.command()
def hello():
    print("Hello, Plantable!")


@plantable.group()
def agent():
    pass


@agent.command()
@click.option("--seatable-url", default=None)
@click.option("--seatable-username", default=None)
@click.option("--seatable-password", default=None)
@click.option("--redis-host", default=None)
@click.option("--redis-port", default=None)
@click.option("--log-level", default=logging.WARNING, type=LogLevel())
def run_producer(seatable_url, seatable_username, seatable_password, redis_host, redis_port, log_level):
    import asyncio

    from redis.exceptions import ConnectionError as RedisConnectionError

    from plantable.agent import Producer, RedisStreamAdder
    from plantable.agent.conf import (
        AWS_S3_ACCESS_KEY_ID,
        AWS_S3_SECRET_ACCESS_KEY,
        REDIS_HOST,
        REDIS_PORT,
        SEATABLE_PASSWORD,
        SEATABLE_URL,
        SEATABLE_USERNAME,
    )

    logging.basicConfig(level=log_level)

    REDIS_CONF = {
        "host": redis_host or REDIS_HOST,
        "port": redis_port or REDIS_PORT,
    }

    SEATABLE_CONF = {
        "seatable_url": seatable_url or SEATABLE_URL,
        "seatable_username": seatable_username or SEATABLE_USERNAME,
        "seatable_password": seatable_password or SEATABLE_PASSWORD,
    }

    async def main():
        handler = RedisStreamAdder(**REDIS_CONF)
        for _ in range(12):
            try:
                await handler.redis_client.ping()
                break
            except RedisConnectionError:
                print("Wait Redis...")
            await asyncio.sleep(5.0)

        producer = Producer(**SEATABLE_CONF, handler=handler)

        try:
            await producer.run()
        except asyncio.CancelledError:
            return

    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    asyncio.run(main())


@plantable.group()
def server():
    pass


@server.command()
@click.option("-h", "--host", type=str, default="0.0.0.0")
@click.option("-p", "--port", type=int, default=3000)
@click.option("--reload", is_flag=True)
@click.option("--workers", type=int, default=None)
@click.option("--log-level", type=LogLevel(), default=logging.INFO)
def run(host, port, reload, workers, log_level):
    logging.basicConfig(level=log_level)

    if reload:
        app = "plantable.server.app:app"
    else:
        from .server.app import app

    uvicorn.run(
        app,
        host=host,
        port=port,
        reload=reload,
        workers=workers,
        log_level=log_level,
    )
