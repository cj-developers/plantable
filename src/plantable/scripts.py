import logging

import click
import uvicorn
from click_loglevel import LogLevel

from .agent import conf as AGENT_CONF

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
@click.option("--seatable-url", default=AGENT_CONF.SEATABLE_URL, required=True)
@click.option("--seatable-username", default=AGENT_CONF.SEATABLE_USERNAME, required=True)
@click.option("--seatable-password", default=AGENT_CONF.SEATABLE_PASSWORD, required=True)
@click.option("--redis-host", default=AGENT_CONF.REDIS_HOST, required=True)
@click.option("--redis-port", default=AGENT_CONF.REDIS_PORT, required=True)
@click.option("--log-level", default=logging.WARNING, type=LogLevel())
def run_producer(seatable_url, seatable_username, seatable_password, redis_host, redis_port, log_level):
    import asyncio

    from redis.exceptions import ConnectionError as RedisConnectionError

    from plantable.agent import Producer, RedisStreamAdder

    logging.basicConfig(level=log_level)

    REDIS_CONF = {"host": redis_host, "port": redis_port}

    SEATABLE_CONF = {
        "seatable_url": seatable_url,
        "seatable_username": seatable_username,
        "seatable_password": seatable_password,
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
