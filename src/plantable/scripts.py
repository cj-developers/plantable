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
@click.option("--log-level", default=logging.WARNING, type=LogLevel())
def produce(log_level):
    import asyncio

    from redis.exceptions import ConnectionError as RedisConnectionError

    from plantable.agent import Producer, RedisStreamAdder

    logging.basicConfig(level=log_level)

    async def main():
        handler = RedisStreamAdder()
        for _ in range(12):
            try:
                await handler.redis_client.ping()
                break
            except RedisConnectionError as ex:
                print("Wait Redis...")
            await asyncio.sleep(5.0)

        producer = Producer(handler=handler)

        await producer.run()

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
