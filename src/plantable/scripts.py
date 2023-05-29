import logging

import click
import uvicorn
from click_loglevel import LogLevel


@click.group()
def plantable():
    pass


@plantable.command()
def hello():
    print("Hello, PlanTable!")


@plantable.command()
@click.option("-h", "--host", type=str, default="0.0.0.0")
@click.option("-p", "--port", type=int, default=3000)
@click.option("--reload", is_flag=True)
@click.option("--workers", type=int, default=None)
@click.option("--log-level", type=LogLevel(), default=logging.INFO)
def run(host, port, reload, workers, log_level):
    logging.basicConfig(level=log_level)

    if reload:
        app = "plantable.app:app"
    else:
        from .app import app

    uvicorn.run(
        app,
        host=host,
        port=port,
        reload=reload,
        workers=workers,
        log_level=log_level,
    )
