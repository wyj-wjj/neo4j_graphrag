"""ASGI entrypoint."""

from graphrag.api.app import create_app

app = create_app()
