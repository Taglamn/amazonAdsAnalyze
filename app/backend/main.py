from __future__ import annotations

from fastapi import FastAPI

from .routes import optimization_router


def create_backend_app() -> FastAPI:
    app = FastAPI(title="Amazon Ads AI Optimization Backend", version="1.0.0")
    app.include_router(optimization_router)
    return app


app = create_backend_app()
