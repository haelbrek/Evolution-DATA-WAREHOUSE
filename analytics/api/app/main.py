from __future__ import annotations

from fastapi import FastAPI

from analytics.api.app.config import settings
from analytics.api.app.routers import tables

app = FastAPI(
    title="Projet Data ENG API",
    version="0.1.0",
    description="Exposition REST des tables preparees stockees dans Azure SQL Database",
)

app.include_router(tables.router)


@app.get("/health", tags=["health"])
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/config", tags=["health"], include_in_schema=False)
def config_info() -> dict[str, str | int]:
    return {
        "server": settings.azure_sql_server,
        "database": settings.azure_sql_database,
        "schema": settings.azure_sql_schema,
        "chunksize": settings.azure_sql_chunksize,
        "driver": settings.azure_sql_driver,
    }
