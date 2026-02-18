from __future__ import annotations

from typing import Dict, List

from fastapi import APIRouter, HTTPException, Query
import pandas as pd
import sqlalchemy as sa

from analytics.api.app.config import settings
from analytics.lib.data_prep import prepare_tables, tables_summary

router = APIRouter(prefix="/tables", tags=["tables"])


@router.get("/summary")
def get_tables_summary() -> List[Dict[str, object]]:
    tables = prepare_tables()
    summary_df = tables_summary(tables)
    return summary_df.to_dict(orient="records")


@router.get("/{table_name}")
def get_table_records(
    table_name: str,
    limit: int = Query(100, ge=1, le=1000),
) -> List[Dict[str, object]]:
    allowed = set(settings.allowed_tables) if settings.allowed_tables else None
    if allowed and table_name not in allowed:
        raise HTTPException(status_code=404, detail=f"Table {table_name} non autorisee")

    engine = sa.create_engine(settings.sqlalchemy_dsn, fast_executemany=True)
    query = sa.text(f"SELECT TOP (:limit) * FROM {settings.azure_sql_schema}.{table_name}")

    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(query, conn, params={"limit": limit})
    except sa.exc.SQLAlchemyError as exc:  # pragma: no cover - log/raise generic error
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        engine.dispose()

    return df.to_dict(orient="records")
