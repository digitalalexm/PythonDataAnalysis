"""FastAPI app exposing cleaned/aggregated event metrics.

Run with:
    uvicorn api.main:app --reload

Expects pipeline.run_pipeline to have already produced output/metrics.csv.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query, Request

from .repository import MetricsRepository
from .schemas import MetricResponse

METRICS_CSV_PATH = Path(__file__).resolve().parent.parent / "output" / "metrics.csv"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Loaded once per process at startup, not per-request - metrics.csv is
    # small today, but re-parsing it on every request wouldn't scale even to
    # a modest request rate. See api/repository.py for the swap-the-source
    # seam this enables later.
    app.state.metrics_repository = MetricsRepository.from_csv(METRICS_CSV_PATH)
    yield


app = FastAPI(title="Events Metrics API", lifespan=lifespan)


def get_repository(request: Request) -> MetricsRepository:
    return request.app.state.metrics_repository


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics", response_model=list[MetricResponse])
def get_metrics(
    repository: Annotated[MetricsRepository, Depends(get_repository)],
    service: Annotated[
        str | None, Query(description="Filter by exact service name")
    ] = None,
    from_: Annotated[
        datetime | None,
        Query(alias="from", description="Inclusive start of the time range (ISO-8601)"),
    ] = None,
    to: Annotated[
        datetime | None,
        Query(description="Inclusive end of the time range (ISO-8601)"),
    ] = None,
) -> list[MetricResponse]:
    if from_ is not None and to is not None and from_ > to:
        raise HTTPException(status_code=400, detail="'from' must not be after 'to'")

    rows = repository.query(service=service, start=from_, end=to)
    return [MetricResponse.from_metric_row(row) for row in rows]
