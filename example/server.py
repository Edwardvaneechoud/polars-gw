"""
Minimal FastAPI server to test polars-gw with a real Graphic Walker frontend.

Usage:
    # Terminal 1: start the backend
    pip install fastapi uvicorn
    cd gw_polars/example

    python server.py                          # uses sample data (1000 rows)
    python server.py /path/to/data.csv        # load a CSV file
    python server.py /path/to/data.parquet    # load a Parquet file
    python server.py /path/to/data.json       # load a JSON file

    # Terminal 2: start the frontend
    cd gw_polars/example
    npm install
    npm run dev

    # Open http://localhost:5177 in your browser
"""

import logging
import sys
from pathlib import Path

import polars as pl
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from gw_polars import execute_workflow, get_fields

logger = logging.getLogger("gw_polars.example")


def load_data() -> pl.DataFrame:
    """Load data from a file path (CLI arg) or generate sample data."""
    if len(sys.argv) > 1:
        path = Path(sys.argv[1])
        if not path.exists():
            logger.error("File not found: %s", path)
            sys.exit(1)
        suffix = path.suffix.lower()
        logger.info("Loading %s (%s)", path, suffix)
        if suffix == ".csv":
            return pl.read_csv(path)
        elif suffix == ".parquet":
            return pl.read_parquet(path)
        elif suffix == ".json":
            return pl.read_json(path)
        elif suffix in (".xlsx", ".xls"):
            return pl.read_excel(path)
        else:
            logger.error("Unsupported file type: %s (supported: .csv, .parquet, .json, .xlsx)", suffix)
            sys.exit(1)

    # Default: generate sample data
    import datetime
    import random

    random.seed(42)
    cities = ["Amsterdam", "Berlin", "Paris", "London", "Madrid"] * 200
    # Rough city centres (lat, lng) — add jitter so points spread around each city
    city_coords = {
        "Amsterdam": (52.3676, 4.9041),
        "Berlin": (52.5200, 13.4050),
        "Paris": (48.8566, 2.3522),
        "London": (51.5074, -0.1278),
        "Madrid": (40.4168, -3.7038),
    }
    latitudes = [round(city_coords[c][0] + random.uniform(-0.15, 0.15), 4) for c in cities]
    longitudes = [round(city_coords[c][1] + random.uniform(-0.15, 0.15), 4) for c in cities]
    return pl.DataFrame({
        "city": cities,
        "category": ["Electronics", "Clothing", "Food", "Books", "Sports"] * 200,
        "sales": [round(random.uniform(50, 500), 2) for _ in range(1000)],
        "quantity": [random.randint(1, 100) for _ in range(1000)],
        "latitude": latitudes,
        "longitude": longitudes,
        "date": pl.date_range(
            datetime.date(2023, 1, 1), datetime.date(2025, 9, 30), eager=True
        ).sample(1000, with_replacement=True, seed=42).sort(),
    })


app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

DF = load_data()
FIELDS = get_fields(DF)


class ComputeRequest(BaseModel):
    workflow: list = []
    limit: int | None = None
    offset: int | None = None


@app.post("/api/fields")
def api_fields():
    return FIELDS


@app.post("/api/compute")
def api_compute(request: ComputeRequest):
    return execute_workflow(DF, request.model_dump())


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    source = sys.argv[1] if len(sys.argv) > 1 else "sample data"
    logger.info("Source:  %s", source)
    logger.info("Dataset: %d rows x %d columns", *DF.shape)
    logger.info("Fields:  %s", [f["fid"] for f in FIELDS])
    logger.info("Backend running on http://localhost:8787")
    logger.info("Frontend should be on http://localhost:5177")
    uvicorn.run(app, host="0.0.0.0", port=8787)
