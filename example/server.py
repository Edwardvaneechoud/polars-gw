"""
Minimal FastAPI server to test gw-polars with a real Graphic Walker frontend.

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

import sys
from pathlib import Path

import polars as pl
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from gw_polars import execute_workflow, get_fields


def load_data() -> pl.DataFrame:
    """Load data from a file path (CLI arg) or generate sample data."""
    if len(sys.argv) > 1:
        path = Path(sys.argv[1])
        if not path.exists():
            print(f"  Error: file not found: {path}")
            sys.exit(1)
        suffix = path.suffix.lower()
        if suffix == ".csv":
            return pl.read_csv(path)
        elif suffix == ".parquet":
            return pl.read_parquet(path)
        elif suffix == ".json":
            return pl.read_json(path)
        elif suffix in (".xlsx", ".xls"):
            return pl.read_excel(path)
        else:
            print(f"  Error: unsupported file type: {suffix}")
            print("  Supported: .csv, .parquet, .json, .xlsx")
            sys.exit(1)

    # Default: generate sample data
    import datetime
    import random

    random.seed(42)
    return pl.DataFrame({
        "city": ["Amsterdam", "Berlin", "Paris", "London", "Madrid"] * 200,
        "category": ["Electronics", "Clothing", "Food", "Books", "Sports"] * 200,
        "sales": [round(random.uniform(50, 500), 2) for _ in range(1000)],
        "quantity": [random.randint(1, 100) for _ in range(1000)],
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
    result = execute_workflow(DF, request.model_dump())
    print(f"  compute: {len(request.workflow)} steps -> {len(result)} rows")
    return result


if __name__ == "__main__":
    source = sys.argv[1] if len(sys.argv) > 1 else "sample data"
    print(f"\n  Source:  {source}")
    print(f"  Dataset: {DF.shape[0]} rows x {DF.shape[1]} columns")
    print(f"  Fields:  {[f['fid'] for f in FIELDS]}")
    print("\n  Backend running on http://localhost:8787")
    print("  Frontend should be on http://localhost:5177\n")
    uvicorn.run(app, host="0.0.0.0", port=8787)
