"""
Spotify ETL Pipeline
====================
Extracts raw Spotify track data from CSV,
transforms and cleans it using pandas,
and loads it into a PostgreSQL database via SQLAlchemy.

Author: Dharmik Champaneri
"""

import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ── Configuration ──────────────────────────────────────────────

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

DATA_PATH = os.path.join(os.path.dirname(__file__), "../data/spotify_tracks.csv")

DB_URL = (
    f"postgresql+psycopg2://"
    f"{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}"
    f"/{os.getenv('DB_NAME')}"
)

TABLE_NAME = "spotify_tracks"


# ── Extract ────────────────────────────────────────────────────

def extract(filepath: str) -> pd.DataFrame:
    """Load raw Spotify track data from a CSV file."""
    logger.info(f"Extracting data from: {filepath}")
    df = pd.read_csv(filepath)
    logger.info(f"Extracted {len(df):,} rows and {len(df.columns)} columns.")
    return df


# ── Transform ──────────────────────────────────────────────────

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and transform raw Spotify track data."""
    logger.info("Starting transformation...")

    initial_rows = len(df)

    # Drop duplicates
    df = df.drop_duplicates(subset="track_id")
    logger.info(f"Removed {initial_rows - len(df):,} duplicate rows.")

    # Drop rows missing critical fields
    df = df.dropna(subset=["track_id", "track_name", "artist_name"])

    # Normalise string columns
    str_cols = ["track_name", "artist_name", "album"]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Convert duration from milliseconds to minutes
    if "duration_ms" in df.columns:
        df["duration_min"] = (df["duration_ms"] / 60000).round(2)
        df = df.drop(columns=["duration_ms"])

    # Parse release date
    if "release_date" in df.columns:
        df["release_date"] = pd.to_datetime(
            df["release_date"], errors="coerce"
        )

    # Enforce types
    if "popularity" in df.columns:
        df["popularity"] = (
            pd.to_numeric(df["popularity"], errors="coerce")
            .fillna(0)
            .astype(int)
        )

    if "explicit" in df.columns:
        df["explicit"] = df["explicit"].astype(bool)

    logger.info(f"Transformation complete. {len(df):,} rows ready to load.")
    return df


# ── Load ───────────────────────────────────────────────────────

def load(df: pd.DataFrame, db_url: str, table_name: str) -> None:
    """Load transformed data into PostgreSQL database."""
    logger.info("Connecting to database...")
    engine = create_engine(db_url)

    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    logger.info("Database connection successful.")

    logger.info(f"Loading {len(df):,} rows into '{table_name}'...")
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=500,
    )
    logger.info(f"Load complete. {len(df):,} rows written to '{table_name}'.")


# ── Pipeline ───────────────────────────────────────────────────

def run_pipeline():
    """Run the full ETL pipeline."""
    logger.info("=" * 50)
    logger.info("Spotify ETL Pipeline — Starting")
    logger.info("=" * 50)

    raw_df = extract(DATA_PATH)
    clean_df = transform(raw_df)
    load(clean_df, DB_URL, TABLE_NAME)

    logger.info("=" * 50)
    logger.info("Pipeline completed successfully.")
    logger.info("=" * 50)


if __name__ == "__main__":
    run_pipeline()
