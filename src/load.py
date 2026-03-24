import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# Database connection

engine = create_engine(
    "postgresql+psycopg2://USER:PASSWORD@localhost:5432/ocean_monitoring"
)


# Paths

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_FILE = os.path.join(BASE_DIR, "data", "processed", "ocean_monitoring.csv")


# Create tables

def create_tables() -> None:
    ddl = """

        CREATE TABLE IF NOT EXISTS locations (
            id         SERIAL PRIMARY KEY,
            name       TEXT    NOT NULL UNIQUE,
            latitude   FLOAT   NOT NULL,
            longitude  FLOAT   NOT NULL
        );

        CREATE TABLE IF NOT EXISTS measurements (
            id             SERIAL PRIMARY KEY,
            location_id    INT         NOT NULL REFERENCES locations(id),
            datetime       TIMESTAMP   NOT NULL,
            sst_c          FLOAT,
            wind_speed_ms  FLOAT,
            wave_height_m  FLOAT
            UNIQUE (location_id, datetime)
        );

        CREATE INDEX IF NOT EXISTS idx_measurements_datetime
        ON measurements(datetime);

        CREATE INDEX IF NOT EXISTS idx_measurements_location
        ON measurements(location_id);
    """
    with engine.connect() as conn:
        conn.execute(text(ddl))
        conn.commit()
    log.info("Tables 'locations' and 'measurements' ready.")


# Load CSV files

def load_csv() -> pd.DataFrame:
    if not os.path.exists(CSV_FILE):
        raise FileNotFoundError(
            f"File not found: {CSV_FILE}\n"
            "Run extraction.py first."
        )
    df = pd.read_csv(CSV_FILE, parse_dates=["datetime"])
    log.info("CSV loaded: %d rows, %d locations.", len(df), df["location"].nunique())
    return df


# Insert locations

def insert_locations(df: pd.DataFrame) -> dict:
    # inserts each unique location and returns a name
    unique = df[["location", "latitude", "longitude"]].drop_duplicates()
    location_ids = {}

    with engine.connect() as conn:
        for _, row in unique.iterrows():
            result = conn.execute(text("""
                INSERT INTO locations (name, latitude, longitude)
                VALUES (:name, :lat, :lon)
                ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                RETURNING id
            """), {"name": row["location"], "lat": row["latitude"], "lon": row["longitude"]})
            location_ids[row["location"]] = result.scalar()
        conn.commit()

    log.info("Locations inserted: %s", list(location_ids.keys()))
    return location_ids


# Insert data into database

def insert_measurements(df: pd.DataFrame, location_ids: dict) -> None:
    measurements = df.copy()
    measurements["location_id"] = measurements["location"].map(location_ids)
    measurements = measurements.drop(columns=["location", "latitude", "longitude"])
    measurements = measurements.drop_duplicates(subset=["location_id", "datetime"])

    measurements.to_sql(
        "measurements",
        engine,
        if_exists="append",
        index=False,
        method="multi",  # batches rows into fewer round-trips (faster for large datasets)
        chunksize=1000,
    )
    log.info("Inserted %d rows into 'measurements'.", len(measurements))


def verify() -> None:
    with engine.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM measurements")).scalar()
        rows = conn.execute(text("""
            SELECT l.name, COUNT(m.id)
            FROM measurements m
            JOIN locations l ON l.id = m.location_id
            GROUP BY l.name
            ORDER BY l.name
        """)).fetchall()

    log.info("Total rows in measurements: %d", total)
    for name, count in rows:
        log.info("  %-20s %d rows", name, count)


# Entry point

if __name__ == "__main__":
    log.info("Load pipeline starting...")

    create_tables()

    df = load_csv()

    location_ids = insert_locations(df)

    insert_measurements(df, location_ids)

    verify()

    log.info("Done. Database is ready for analysis.")