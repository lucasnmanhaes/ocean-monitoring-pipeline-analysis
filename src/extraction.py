import os
import logging
import numpy as np
import pandas as pd
import xarray as xr

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# Monitoring points: tropical (Salvador), urbanized (Rio/Santos),
# extratropical southern locations (Florianópolis, Rio Grande)
LOCATIONS = {
    "salvador":       (-13.1, -38.5),
    "rio_de_janeiro": (-22.9, -43.2),
    "santos":         (-24.1, -46.3),
    "florianopolis":  (-27.6, -48.4),
    "rio_grande":     (-32.2, -51.2),
}

BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR    = os.path.join(BASE_DIR, "data", "raw")
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "processed")

PHYSICS_FILE = os.path.join(RAW_DIR, "physics_sst.nc")
WIND_FILE    = os.path.join(RAW_DIR, "wind_surface.nc")
WAVE_FILE    = os.path.join(RAW_DIR, "waves_swh.nc")

OUTPUT_FILE  = os.path.join(OUTPUT_DIR, "ocean_monitoring.csv")

# Waves come in at 3h resolution - resampling everything to match
# avoids inventing data points that the wave model never produced
RESAMPLE_FREQ = "3h"


def create_output_directory() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    log.info("Output directory ready: %s", OUTPUT_DIR)


def load_datasets() -> tuple[xr.Dataset, xr.Dataset, xr.Dataset]:
    # Fail early rather than a cryptic xarray error later
    for path in [PHYSICS_FILE, WIND_FILE, WAVE_FILE]:
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"File not found: {path}\n"
                "Run ingestion.py first to download the raw data."
            )

    log.info("Loading SST dataset...")
    ds_sst  = xr.open_dataset(PHYSICS_FILE)

    log.info("Loading wind dataset...")
    ds_wind = xr.open_dataset(WIND_FILE)

    log.info("Loading wave dataset...")
    ds_wave = xr.open_dataset(WAVE_FILE)

    log.info("All datasets loaded.")
    return ds_sst, ds_wind, ds_wave


def extract_point(ds: xr.Dataset, lat: float, lon: float) -> xr.Dataset:
    # method="nearest" walks the grid and picks the closest cell
    return ds.sel(latitude=lat, longitude=lon, method="nearest")


def compute_wind_speed(ds: xr.Dataset) -> xr.DataArray:
    # scalar magnitude from vector components: ||v|| = sqrt(u² + v²)
    # direction is dropped here; add atan2(v, u) downstream if needed
    return np.sqrt(ds["eastward_wind"] ** 2 + ds["northward_wind"] ** 2)


def resample_to_3h(da: xr.DataArray) -> xr.DataArray:
    # mean over each 3h window rather than just picking one sample
    return da.resample(time=RESAMPLE_FREQ).mean()


def extract_all_locations(
    ds_sst: xr.Dataset,
    ds_wind: xr.Dataset,
    ds_wave: xr.Dataset,
) -> pd.DataFrame:
    records = []

    for location, (lat, lon) in LOCATIONS.items():
        log.info("Extracting point: %s (lat=%.1f, lon=%.1f)", location, lat, lon)

        sst_point = extract_point(ds_sst, lat, lon)
        sst       = resample_to_3h(sst_point["thetao"])

        # thetao can come in Kelvin depending on the dataset version —
        # values above 100 are a safe indicator that conversion is needed
        if float(sst.mean()) > 100:
            sst = sst - 273.15
            log.info("  SST converted from Kelvin to Celsius.")

        wind_point  = extract_point(ds_wind, lat, lon)
        wind_speed  = compute_wind_speed(wind_point)
        wind_speed  = resample_to_3h(wind_speed)

        wave_point  = extract_point(ds_wave, lat, lon)
        wave_height = resample_to_3h(wave_point["VHM0"])

        # Take the intersection of all three time axes - datasets
        # don't always start and end at exactly the same timestamp
        common_times = (
            pd.Index(sst.time.values)
            .intersection(pd.Index(wind_speed.time.values))
            .intersection(pd.Index(wave_height.time.values))
        )

        if len(common_times) == 0:
            log.warning("  No overlapping timestamps for %s — skipping.", location)
            continue

        sst         = sst.sel(time=common_times)
        wind_speed  = wind_speed.sel(time=common_times)
        wave_height = wave_height.sel(time=common_times)

        log.info("  %d timesteps extracted.", len(common_times))

        df = pd.DataFrame({
            "location":      location,
            "latitude":      lat,
            "longitude":     lon,
            "datetime":      common_times,
            "sst_c":         sst.squeeze().values,
            "wind_speed_ms": wind_speed.squeeze().values,
            "wave_height_m": wave_height.squeeze().values,
        })

        records.append(df)

    if not records:
        raise ValueError("No data extracted for any location. Check the .nc files.")

    result = pd.concat(records, ignore_index=True)
    result["datetime"] = pd.to_datetime(result["datetime"])
    result = result.sort_values(["location", "datetime"]).reset_index(drop=True)

    return result


def validate(df: pd.DataFrame) -> None:
    # Physical plausibility checks — not exhaustive, but catches obvious issues
    # like a unit conversion that didn't fire or NaNs leaking from the grid edges
    log.info("Running validation checks...")

    null_counts = df.isnull().sum()
    if null_counts.any():
        log.warning("Null values found:\n%s", null_counts[null_counts > 0])

    sst_range = (df["sst_c"].min(), df["sst_c"].max())
    if not (-2 <= sst_range[0] and sst_range[1] <= 35):
        log.warning("SST out of expected range (-2 to 35°C): %s", sst_range)
    else:
        log.info("  SST range OK: %.2f to %.2f °C", *sst_range)

    wind_max = df["wind_speed_ms"].max()
    if wind_max > 50:
        log.warning("Wind speed unusually high: %.2f m/s — check units", wind_max)
    else:
        log.info("  Wind speed max OK: %.2f m/s", wind_max)

    wave_max = df["wave_height_m"].max()
    if wave_max > 20:
        log.warning("Wave height unusually high: %.2f m — possible land mask issue", wave_max)
    else:
        log.info("  Wave height max OK: %.2f m", wave_max)

    log.info("Validation complete.")
    log.info("Rows per location:\n%s", df["location"].value_counts())


def save_csv(df: pd.DataFrame) -> None:
    df.to_csv(OUTPUT_FILE, index=False)
    size_kb = os.path.getsize(OUTPUT_FILE) / 1024
    log.info("CSV saved -> %s (%.1f KB, %d rows)", OUTPUT_FILE, size_kb, len(df))


if __name__ == "__main__":
    log.info("Extraction pipeline starting...")

    create_output_directory()

    ds_sst, ds_wind, ds_wave = load_datasets()

    df = extract_all_locations(ds_sst, ds_wind, ds_wave)

    log.info("Extraction complete: %d rows, %d locations.",
             len(df), df["location"].nunique())
    log.info("Time range: %s -> %s", df["datetime"].min(), df["datetime"].max())

    validate(df)

    save_csv(df)

    log.info("Done. Next step: run load.py to insert into PostgreSQL.")