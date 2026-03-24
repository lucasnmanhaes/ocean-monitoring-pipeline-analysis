import os
import logging
import copernicusmarine
 
# Logging — timestamps + level prefix for easy debugging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)
 
 
# Dataset 1: SST
# Source: GLOBAL_ANALYSISFORECAST_PHY_001_024 (~9 km, hourly)
# NOTE: uo/vo in this dataset are ocean currents, not wind — excluded intentionally.

PHYSICS_DATASET_ID = "cmems_mod_glo_phy_anfc_0.083deg_PT1H-m"
PHYSICS_VARIABLES  = ["thetao"]   # SST (potential temperature)
PHYSICS_FILENAME   = "physics_sst.nc"
 
# Dataset 2: Atmospheric surface wind
# Source: WIND_GLO_PHY_L4_NRT_012_004 — KNMI scatterometer L4, bias-corrected with ECMWF (0.125°, hourly)
# wind_speed = sqrt(eastward_wind**2 + northward_wind**2) is computed in the next step

WIND_DATASET_ID  = "cmems_obs-wind_glo_phy_nrt_l4_0.125deg_PT1H"
WIND_VARIABLES   = ["eastward_wind", "northward_wind"]
WIND_FILENAME    = "wind_surface.nc"
 
# Dataset 3: Significant wave height
# Source: GLOBAL_ANALYSISFORECAST_WAV_001_027 (~9 km, 3-hourly)
# VHM0 is the correct variable name here, not "swh"

WAVE_DATASET_ID  = "cmems_mod_glo_wav_anfc_0.083deg_PT3H-i"
WAVE_VARIABLES   = ["VHM0"]
WAVE_FILENAME    = "waves_swh.nc"
 
 
# Geographic bounding box — Brazilian coastline
# Covering monitoring points: Salvador, Rio de Janeiro, Santos, Florianópolis, Rio Grande do Sul
MIN_LONGITUDE = -55.0
MAX_LONGITUDE = -35.0
MIN_LATITUDE  = -35.0
MAX_LATITUDE  = -10.0
 
# Time range — keep it narrow for initial testing, expand for full analysis
START_DATE = "2024-01-01"
END_DATE   = "2025-12-31"
 
# Output directory
BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "raw")
 
 
# Utility functions
 
def create_output_directory() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    log.info("Output directory ready: %s", OUTPUT_DIR)
 
 
def test_connection() -> bool:
    log.info("Testing connection to Copernicus Marine Service...")
    try:
        copernicusmarine.describe(contains=["cmems_mod_glo_phy_anfc"])
        log.info("Connection OK.")
        return True
    except Exception as exc:
        log.error("Connection failed: %s", exc)
        log.error("Re-authenticate with: copernicusmarine login")
        return False
 
 
def _subset(dataset_id: str, variables: list, filename: str, label: str) -> None:
    """Shared download wrapper — all datasets use the same bbox and time range."""
    log.info("%s | %s | %s -> %s", label, dataset_id, START_DATE, END_DATE)
    log.info("No output for 1-2 min is normal — API processes subset server-side before streaming.")
 
    try:
        copernicusmarine.subset(
            dataset_id=dataset_id,
            variables=variables,
            minimum_longitude=MIN_LONGITUDE,
            maximum_longitude=MAX_LONGITUDE,
            minimum_latitude=MIN_LATITUDE,
            maximum_latitude=MAX_LATITUDE,
            start_datetime=START_DATE,
            end_datetime=END_DATE,
            output_directory=OUTPUT_DIR,
            output_filename=filename,
            overwrite=True,
        )
        log.info("%s download complete -> %s", label, os.path.join(OUTPUT_DIR, filename))
    except Exception as exc:
        log.error("%s download failed: %s", label, exc)
        raise
 
 
# Individual download functions
 
def download_sst() -> None:
    _subset(PHYSICS_DATASET_ID, PHYSICS_VARIABLES, PHYSICS_FILENAME, "SST")
 
 
def download_wind() -> None:
    _subset(WIND_DATASET_ID, WIND_VARIABLES, WIND_FILENAME, "Wind")
 
 
def download_waves() -> None:
    _subset(WAVE_DATASET_ID, WAVE_VARIABLES, WAVE_FILENAME, "Waves")
 
 
# Post-download verification
 
def verify_outputs() -> None:
    log.info("Verifying output files...")
    all_ok = True
 
    for filename in [PHYSICS_FILENAME, WIND_FILENAME, WAVE_FILENAME]:
        path = os.path.join(OUTPUT_DIR, filename)
        if os.path.exists(path):
            size_mb = os.path.getsize(path) / (1024 ** 2)
            status = "OK" if size_mb > 0 else "EMPTY"
            log.info("  [%s]  %-35s  %.2f MB", status, filename, size_mb)
            if size_mb == 0:
                all_ok = False
        else:
            log.warning("  [MISSING]  %s", filename)
            all_ok = False
 
    if not all_ok:
        log.warning("One or more files are missing or empty.")
 
 

# Entry point

 
if __name__ == "__main__":
    log.info("Ingestion pipeline starting — %s to %s", START_DATE, END_DATE)
 
    create_output_directory()
 
    # Abort early if credentials or network are not working
    if not test_connection():
        raise SystemExit(1)
 
    # Run all three downloads sequentially
    download_sst()
    download_wind()
    download_waves()
 
    # Confirm all files exist and are non-empty
    verify_outputs()
 
    log.info("Ingestion complete")