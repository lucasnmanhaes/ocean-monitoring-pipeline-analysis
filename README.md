# Ocean Environmental Monitoring Pipeline
# Satellite-Based Coastal Analysis

## Language

This README is written in English. A Portuguese summary is available at the end of the document.

Este README está em inglês. Um resumo em português está disponível ao final do documento.

## Overview

This project builds an end-to-end ocean monitoring pipeline using satellite-derived data to detect and analyse potentially critical coastal conditions.

It integrates:

Sea Surface Temperature (SST)
Surface Wind Speed
Significant Wave Height

The pipeline processes raw satellite data into structured datasets and enables spatial and temporal analysis across key coastal regions in Brazil.

---

## Data

The data/ directory is empty in this repository due to the large size of the NetCDF files used in this project.

All data is automatically retrieved through the ingestion pipeline:

ingestion.py

---

## Problem Statement

How can we monitor and detect potentially critical ocean conditions (temperature, wind, and waves) in strategic coastal regions using satellite data?

This project addresses:

Ocean warming (SST anomalies)
Atmospheric forcing (wind dynamics)
Coastal impact drivers (wave height)

---

## Applications:

Coastal risk assessment
Port and offshore operations
Extreme event detection
Environmental monitoring

---

## Study Area

The analysis focuses on five strategic coastal regions in Brazil:

- Tropical (Salvador)
- Major port regions (Rio de Janeiro, Santos)
- High-energy southern coast (Florianópolis, Rio Grande)

These locations were selected to capture different oceanographic regimes.

---

## Architecture
```
Copernicus Marine API
        ↓
[ ingestion.py ]
        ↓
Raw NetCDF (.nc)
        ↓
[ extraction.py ]
        ↓
Processed CSV
        ↓
[ load.py ]
        ↓
PostgreSQL Database
        ↓
Analysis (Jupyter Notebooks)
```
---

## Database Schema

The database structure is defined in:

sql/schema.sql

---

## Pipeline Description
1. Data Ingestion

Script: ingestion.py
Source: Copernicus Marine Service

Downloads three datasets:

- SST - GLOBAL_ANALYSISFORECAST_PHY_001_024 (~9 km, hourly)
- Wind - WIND_GLO_PHY_L4_NRT_012_004 — KNMI scatterometer L4, bias-corrected with ECMWF (0.125°, hourly)
- Waves - GLOBAL_ANALYSISFORECAST_WAV_001_027 (~9 km, 3-hourly)

Data Considerations

Although this project uses analysis/forecast products, they are fully suitable for environmental monitoring and exploratory analysis.

Key considerations:

- These datasets are model-based and assimilated products, not pure in-situ observations
- They provide consistent spatial and temporal coverage, ideal for large-scale monitoring
- They are widely used in operational oceanography

Key features:

- Geographic subsetting (Brazilian coast)
- Time filtering (2024–2025)
- Automated validation of downloads

See implementation:

2. Data Processing & Feature Engineering

Script: extraction.py

Main steps:

- Extract nearest grid point for each location
- Convert SST from Kelvin → Celsius (if needed)
- Compute wind speed: U = sqrt(u^2 + v^2)
- Resample all variables to 3-hour resolution
- Align datasets by common timestamps
- Build unified dataset

Output:

location | datetime | sst_c | wind_speed_ms | wave_height_m

Includes:

- Physical validation checks (range sanity)
- Missing data detection
- Sorting and structuring

See implementation:

3. Data Loading (Database)

Script: load.py

- PostgreSQL integration via SQLAlchemy
Normalized schema:
```
locations

id
name
latitude
longitude

measurements

location_id
datetime
sst_c
wind_speed_ms
wave_height_m
```
Features:

- Deduplication
- Indexing for performance
- Batch inserts (optimized)

See implementation:

4. Analysis

Conducted in Jupyter Notebooks:

- 01_data_validation.ipynb
- 02_ocean_analysis.ipynb

Includes:

- Spatial distribution of extreme events
- SST vs event probability analysis
- Regional comparisons (tropical vs southern regimes)
- Detection of compound conditions
- Interactive map available in: notebooks/interactive_map.html
Note: GitHub does not render interactive maps (Folium/JavaScript). Please download and open the file locally to view the full visualization.

---

## Tech Stack
- Python (pandas, numpy, xarray)
- Copernicus Marine API
- PostgreSQL + SQLAlchemy
- Jupyter Notebook
- NetCDF (scientific data format)

---

## Key Insights
Southern regions (Florianópolis, Rio Grande):
- Higher frequency of extreme conditions
- Strong link to low SST + high-energy systems

Tropical region (Salvador):
- More stable conditions
- Lower variability
- No universal SST–event relationship:
- Indicates regional ocean dynamics dominance

---

## How to Run
```
1. Install dependencies
pip install -r requirements.txt
2. Authenticate Copernicus
copernicusmarine login
3. Run pipeline
python ingestion.py
python extraction.py
python load.py
4. Run analysis
jupyter notebook
```

---

## Project Structure
```
ocean-monitoring-pipeline-analysis
│
├── data/
│   ├── raw/
│   └── processed/
│
├── notebooks/
│   ├── 01_data_validation.ipynb
│   └── 02_ocean_analysis.ipynb
│
├── src/
│   ├── ingestion.py
│   ├── extraction.py
│   └── load.py
│
├── README.md
└── requirements.txt
```
---

## Data Sources
- Copernicus Marine Service (CMEMS)
- Global Ocean Physics
- Wind Observations (KNMI scatterometer)
- Wave Model Outputs

---

## Future Improvements
- Add anomaly detection (z-score / ML models)
- Real-time pipeline (streaming ingestion)
- Integration with coastal risk indices
- Wave direction and spectral analysis

---

## Resumo (Português)
Este projeto desenvolve um pipeline completo para monitoramento ambiental oceânico utilizando dados satelitais.

A solução integra variáveis físicas essenciais — temperatura da superfície do mar (SST), vento e altura de ondas — para identificar condições potencialmente críticas em regiões costeiras do Brasil.

O pipeline realiza:

Ingestão de dados via Copernicus Marine Service
Processamento e padronização dos dados
Extração por pontos geográficos estratégicos
Armazenamento em banco de dados PostgreSQL
Análise espacial e temporal dos eventos

O objetivo é demonstrar como dados oceanográficos podem ser transformados em informação estruturada para suporte à tomada de decisão em contextos ambientais e operacionais.

**Observação sobre os dados**

A pasta data/ está vazia no repositório devido ao grande volume dos arquivos NetCDF utilizados no projeto.

Os dados são obtidos automaticamente através do pipeline de ingestão:

ingestion.py

---

## Author
Lucas Manhaes<br>
Oceanography | Data Science | Data Analyst

---

## Final Note

This project bridges oceanography and data engineering, demonstrating how satellite data can be transformed into actionable insights for coastal monitoring and decision-making.