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
    wave_height_m  FLOAT,
    UNIQUE (location_id, datetime)
);

CREATE INDEX IF NOT EXISTS idx_measurements_datetime
ON measurements(datetime);

CREATE INDEX IF NOT EXISTS idx_measurements_location
ON measurements(location_id);