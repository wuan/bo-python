-- -*- coding: utf8 -*-
--
--   Copyright 2025 Andreas Würl
--
--   Licensed under the Apache License, Version 2.0 (the "License");
--   you may not use this file except in compliance with the License.
--   You may obtain a copy of the License at
--
--       http://www.apache.org/licenses/LICENSE-2.0
--
--   Unless required by applicable law or agreed to in writing, software
--   distributed under the License is distributed on an "AS IS" BASIS,
--   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--   See the License for the specific language governing permissions and
--   limitations under the License.
--
-- Canonical schema and index/maintenance configuration for the ``strikes``
-- table (see the class docstring in ``blitzortung/db/table.py``).
--
-- The table is append-only: rows are inserted once and are practically never
-- updated or deleted.  That makes it a good fit for BRIN indexes and for
-- autovacuum settings tuned to keep planner statistics fresh.
--
-- The statements are idempotent, so this file can be applied both to a fresh
-- database and to an existing one as a migration.

-- PostgreSQL/PostGIS extensions required by the schema.
CREATE EXTENSION IF NOT EXISTS postgis;
-- The composite gist indexes below mix the timestamp with the geography
-- column, which requires the btree_gist extension.
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE IF NOT EXISTS strikes (
    id          bigserial,
    "timestamp" timestamptz,
    nanoseconds SMALLINT,
    geog        GEOGRAPHY(Point),
    PRIMARY KEY (id)
);
ALTER TABLE strikes ADD COLUMN IF NOT EXISTS altitude SMALLINT;
ALTER TABLE strikes ADD COLUMN IF NOT EXISTS region SMALLINT;
ALTER TABLE strikes ADD COLUMN IF NOT EXISTS amplitude REAL;
ALTER TABLE strikes ADD COLUMN IF NOT EXISTS error2d SMALLINT;
ALTER TABLE strikes ADD COLUMN IF NOT EXISTS stationcount SMALLINT;

-- Time-range queries (URL de-duplication, histogram) and get_latest_time()
-- (ORDER BY "timestamp" DESC LIMIT 1) use the btree index.
CREATE INDEX IF NOT EXISTS strikes_timestamp ON strikes USING btree("timestamp");

-- Combined region/time-range queries.
CREATE INDEX IF NOT EXISTS strikes_region_timestamp ON strikes USING btree(region, "timestamp");
CREATE INDEX IF NOT EXISTS strikes_region_timestamp_nanoseconds
    ON strikes USING btree(region, "timestamp", nanoseconds);

-- Spatial queries.
CREATE INDEX IF NOT EXISTS strikes_geog ON strikes USING gist(geog);
CREATE INDEX IF NOT EXISTS strikes_timestamp_geog ON strikes USING gist("timestamp", geog);
CREATE INDEX IF NOT EXISTS strikes_id_timestamp ON strikes USING btree(id, "timestamp");
CREATE INDEX IF NOT EXISTS strikes_id_timestamp_geog ON strikes USING gist(id, "timestamp", geog);

-- A BRIN index on the append-only timestamp column.  It is orders of magnitude
-- smaller than the btree and speeds up the large range scans used for
-- de-duplication and histograms as long as the physical row order correlates
-- with time (which it does for an insert-only table).
CREATE INDEX IF NOT EXISTS strikes_timestamp_brin
    ON strikes USING brin("timestamp") WITH (pages_per_range = 32);

-- Keep statistics current for the time-range planner estimates.  The default
-- analyze scale factor (10%) is too coarse for a rapidly growing table.
ALTER TABLE strikes SET (
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_analyze_threshold = 1000
);
