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
-- Canonical schema and indexes for the ``strikes`` table (see the class
-- docstring in ``blitzortung/db/table.py``).
--
-- IMPORTANT: this file mirrors the schema that is actually deployed in
-- production.  Keep it in sync with the live database (``\di``) and with
-- ``PRODUCTION_INDEXES`` in ``tests/db/test_db.py``; the test suite fails if
-- the index set here drifts from that list.  Proposed optimisations that are
-- NOT deployed live in ``docs/schema/proposed-indexes.sql`` and must be
-- validated against production query plans before they are moved here.
--
-- The statements are idempotent, so this file can be applied both to a fresh
-- database and to an existing one as a migration.

-- PostgreSQL/PostGIS extensions required by the schema.
CREATE EXTENSION IF NOT EXISTS postgis;
-- The ``strikes_timestamp_geog`` index mixes the timestamp with the geography
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
-- (ORDER BY "timestamp" DESC LIMIT 1) use this btree index.
CREATE INDEX IF NOT EXISTS strikes_timestamp ON strikes USING btree("timestamp");

-- Combined region/time-range queries.
CREATE INDEX IF NOT EXISTS strikes_region_timestamp ON strikes USING btree(region, "timestamp");

-- Spatial queries (grid and histogram envelope filters).  As a multicolumn
-- GiST index it can also serve predicates on the geography column alone.
CREATE INDEX IF NOT EXISTS strikes_timestamp_geog ON strikes USING gist("timestamp", geog);
