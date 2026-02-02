-- Create the table of WKB values and insert test values.
DROP TABLE IF EXISTS geodata;
CREATE TABLE geodata(wkb BINARY) USING parquet;
-- See: https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry#Well-known_binary
-- to understand the formatting/layout of the input Well-Known Binary (WKB) values.
INSERT INTO geodata VALUES
(NULL),
(X'0101000000000000000000F03F0000000000000040');

--- Casting geospatial data types

-- GEOGRAPHY and GEOMETRY data types cannot be cast to/from other data types.
SELECT CAST(ST_GeogFromWKB(X'0101000000000000000000f03f0000000000000040') AS STRING) AS result;
SELECT CAST(X'0101000000000000000000f03f0000000000000040' AS GEOMETRY(4326)) AS result;

-- Casting GEOGRAPHY(<srid>) to GEOGRAPHY(ANY) is allowed.
SELECT hex(ST_AsBinary(CAST(ST_GeogFromWKB(X'0101000000000000000000f03f0000000000000040') AS GEOGRAPHY(ANY)))) AS result;
-- Casting GEOGRAPHY(ANY) to GEOGRAPHY(<srid>) is not allowed.
SELECT CAST(ST_GeogFromWKB(X'0101000000000000000000f03f0000000000000040')::GEOGRAPHY(ANY) AS GEOGRAPHY(4326)) AS result;

-- Casting GEOGRAPHY to GEOMETRY is allowed only if SRIDs match.
SELECT hex(ST_AsBinary(CAST(ST_GeogFromWKB(X'0101000000000000000000f03f0000000000000040') AS GEOMETRY(4326)))) AS result;
-- Error handling: mismatched SRIDs.
SELECT CAST(ST_GeogFromWKB(X'0101000000000000000000f03f0000000000000040') AS GEOMETRY(ANY)) AS result;

-- Casting GEOMETRY(<srid>) to GEOMETRY(ANY) is allowed.
SELECT hex(ST_AsBinary(CAST(ST_GeomFromWKB(X'0101000000000000000000f03f0000000000000040') AS GEOMETRY(ANY)))) AS result;
-- Casting GEOMETRY(ANY) to GEOMETRY(<srid>) is not allowed.
SELECT CAST(ST_GeomFromWKB(X'0101000000000000000000f03f0000000000000040')::GEOMETRY(ANY) AS GEOMETRY(4326)) AS result;

---- Geospatial type coercion

-- Array
SELECT typeof(array(ST_GeogFromWKB(wkb), ST_GeogFromWKB(wkb)::GEOGRAPHY(ANY))) FROM geodata;
SELECT typeof(array(ST_GeomFromWKB(wkb), ST_GeomFromWKB(wkb)::GEOMETRY(ANY))) FROM geodata;
-- Map
SELECT typeof(map('a', ST_GeogFromWKB(wkb), 'b', ST_GeogFromWKB(wkb)::GEOGRAPHY(ANY))) FROM geodata;
SELECT typeof(map('a', ST_GeomFromWKB(wkb), 'b', ST_GeomFromWKB(wkb)::GEOMETRY(ANY))) FROM geodata;
-- Struct
SELECT typeof(array(named_struct('g1', ST_GeogFromWKB(wkb), 'g2', ST_GeogFromWKB(wkb)::GEOGRAPHY(ANY)), named_struct('g1', ST_GeogFromWKB(wkb)::GEOGRAPHY(ANY), 'g2', ST_GeogFromWKB(wkb)))) FROM geodata;
SELECT typeof(array(named_struct('g1', ST_GeomFromWKB(wkb), 'g2', ST_GeomFromWKB(wkb)::GEOMETRY(ANY)), named_struct('g1', ST_GeomFromWKB(wkb)::GEOMETRY(ANY), 'g2', ST_GeomFromWKB(wkb)))) FROM geodata;
-- Nested
SELECT typeof(named_struct('a', array(ST_GeogFromWKB(wkb), ST_GeogFromWKB(wkb)::GEOGRAPHY(ANY)), 'b', map('g', ST_GeogFromWKB(wkb), 'h', ST_GeogFromWKB(wkb)::GEOGRAPHY(ANY)))) FROM geodata;
SELECT typeof(named_struct('a', array(ST_GeomFromWKB(wkb), ST_GeomFromWKB(wkb)::GEOMETRY(ANY)), 'b', map('g', ST_GeomFromWKB(wkb), 'h', ST_GeomFromWKB(wkb)::GEOMETRY(ANY)))) FROM geodata;

-- NVL
SELECT typeof(nvl(ST_GeogFromWKB(wkb), ST_GeogFromWKB(wkb)::GEOGRAPHY(ANY))) FROM geodata;
SELECT typeof(nvl(ST_GeomFromWKB(wkb), ST_GeomFromWKB(wkb)::GEOMETRY(ANY))) FROM geodata;
-- NVL2
SELECT typeof(nvl2(ST_GeogFromWKB(wkb), ST_GeogFromWKB(wkb)::GEOGRAPHY(ANY), ST_GeogFromWKB(wkb))) FROM geodata;
SELECT typeof(nvl2(ST_GeomFromWKB(wkb), ST_GeomFromWKB(wkb)::GEOMETRY(ANY), ST_GeomFromWKB(wkb))) FROM geodata;
-- CASE WHEN
SELECT typeof(CASE WHEN wkb IS NOT NULL THEN ST_GeogFromWKB(wkb)::GEOGRAPHY(ANY) ELSE ST_GeogFromWKB(wkb) END) FROM geodata;
SELECT typeof(CASE WHEN wkb IS NOT NULL THEN ST_GeomFromWKB(wkb)::GEOMETRY(ANY) ELSE ST_GeomFromWKB(wkb) END) FROM geodata;
-- COALESCE
SELECT typeof(coalesce(ST_GeogFromWKB(wkb), ST_GeogFromWKB(wkb)::GEOGRAPHY(ANY))) FROM geodata;
SELECT typeof(coalesce(ST_GeomFromWKB(wkb), ST_GeomFromWKB(wkb)::GEOMETRY(ANY))) FROM geodata;
-- IF
SELECT typeof(IF(wkb IS NOT NULL, ST_GeogFromWKB(wkb)::GEOGRAPHY(ANY), ST_GeogFromWKB(wkb))) FROM geodata;
SELECT typeof(IF(wkb IS NOT NULL, ST_GeomFromWKB(wkb)::GEOMETRY(ANY), ST_GeomFromWKB(wkb))) FROM geodata;

---- ST reader/writer expressions

-- WKB (Well-Known Binary) round-trip tests for GEOGRAPHY and GEOMETRY types.
SELECT hex(ST_AsBinary(ST_GeogFromWKB(X'0101000000000000000000f03f0000000000000040'))) AS result;
SELECT hex(ST_AsBinary(ST_GeomFromWKB(X'0101000000000000000000f03f0000000000000040'))) AS result;

------ ST accessor expressions

---- ST_Srid

-- 1. Driver-level queries.
SELECT ST_Srid(NULL);
SELECT ST_Srid(ST_GeogFromWKB(X'0101000000000000000000F03F0000000000000040'));
SELECT ST_Srid(ST_GeomFromWKB(X'0101000000000000000000F03F0000000000000040'));

-- 2. Table-level queries.
SELECT COUNT(*) FROM geodata WHERE ST_Srid(ST_GeogFromWKB(wkb)) <> 4326;
SELECT COUNT(*) FROM geodata WHERE ST_Srid(ST_GeomFromWKB(wkb)) <> 0;

------ ST modifier expressions

---- ST_SetSrid

-- 1. Driver-level queries.
SELECT ST_Srid(ST_SetSrid(ST_GeogFromWKB(X'0101000000000000000000F03F0000000000000040'), 4326));
SELECT ST_Srid(ST_SetSrid(ST_GeomFromWKB(X'0101000000000000000000F03F0000000000000040'), 3857));
-- Error handling: invalid SRID.
SELECT ST_Srid(ST_SetSrid(ST_GeogFromWKB(X'0101000000000000000000F03F0000000000000040'), 3857));
SELECT ST_Srid(ST_SetSrid(ST_GeomFromWKB(X'0101000000000000000000F03F0000000000000040'), 9999));

-- 2. Table-level queries.
SELECT COUNT(*) FROM geodata WHERE ST_Srid(ST_SetSrid(ST_GeogFromWKB(wkb), 4326)) <> 4326;
SELECT COUNT(*) FROM geodata WHERE ST_Srid(ST_SetSrid(ST_GeomFromWKB(wkb), 3857)) <> 3857;
-- Error handling: invalid SRID.
SELECT COUNT(*) FROM geodata WHERE ST_Srid(ST_SetSrid(ST_GeogFromWKB(wkb), 3857)) IS NOT NULL;
SELECT COUNT(*) FROM geodata WHERE ST_Srid(ST_SetSrid(ST_GeomFromWKB(wkb), 9999)) IS NOT NULL;

-- Drop the test table.
DROP TABLE geodata;
