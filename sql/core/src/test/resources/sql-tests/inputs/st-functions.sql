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
