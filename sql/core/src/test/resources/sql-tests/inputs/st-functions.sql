-- Create the table of WKB values and insert test values.
DROP TABLE IF EXISTS geodata;
CREATE TABLE geodata(wkb BINARY) USING parquet;
-- See: https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry#Well-known_binary
-- to understand the formatting/layout of the input Well-Known Binary (WKB) values.
INSERT INTO geodata VALUES
(NULL),
(X'0101000000000000000000F03F0000000000000040');

-- Create a table of invalid WKB values and insert test data.
DROP TABLE IF EXISTS geoinvaliddata;
CREATE TABLE geoinvaliddata(wkb BINARY) USING parquet;
-- See: https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry#Well-known_binary
-- to understand the formatting/layout of the input Well-Known Binary (WKB) values.
INSERT INTO geoinvaliddata VALUES
(X'010203');

--- Casting geospatial data types

-- GEOGRAPHY and GEOMETRY data types cannot be cast to/from other data types.
SELECT CAST(ST_GeogFromWKB(X'0101000000000000000000f03f0000000000000040') AS STRING) AS result;
SELECT CAST(X'0101000000000000000000f03f0000000000000040' AS GEOMETRY(4326)) AS result;

---- ST reader/writer expressions

-- WKB (Well-Known Binary) round-trip tests for GEOGRAPHY and GEOMETRY types.
SELECT hex(ST_AsBinary(ST_GeogFromWKB(X'0101000000000000000000f03f0000000000000040'))) AS result;
SELECT hex(ST_AsBinary(ST_GeomFromWKB(X'0101000000000000000000f03f0000000000000040'))) AS result;

---- ST_GeogFromWKB

-- 1. Driver-level queries.
SELECT ST_GeogFromWKB(NULL);
SELECT ST_AsBinary(ST_GeogFromWKB(X'0101000000000000000000F03F0000000000000040')); -- NDR.
SELECT ST_AsBinary(ST_GeomFromWKB(X'00000000013FF00000000000004000000000000000')); -- XDR.
SELECT ST_AsBinary(ST_GeomFromWKB(X'010203')); -- Invalid WKB.

-- 2. Table-level queries.
SELECT COUNT(*) FROM geodata WHERE wkb IS NOT NULL AND ST_AsBinary(ST_GeogFromWKB(wkb)) IS NULL;
-- Error handling: invalid SRID.
SELECT ST_AsBinary(ST_GeogFromWKB(wkb)) FROM geoinvaliddata;

------ ST accessor expressions

---- ST_Srid

-- 1. Driver-level queries.
SELECT ST_Srid(NULL);
SELECT ST_Srid(ST_GeogFromWKB(X'0101000000000000000000F03F0000000000000040'));
SELECT ST_Srid(ST_GeomFromWKB(X'0101000000000000000000F03F0000000000000040'));

-- 2. Table-level queries.
SELECT COUNT(*) FROM geodata WHERE ST_Srid(ST_GeogFromWKB(wkb)) <> 4326;
SELECT COUNT(*) FROM geodata WHERE ST_Srid(ST_GeomFromWKB(wkb)) <> 0;

-- Drop the test table.
DROP TABLE geodata;
DROP TABLE geoinvaliddata;
