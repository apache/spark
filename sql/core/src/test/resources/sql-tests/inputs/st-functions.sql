--- Casting geospatial data types

-- Cast a GEOGRAPHY type with fixed SRID to a GEOGRAPHY type with mixed SRID.
SELECT hex(ST_AsBinary(ST_GeogFromWKB(X'0101000000000000000000F03F0000000000000040')::GEOGRAPHY(ANY))) AS result;
SELECT hex(ST_AsBinary(CAST(ST_GeogFromWKB(X'0101000000000000000000F03F0000000000000040') AS GEOGRAPHY(ANY)))) AS result;

---- ST reader/writer expressions

-- WKB (Well-Known Binary) round-trip tests for GEOGRAPHY and GEOMETRY types.
SELECT hex(ST_AsBinary(ST_GeogFromWKB(X'0101000000000000000000f03f0000000000000040'))) AS result;
SELECT hex(ST_AsBinary(ST_GeomFromWKB(X'0101000000000000000000f03f0000000000000040'))) AS result;
