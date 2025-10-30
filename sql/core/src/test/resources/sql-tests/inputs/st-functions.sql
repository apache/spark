---- ST reader/writer expressions

-- WKB (Well-Known Binary) round-trip tests for GEOGRAPHY and GEOMETRY types.
SELECT hex(ST_AsBinary(ST_GeogFromWKB(X'0101000000000000000000f03f0000000000000040'))) AS result;
SELECT hex(ST_AsBinary(ST_GeomFromWKB(X'0101000000000000000000f03f0000000000000040'))) AS result;
