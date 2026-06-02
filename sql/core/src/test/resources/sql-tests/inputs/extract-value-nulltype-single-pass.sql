-- SPARK-57186: multipart field access (col.a) on a NullType base propagates NULL under the
-- single-pass resolver as well, consistently with the legacy analyzer. Dual-running both analyzers
-- locks in that consistency (no HYBRID_ANALYZER_EXCEPTION).
-- The col[0]/col['key'] subscript forms are intentionally not covered here: the single-pass
-- resolver does not resolve subscript extraction (UnresolvedExtractValue) at all -- a pre-existing
-- limitation independent of NullType -- so they are exercised only under the legacy analyzer in
-- extract-value-resolution-edge-cases.sql.
--SET spark.sql.analyzer.singlePassResolver.dualRunWithLegacy=true

SELECT col.a FROM (SELECT null AS col) t;
