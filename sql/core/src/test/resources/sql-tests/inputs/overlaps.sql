-- Test the OVERLAPS predicate for datetime periods

-- TIME overlapping periods
SELECT (TIME'09:00:00', TIME'12:00:00') OVERLAPS (TIME'11:00:00', TIME'13:00:00');

-- TIME non-overlapping periods
SELECT (TIME'09:00:00', TIME'11:00:00') OVERLAPS (TIME'13:00:00', TIME'15:00:00');

-- TIME touching endpoints (not overlapping per ANSI)
SELECT (TIME'09:00:00', TIME'12:00:00') OVERLAPS (TIME'12:00:00', TIME'13:00:00');

-- TIME endpoint normalization (swapped start/end)
SELECT (TIME'12:00:00', TIME'09:00:00') OVERLAPS (TIME'11:00:00', TIME'13:00:00');

-- TIME zero-length period (point) contained in period
SELECT (TIME'10:00:00', TIME'10:00:00') OVERLAPS (TIME'09:00:00', TIME'11:00:00');

-- TIME zero-length period (point) at boundary (not overlapping)
SELECT (TIME'12:00:00', TIME'12:00:00') OVERLAPS (TIME'09:00:00', TIME'12:00:00');

-- TIME two identical points overlap
SELECT (TIME'10:00:00', TIME'10:00:00') OVERLAPS (TIME'10:00:00', TIME'10:00:00');

-- TIME two different points do not overlap
SELECT (TIME'10:00:00', TIME'10:00:00') OVERLAPS (TIME'11:00:00', TIME'11:00:00');

-- TIME with microsecond precision
SELECT (TIME'09:00:00.000001', TIME'12:00:00.999999') OVERLAPS (TIME'12:00:00.999998', TIME'13:00:00');

-- TIME with interval form (explicit addition)
SELECT (TIME'09:00:00', TIME'09:00:00' + INTERVAL '3' HOUR) OVERLAPS (TIME'11:00:00', TIME'13:00:00');

-- TIME with interval form (non-overlapping, explicit addition)
SELECT (TIME'09:00:00', TIME'09:00:00' + INTERVAL '1' HOUR) OVERLAPS (TIME'11:00:00', TIME'13:00:00');

-- TIME with raw interval endpoint (resolved by ResolveOverlaps rule)
SELECT (TIME'09:00:00', INTERVAL '3' HOUR) OVERLAPS (TIME'11:00:00', TIME'13:00:00');

-- NULL endpoints (typed NULL from CAST)
SELECT (TIME'09:00:00', CAST(NULL AS TIME)) OVERLAPS (TIME'11:00:00', TIME'13:00:00');
SELECT (CAST(NULL AS TIME), TIME'12:00:00') OVERLAPS (TIME'11:00:00', TIME'13:00:00');

-- DATE overlapping periods
SELECT (DATE'2024-01-01', DATE'2024-06-30') OVERLAPS (DATE'2024-03-01', DATE'2024-12-31');

-- DATE non-overlapping periods
SELECT (DATE'2024-01-01', DATE'2024-03-01') OVERLAPS (DATE'2024-06-01', DATE'2024-12-31');

-- TIMESTAMP overlapping periods
SELECT (TIMESTAMP'2024-01-01 09:00:00', TIMESTAMP'2024-01-01 12:00:00') OVERLAPS (TIMESTAMP'2024-01-01 11:00:00', TIMESTAMP'2024-01-01 13:00:00');

-- TIMESTAMP_NTZ overlapping periods
SELECT (TIMESTAMP_NTZ'2024-01-01 09:00:00', TIMESTAMP_NTZ'2024-01-01 12:00:00') OVERLAPS (TIMESTAMP_NTZ'2024-01-01 11:00:00', TIMESTAMP_NTZ'2024-01-01 13:00:00');

-- Error cases: mixed types rejected
SELECT (DATE'2024-01-01', DATE'2024-06-30') OVERLAPS (TIMESTAMP'2024-03-01 00:00:00', TIMESTAMP'2024-12-31 00:00:00');

-- Error cases: non-datetime types rejected
SELECT (1, 5) OVERLAPS (3, 7);

-- TIME interval overflow (interval form crossing midnight throws DATETIME_OVERFLOW)
SELECT (TIME'23:00:00', INTERVAL '3' HOUR) OVERLAPS (TIME'01:00:00', TIME'04:00:00');

-- WHERE clause filter (exercises codegen fallback path)
SELECT * FROM VALUES (TIME'09:00:00', TIME'12:00:00'), (TIME'14:00:00', TIME'16:00:00') AS t(start_t, end_t) WHERE (start_t, end_t) OVERLAPS (TIME'11:00:00', TIME'15:00:00');
