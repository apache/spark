--SET spark.sql.charVarchar.standardSemantics.enabled=true

-- R3: CAST introduces CHAR/VARCHAR
SELECT typeof(CAST('ab' AS CHAR(5)));
SELECT typeof(CAST('hello' AS VARCHAR(5)));
SELECT 'X' || CAST('5' AS CHAR(5)) || 'X';

-- CAST length: character-to-character truncates (ISO 6.13); numeric overflow errors
SELECT CAST('ab   ' AS CHAR(2));
SELECT CAST('abcdef' AS CHAR(2));
SELECT CAST('abcdef' AS VARCHAR(2));
SELECT try_cast('abcdef' AS CHAR(2));
SELECT try_cast('abcdef' AS VARCHAR(2));
SELECT CAST(12345 AS VARCHAR(4));
SELECT CAST(12345 AS VARCHAR(5));
SELECT try_cast(12345 AS VARCHAR(4));

-- Explicit CAST inside LCT must keep inner truncation / overflow (do not retarget).
SELECT coalesce(CAST('abcdef' AS VARCHAR(2)), CAST('x' AS VARCHAR(4)));
SELECT CASE WHEN true THEN CAST('abcdef' AS VARCHAR(2)) ELSE CAST('x' AS VARCHAR(4)) END;
SELECT CAST('abcdef' AS VARCHAR(2)) IN (CAST('ab' AS VARCHAR(4)));
SELECT coalesce(
  CAST('abcdef' AS VARCHAR(2) COLLATE UTF8_LCASE),
  CAST('x' AS VARCHAR(4) COLLATE UTF8_LCASE));
SELECT coalesce(try_cast(12345 AS VARCHAR(4)), CAST('x' AS VARCHAR(5)));
SELECT coalesce(CAST(12345 AS VARCHAR(4)), CAST('x' AS VARCHAR(5)));

-- R2: least common type (COALESCE / CASE)
SELECT typeof(coalesce(cast('hello' AS VARCHAR(5)), cast('world' AS VARCHAR(10))));
SELECT typeof(coalesce(cast('hello' AS VARCHAR(5)), cast('world!' AS CHAR(6))));
SELECT typeof(coalesce(cast('hello' AS CHAR(5)), cast('world!' AS CHAR(6))));
SELECT typeof(coalesce(cast('hello' AS VARCHAR(5)), 'world'));
SELECT typeof(coalesce(cast('hello' AS CHAR(5)), NULL));
SELECT typeof(
  CASE WHEN true THEN cast('a' AS CHAR(2)) ELSE cast('bb' AS CHAR(4)) END);

-- R2: least common type for IN lists
SELECT cast('a' AS CHAR(2)) IN (cast('a ' AS CHAR(2)), cast('bbb' AS VARCHAR(3)));
SELECT typeof(c) FROM (SELECT cast('a' AS CHAR(2)) AS c) t WHERE c IN ('a ', 'b');

-- R1: transforming functions return STRING
SELECT typeof(upper(cast('ab' AS CHAR(2))));
SELECT typeof(lower(cast('AB' AS VARCHAR(2))));
SELECT typeof(cast('a' AS CHAR(1)) || cast('b' AS VARCHAR(1)));
SELECT typeof(substr(cast('hello' AS VARCHAR(5)), 1, 2));
SELECT typeof(upper(coalesce(cast('a' AS CHAR(2)), cast('b' AS CHAR(4)))));
SELECT typeof(concat(cast('a' AS CHAR(2)), cast('b' AS CHAR(3))));
SELECT typeof(trim(cast('ab  ' AS CHAR(4))));
SELECT typeof(lpad(cast('ab' AS CHAR(2)), 5, 'x'));

-- R1: regexp / mask / split family
SELECT typeof(regexp_replace(cast('ab' AS CHAR(2)), 'a', 'x'));
SELECT typeof(regexp_extract(cast('ab' AS VARCHAR(2)), '(a)', 1));
SELECT typeof(regexp_extract_all(cast('aab' AS VARCHAR(3)), '(a)', 1));
SELECT typeof(split(cast('a,b' AS CHAR(3)), ','));
SELECT typeof(mask(cast('ab' AS CHAR(2))));

-- R1: CHAR/VARCHAR promote to STRING where a plain string is expected, so expressions that
-- require all their string inputs to share one type accept them alongside a STRING argument.
-- Values are wrapped in sentinels because the golden format trims trailing blanks, which would
-- otherwise hide the CHAR padding these expressions operate on.
SELECT typeof(overlay(cast('ab' AS CHAR(5)) PLACING 'x' FROM 1));
SELECT concat('<', overlay(cast('ab' AS CHAR(5)) PLACING 'x' FROM 1), '>');
SELECT typeof(elt(1, cast('ab' AS CHAR(5)), 'x'));
SELECT typeof(right(cast('ab' AS CHAR(5)), 2));
SELECT concat('<', right(cast('ab' AS CHAR(5)), 2), '>');
SELECT typeof(left(cast('ab' AS CHAR(5)), 2));

-- R1: transforms whose result length differs from the input must not inherit the constraint.
SELECT typeof(reverse(cast('ab' AS CHAR(5))));
SELECT typeof(hex(cast('ab' AS CHAR(5))));
SELECT hex(cast('ab' AS CHAR(5)));
SELECT typeof(array_join(array(cast('ab' AS CHAR(5)), cast('cd' AS CHAR(5))), '-'));
SELECT concat('<', array_join(array(cast('ab' AS CHAR(5)), cast('cd' AS CHAR(5))), '-'), '>');
-- reverse() on a non-string input is unaffected.
SELECT typeof(reverse(array(1, 2)));
-- Expressions that pull values out of a wrapper do not inherit the wrapper's constraint. These
-- take their inputs through ExpectsInputTypes, so no implicit cast strips the length for them.
SELECT typeof(str_to_map(cast('a:1,b:2' AS CHAR(7))));
SELECT typeof(c0) FROM (SELECT json_tuple(cast('{"a":"1"}' AS CHAR(9)), 'a') AS c0);

-- Collation survives CAST and LCT. Mixed lengths with the same collation widen to max(n, m);
-- they must not collapse to an indeterminate collation.
SELECT typeof(cast('a' AS CHAR(2) COLLATE UTF8_LCASE));
SELECT typeof(coalesce(
  cast('a' AS CHAR(2) COLLATE UTF8_LCASE), cast('bb' AS CHAR(2) COLLATE UTF8_LCASE)));
SELECT typeof(coalesce(
  cast('a' AS CHAR(2) COLLATE UTF8_LCASE), cast('bb' AS CHAR(4) COLLATE UTF8_LCASE)));
SELECT hex(coalesce(
  cast('a' AS CHAR(2) COLLATE UTF8_LCASE), cast('bb' AS CHAR(4) COLLATE UTF8_LCASE)));
SELECT typeof(coalesce(
  cast('a' AS CHAR(2) COLLATE UTF8_LCASE), cast('bb' AS VARCHAR(4) COLLATE UTF8_LCASE)));
-- Mixed strength, same collation: Implicit string CAST CHAR(2) vs Default
-- non-string CAST CHAR(4). Length still widens to max(n, m); the COLLATE
-- operator itself is STRING, so it is not used here.
SELECT typeof(coalesce(
  cast('a' AS CHAR(2) COLLATE UTF8_LCASE),
  cast(1 AS CHAR(4) COLLATE UTF8_LCASE)));
SELECT hex(coalesce(
  cast('a' AS CHAR(2) COLLATE UTF8_LCASE),
  cast(1 AS CHAR(4) COLLATE UTF8_LCASE)));

-- Set operations and multi-row VALUES share the same LCT as COALESCE.
SELECT typeof(c) FROM (
  SELECT cast('a' AS VARCHAR(3)) AS c
  UNION ALL
  SELECT cast('abcd' AS VARCHAR(8)) AS c
) t LIMIT 1;
SELECT typeof(c) FROM (
  SELECT cast('a' AS CHAR(2)) AS c
  UNION ALL
  SELECT cast('bb' AS CHAR(4)) AS c
) t LIMIT 1;
SELECT concat('<', c, '>') FROM (
  SELECT cast('a' AS CHAR(2)) AS c
  UNION ALL
  SELECT cast('bb' AS CHAR(4)) AS c
) t;
SELECT typeof(c) FROM (
  SELECT cast('a' AS CHAR(2)) AS c
  UNION
  SELECT cast('a' AS CHAR(4)) AS c
) t;
SELECT concat('<', c, '>') FROM (
  SELECT cast('a' AS CHAR(2)) AS c
  UNION
  SELECT cast('a' AS CHAR(4)) AS c
) t;
SELECT typeof(c) FROM (
  SELECT cast('ab' AS CHAR(2)) AS c
  INTERSECT
  SELECT cast('ab' AS CHAR(4)) AS c
) t;
SELECT concat('<', c, '>') FROM (
  SELECT cast('ab' AS CHAR(2)) AS c
  INTERSECT
  SELECT cast('ab' AS CHAR(4)) AS c
) t;
-- Non-empty EXCEPT: after widening, 'ab  ' is not 'xy  '.
SELECT typeof(c) FROM (
  SELECT cast('ab' AS CHAR(2)) AS c
  EXCEPT
  SELECT cast('xy' AS CHAR(4)) AS c
) t;
SELECT concat('<', c, '>') FROM (
  SELECT cast('ab' AS CHAR(2)) AS c
  EXCEPT
  SELECT cast('xy' AS CHAR(4)) AS c
) t;
SELECT typeof(c) FROM (VALUES
  (cast('a' AS CHAR(2))),
  (cast('bb' AS CHAR(4)))
) t(c);
SELECT concat('<', c, '>') FROM (VALUES
  (cast('a' AS CHAR(2))),
  (cast('bb' AS CHAR(4)))
) t(c);

-- Comparison and IN: both sides (including the IN left-hand side) are cast to the LCT of all
-- participants. Casting to CHAR pads, so CHAR vs CHAR of different lengths compares equal after
-- widening; casting to VARCHAR/STRING keeps the CHAR pad, so CHAR 'a' (stored as 'a ') is not equal
-- to VARCHAR/STRING 'a' unless the other side carries the same trailing blank. Trailing-blank
-- ignoring is a collation concern (RTRIM), not a type-level PAD SPACE policy.
SELECT cast('a' AS CHAR(2)) = cast('a' AS CHAR(4));
SELECT cast('a' AS CHAR(2)) = cast('a' AS VARCHAR(2));
SELECT cast('a' AS CHAR(2)) = cast('a ' AS VARCHAR(2));
SELECT cast('a' AS CHAR(2)) = 'a';
SELECT cast('a' AS CHAR(2)) = 'a ';
SELECT cast('a' AS CHAR(2) COLLATE UTF8_BINARY_RTRIM) = 'a';
SELECT cast('a' AS CHAR(2) COLLATE UTF8_BINARY_RTRIM) =
  cast('a' AS CHAR(4) COLLATE UTF8_BINARY_RTRIM);
SELECT cast('a' AS CHAR(2)) IN (cast('a' AS CHAR(4)));
SELECT cast('a' AS CHAR(2)) IN (cast('a' AS VARCHAR(2)));
SELECT cast('a' AS CHAR(2)) IN (cast('a ' AS VARCHAR(2)));
SELECT cast('a' AS CHAR(2)) IN ('a', 'b');
SELECT cast('a' AS CHAR(2)) IN ('a ', 'b');
-- Three-part IN: LHS CHAR(2) and list CHAR(4)/VARCHAR(3) all widen to VARCHAR(4).
SELECT cast('a' AS CHAR(2)) IN (cast('a' AS CHAR(4)), cast('b' AS VARCHAR(3)));
-- Same CHAR-pad rule under a non-RTRIM collation: UTF8_LCASE must not make
-- CHAR 'a' equal VARCHAR 'a'. The analyzer must nest CHAR then VARCHAR, not
-- retarget CAST('a' AS CHAR(2) COLLATE UTF8_LCASE) to VARCHAR(2).
SELECT cast('a' AS CHAR(2) COLLATE UTF8_LCASE) = cast('a' AS VARCHAR(2) COLLATE UTF8_LCASE);
SELECT cast('a' AS CHAR(2) COLLATE UTF8_LCASE) IN (cast('a' AS VARCHAR(2) COLLATE UTF8_LCASE));

-- Nested types keep CHAR/VARCHAR
SELECT typeof(array(cast('a' AS CHAR(2)), cast('bb' AS CHAR(3))));
SELECT typeof(struct(cast('a' AS CHAR(2)) AS f));
SELECT typeof(map('k', cast('a' AS VARCHAR(2))));

-- R3: bare column references keep the declared type; write and read pad CHAR
CREATE TABLE char_varchar_std (c CHAR(5), v VARCHAR(5)) USING parquet;
INSERT INTO char_varchar_std VALUES ('ab', 'ab');
SELECT typeof(c), typeof(v) FROM char_varchar_std;
SELECT concat('[', c, ']'), concat('[', v, ']') FROM char_varchar_std;
SELECT length(c), length(v) FROM char_varchar_std;
DROP TABLE char_varchar_std;
