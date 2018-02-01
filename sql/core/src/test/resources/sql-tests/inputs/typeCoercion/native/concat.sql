-- Concatenate mixed inputs (output type is string)
SELECT (col1 || col2 || col3) col
FROM (
  SELECT
    id col1,
    string(id + 1) col2,
    encode(string(id + 2), 'utf-8') col3
  FROM range(10)
);

SELECT ((col1 || col2) || (col3 || col4) || col5) col
FROM (
  SELECT
    'prefix_' col1,
    id col2,
    string(id + 1) col3,
    encode(string(id + 2), 'utf-8') col4,
    CAST(id AS DOUBLE) col5
  FROM range(10)
);

SELECT ((col1 || col2) || (col3 || col4)) col
FROM (
  SELECT
    string(id) col1,
    string(id + 1) col2,
    encode(string(id + 2), 'utf-8') col3,
    encode(string(id + 3), 'utf-8') col4
  FROM range(10)
);

-- turn on concatBinaryAsString
set spark.sql.function.concatBinaryAsString=true;

SELECT (col1 || col2) col
FROM (
  SELECT
    encode(string(id), 'utf-8') col1,
    encode(string(id + 1), 'utf-8') col2
  FROM range(10)
);

SELECT (col1 || col2 || col3 || col4) col
FROM (
  SELECT
    encode(string(id), 'utf-8') col1,
    encode(string(id + 1), 'utf-8') col2,
    encode(string(id + 2), 'utf-8') col3,
    encode(string(id + 3), 'utf-8') col4
  FROM range(10)
);

SELECT ((col1 || col2) || (col3 || col4)) col
FROM (
  SELECT
    encode(string(id), 'utf-8') col1,
    encode(string(id + 1), 'utf-8') col2,
    encode(string(id + 2), 'utf-8') col3,
    encode(string(id + 3), 'utf-8') col4
  FROM range(10)
);

-- turn off concatBinaryAsString
set spark.sql.function.concatBinaryAsString=false;

-- Concatenate binary inputs (output type is binary)
SELECT (col1 || col2) col
FROM (
  SELECT
    encode(string(id), 'utf-8') col1,
    encode(string(id + 1), 'utf-8') col2
  FROM range(10)
);

SELECT (col1 || col2 || col3 || col4) col
FROM (
  SELECT
    encode(string(id), 'utf-8') col1,
    encode(string(id + 1), 'utf-8') col2,
    encode(string(id + 2), 'utf-8') col3,
    encode(string(id + 3), 'utf-8') col4
  FROM range(10)
);

SELECT ((col1 || col2) || (col3 || col4)) col
FROM (
  SELECT
    encode(string(id), 'utf-8') col1,
    encode(string(id + 1), 'utf-8') col2,
    encode(string(id + 2), 'utf-8') col3,
    encode(string(id + 3), 'utf-8') col4
  FROM range(10)
);
