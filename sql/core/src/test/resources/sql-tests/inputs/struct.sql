CREATE TEMPORARY VIEW tbl_x AS VALUES
  (1, NAMED_STRUCT('C', 'gamma', 'D', 'delta')),
  (2, NAMED_STRUCT('C', 'epsilon', 'D', 'eta')),
  (3, NAMED_STRUCT('C', 'theta', 'D', 'iota'))
  AS T(ID, ST);

-- Create a struct
SELECT STRUCT('alpha', 'beta') ST;

-- Create a struct with aliases
SELECT STRUCT('alpha' AS A, 'beta' AS B) ST;

-- Star expansion in a struct.
SELECT ID, STRUCT(ST.*) NST FROM tbl_x;

-- Append a column to a struct
SELECT ID, STRUCT(ST.*,CAST(ID AS STRING) AS E) NST FROM tbl_x;

-- Prepend a column to a struct
SELECT ID, STRUCT(CAST(ID AS STRING) AS AA, ST.*) NST FROM tbl_x;

-- Select a column from a struct
SELECT ID, STRUCT(ST.*).C NST FROM tbl_x;
SELECT ID, STRUCT(ST.C, ST.D).D NST FROM tbl_x;

-- Select an alias from a struct
SELECT ID, STRUCT(ST.C as STC, ST.D as STD).STD FROM tbl_x;