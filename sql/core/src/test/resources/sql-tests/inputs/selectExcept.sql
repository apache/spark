CREATE TEMPORARY VIEW tbl_view AS SELECT * FROM VALUES
  (10, "name1"),
  (20, "name2"),
  (30, "name3"),
  (40, "name4"),
  (50, "name5"),
  (70, "name7")
AS tbl_view(id, name);

CREATE TABLE ids (id INT) USING CSV;

-- Happy path
-- EXCEPT basic scenario
SELECT * FROM tbl_view;
SELECT * EXCEPT (id) FROM tbl_view;
SELECT * EXCEPT (name) FROM tbl_view;
-- EXCEPT all columns
SELECT * EXCEPT (id, name) FROM tbl_view;
-- CREATE TABLE with SELECT
CREATE TABLE namesTbl USING CSV AS SELECT * EXCEPT (id) FROM tbl_view;
SELECT * FROM namesTbl;
-- EXCEPT special character names
SELECT * EXCEPT (`a-b-c`) FROM (SELECT 1 a_b_c, 2 `a-b-c`);
-- EXCEPT qualified star
SELECT tbl_view.* EXCEPT (name) FROM tbl_view;
-- EXCEPT insert into select
INSERT INTO ids
SELECT * EXCEPT (name) FROM tbl_view;
SELECT * FROM ids;
-- EXCEPT qualified columns
SELECT * EXCEPT (ids.id) FROM ids;
-- EXCEPT nested
SELECT * FROM (SELECT * EXCEPT (name) FROM tbl_view);

-- Errors
-- EXCEPT missing brackets
SELECT * EXCEPT name FROM tbl_view;
-- EXCEPT no columns
SELECT * EXCEPT() name FROM tbl_view;
-- EXCEPT invalid column
SELECT * EXCEPT(invalid_column) FROM tbl_view;
-- EXCEPT find invalid column
SELECT * EXCEPT(id, invalid_column) FROM tbl_view;
-- EXCEPT duplicate column
SELECT * EXCEPT(id, id) FROM tbl_view;
