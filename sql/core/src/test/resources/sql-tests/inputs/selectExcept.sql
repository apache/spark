CREATE TEMPORARY VIEW tbl_view AS SELECT * FROM VALUES
  (10, "name1", named_struct("f1", 1, "s2", named_struct("f2", 101, "f3", "a"))),
  (20, "name2", named_struct("f1", 2, "s2", named_struct("f2", 202, "f3", "b"))),
  (30, "name3", named_struct("f1", 3, "s2", named_struct("f2", 303, "f3", "c"))),
  (40, "name4", named_struct("f1", 4, "s2", named_struct("f2", 404, "f3", "d"))),
  (50, "name5", named_struct("f1", 5, "s2", named_struct("f2", 505, "f3", "e"))),
  (60, "name6", named_struct("f1", 6, "s2", named_struct("f2", 606, "f3", "f"))),
  (70, "name7", named_struct("f1", 7, "s2", named_struct("f2", 707, "f3", "g")))
AS tbl_view(id, name, data);

CREATE TABLE ids (id INT) USING CSV;

-- Happy path
-- EXCEPT basic scenario
SELECT * FROM tbl_view;
SELECT * EXCEPT (id) FROM tbl_view;
SELECT * EXCEPT (name) FROM tbl_view;
-- EXCEPT named structs
SELECT * EXCEPT (data) FROM tbl_view;
SELECT * EXCEPT (data.f1) FROM tbl_view;
SELECT * EXCEPT (data.s2) FROM tbl_view;
SELECT * EXCEPT (data.s2.f2) FROM tbl_view;
-- EXCEPT all columns
SELECT * EXCEPT (id, name, data) FROM tbl_view;
-- EXCEPT special character names
SELECT * EXCEPT (`a-b-c`) FROM (SELECT 1 a_b_c, 2 `a-b-c`);
-- EXCEPT qualified star
SELECT tbl_view.* EXCEPT (name) FROM tbl_view;
INSERT INTO ids
SELECT * EXCEPT (name, data) FROM tbl_view;
SELECT * FROM ids;
-- EXCEPT qualified columns
SELECT * EXCEPT (ids.id) FROM ids;
-- EXCEPT structs
SELECT data.* EXCEPT (s2) FROM tbl_view;
SELECT data.* EXCEPT (s2.f2) FROM tbl_view;
SELECT data.s2.* EXCEPT (f2) FROM tbl_view;

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
-- EXCEPT overlapping columns
SELECT * EXCEPT(data.s2, data.s2.f2) FROM tbl_view;
