-- Tests that selecting from a view and another view that selects from that same view

CREATE VIEW test_view1 AS SELECT * FROM src;

CREATE VIEW test_view2 AS SELECT * FROM test_view1;

SELECT COUNT(*) FROM test_view1 a JOIN test_view2 b ON a.key = b.key;
