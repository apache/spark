SELECT id % 2 AS id2, id + id2 + 1 FROM range(4) ORDER BY id2;
SELECT max(id) AS id FROM range(4) ORDER BY avg(id);
SELECT max(id) AS id FROM range(4) GROUP BY id ORDER BY avg(id);
SELECT max(id) AS id2 FROM range(4) ORDER BY avg(id2);
SELECT max(id) AS id2 FROM range(4) ORDER BY avg(id2 / 2);
SELECT max(id) AS id2 FROM range(4) ORDER BY avg(id2) / 1;
SELECT max(id) AS id2 FROM range(4) ORDER BY cast(id2 as string);
SELECT max(id) AS id FROM range(4) ORDER BY id;
SELECT max(id) AS id2 FROM range(4) ORDER BY id2;
SELECT max(id) AS id2 FROM range(4) ORDER BY id2 / 2;

-- referencing a LCA with conflicts
SELECT id % 2 AS id FROM range(4) ORDER BY id;
SELECT id % 2 AS id, id / 2 AS id FROM range(4) ORDER BY id;
SELECT id % 2 AS id2, id / 2 AS id2 FROM range(4) ORDER BY id2;
