DROP TABLE show_idx_empty;
DROP TABLE show_idx_full;

CREATE TABLE show_idx_empty(KEY STRING, VALUE STRING);
CREATE TABLE show_idx_full(KEY STRING, VALUE1 STRING, VALUE2 STRING);

CREATE INDEX idx_1 ON TABLE show_idx_full(KEY) AS "COMPACT" WITH DEFERRED REBUILD;
CREATE INDEX idx_2 ON TABLE show_idx_full(VALUE1) AS "COMPACT" WITH DEFERRED REBUILD;

CREATE INDEX idx_comment ON TABLE show_idx_full(VALUE2) AS "COMPACT" WITH DEFERRED REBUILD COMMENT "index comment";
CREATE INDEX idx_compound ON TABLE show_idx_full(KEY, VALUE1) AS "COMPACT" WITH DEFERRED REBUILD;

ALTER INDEX idx_1 ON show_idx_full REBUILD;
ALTER INDEX idx_2 ON show_idx_full REBUILD;
ALTER INDEX idx_comment ON show_idx_full REBUILD;
ALTER INDEX idx_compound ON show_idx_full REBUILD;

EXPLAIN SHOW INDEXES ON show_idx_full;
SHOW INDEXES ON show_idx_full;

EXPLAIN SHOW INDEXES ON show_idx_empty;
SHOW INDEXES ON show_idx_empty;

DROP INDEX idx_1 on show_idx_full;
DROP INDEX idx_2 on show_idx_full;
DROP TABLE show_idx_empty;
DROP TABLE show_idx_full;