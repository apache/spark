CREATE TABLE tmp_pyang_bucket3 (userId INT) CLUSTERED BY (userid) INTO 32 BUCKETS;
DROP TABLE tmp_pyang_bucket3;
CREATE TABLE tmp_pyang_bucket3 (userId INT) CLUSTERED BY (userid) SORTED BY (USERID) INTO 32 BUCKETS;
