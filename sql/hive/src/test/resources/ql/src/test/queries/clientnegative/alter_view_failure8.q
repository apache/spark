-- should fail:  can't use ALTER VIEW on a table
CREATE TABLE invites (foo INT, bar STRING) PARTITIONED BY (ds STRING);
ALTER VIEW invites RENAME TO invites2;
