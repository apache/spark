CREATE VIEW testView AS SELECT value FROM src WHERE key=86;
ALTER VIEW testView SET TBLPROPERTIES ('propA'='100', 'propB'='200');
SHOW TBLPROPERTIES testView;

-- unset a subset of the properties and some non-existed properties without if exists
ALTER VIEW testView UNSET TBLPROPERTIES ('propB', 'propX', 'propY', 'propZ');
