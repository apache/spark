DROP VIEW testView;

-- Cannot ALTER VIEW AS SELECT if view currently does not exist
ALTER VIEW testView AS SELECT * FROM srcpart;
