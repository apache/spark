-- full partition spec for not existing partition
TRUNCATE TABLE srcpart partition (ds='2012-12-17', hr='15');
