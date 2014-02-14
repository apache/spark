
DROP TABLE IF EXISTS UserVisits_web_text_none;

CREATE TABLE UserVisits_web_text_none (
  sourceIP string,
  destURL string,
  visitDate string,
  adRevenue float,
  userAgent string,
  cCode string,
  lCode string,
  sKeyword string,
  avgTimeOnSite int)
row format delimited fields terminated by '|'  stored as textfile;

LOAD DATA LOCAL INPATH "../data/files/UserVisits.dat" INTO TABLE UserVisits_web_text_none;

explain 
analyze table UserVisits_web_text_none compute statistics for columns destIP;

analyze table UserVisits_web_text_none compute statistics for columns destIP;

