DROP TABLE IF EXISTS atab; 
CREATE TABLE IF NOT EXISTS atab (ks_uid BIGINT, sr_uid STRING, sr_id STRING, tstamp STRING, m_id STRING, act STRING, at_sr_uid STRING, tstamp_type STRING, original_m_id STRING, original_tstamp STRING, registered_flag TINYINT, at_ks_uid BIGINT) PARTITIONED BY (dt STRING,nt STRING); 
LOAD DATA LOCAL INPATH '../data/files/v1.txt' INTO TABLE atab PARTITION (dt='20130312', nt='tw');
LOAD DATA LOCAL INPATH '../data/files/v1.txt' INTO TABLE atab PARTITION (dt='20130311', nt='tw');

DROP TABLE IF EXISTS  mstab;
CREATE TABLE  mstab(ks_uid INT, csc INT) PARTITIONED BY (dt STRING);
LOAD DATA LOCAL INPATH '../data/files/v2.txt' INTO TABLE mstab PARTITION (dt='20130311');

DROP VIEW IF EXISTS aa_view_tw;
CREATE VIEW aa_view_tw AS SELECT ks_uid, sr_id, act, at_ks_uid, at_sr_uid, from_unixtime(CAST(CAST( tstamp as BIGINT)/1000 AS BIGINT),'yyyyMMdd') AS act_date, from_unixtime(CAST(CAST( original_tstamp AS BIGINT)/1000 AS BIGINT),'yyyyMMdd') AS content_creation_date FROM atab WHERE dt='20130312' AND nt='tw' AND ks_uid != at_ks_uid;

DROP VIEW IF EXISTS joined_aa_view_tw;
CREATE VIEW joined_aa_view_tw AS SELECT aa.ks_uid, aa.sr_id, aa.act, at_sr_uid, aa.act_date, aa.at_ks_uid, aa.content_creation_date, coalesce( other.ksc, 10.0) AS at_ksc, coalesce( self.ksc , 10.0 ) AS self_ksc FROM aa_view_tw aa LEFT OUTER JOIN ( SELECT ks_uid, csc AS ksc FROM mstab WHERE dt='20130311' ) self ON ( CAST(aa.ks_uid AS BIGINT) = CAST(self.ks_uid AS BIGINT) ) LEFT OUTER JOIN ( SELECT ks_uid, csc AS ksc FROM mstab WHERE dt='20130311' ) other ON ( CAST(aa.at_ks_uid AS BIGINT) = CAST(other.ks_uid AS BIGINT) );

SELECT * FROM joined_aa_view_tw;
