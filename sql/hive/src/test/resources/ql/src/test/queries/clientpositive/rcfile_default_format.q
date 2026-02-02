SET hive.default.fileformat = RCFile;

CREATE TABLE rcfile_default_format (key STRING);
DESCRIBE FORMATTED rcfile_default_format; 

CREATE TABLE rcfile_default_format_ctas AS SELECT key,value FROM src;
DESCRIBE FORMATTED rcfile_default_format_ctas; 

CREATE TABLE rcfile_default_format_txtfile (key STRING) STORED AS TEXTFILE;
INSERT OVERWRITE TABLE rcfile_default_format_txtfile SELECT key from src;
DESCRIBE FORMATTED rcfile_default_format_txtfile; 

SET hive.default.fileformat = TextFile;
CREATE TABLE textfile_default_format_ctas AS SELECT key,value FROM rcfile_default_format_ctas;
DESCRIBE FORMATTED textfile_default_format_ctas;

SET hive.default.fileformat = RCFile;
SET hive.default.rcfile.serde = org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
CREATE TABLE rcfile_default_format_ctas_default_serde AS SELECT key,value FROM rcfile_default_format_ctas;
DESCRIBE FORMATTED rcfile_default_format_ctas_default_serde;

CREATE TABLE rcfile_default_format_default_serde (key STRING);
DESCRIBE FORMATTED rcfile_default_format_default_serde;

SET hive.default.fileformat = TextFile;
CREATE TABLE rcfile_ctas_default_serde STORED AS rcfile AS SELECT key,value FROM rcfile_default_format_ctas;
DESCRIBE FORMATTED rcfile_ctas_default_serde;

CREATE TABLE rcfile_default_serde (key STRING) STORED AS rcfile;
DESCRIBE FORMATTED rcfile_default_serde;

