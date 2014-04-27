CREATE TABLE dest1(key INT, value STRING) STORED AS
  INPUTFORMAT 'ClassDoesNotExist'
  OUTPUTFORMAT 'java.lang.Void';
