CREATE TABLE kafka (contents STRING);
LOAD DATA LOCAL INPATH '../data/files/text-en.txt' INTO TABLE kafka;
set mapred.reduce.tasks=1;
set hive.exec.reducers.max=1;

SELECT context_ngrams(sentences(lower(contents)), array(null), 100, 1000).estfrequency FROM kafka;
SELECT context_ngrams(sentences(lower(contents)), array("he",null), 100, 1000) FROM kafka;
SELECT context_ngrams(sentences(lower(contents)), array(null,"salesmen"), 100, 1000) FROM kafka;
SELECT context_ngrams(sentences(lower(contents)), array("what","i",null), 100, 1000) FROM kafka;
SELECT context_ngrams(sentences(lower(contents)), array(null,null), 100, 1000).estfrequency FROM kafka;

DROP TABLE kafka;
