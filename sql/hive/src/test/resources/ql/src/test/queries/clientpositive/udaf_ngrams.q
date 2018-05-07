CREATE TABLE kafka (contents STRING);
LOAD DATA LOCAL INPATH '../../data/files/text-en.txt' INTO TABLE kafka;
set mapreduce.job.reduces=1;
set hive.exec.reducers.max=1;

SELECT ngrams(sentences(lower(contents)), 1, 100, 1000).estfrequency FROM kafka;
SELECT ngrams(sentences(lower(contents)), 2, 100, 1000).estfrequency FROM kafka;
SELECT ngrams(sentences(lower(contents)), 3, 100, 1000).estfrequency FROM kafka;
SELECT ngrams(sentences(lower(contents)), 4, 100, 1000).estfrequency FROM kafka;
SELECT ngrams(sentences(lower(contents)), 5, 100, 1000).estfrequency FROM kafka;

DROP TABLE kafka;
