-- unicode_encode function
select unicode_encode('https://spark.apache.org');
select unicode_encode(null);

-- unicode_decode function
select unicode_decode('\u0061\u0070\u0061\u0063\u0068\u0065');
select unicode_decode(null);