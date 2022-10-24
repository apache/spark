-- trunc decimal
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), 11);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), 10);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), 9);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), 8);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), 7);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), 6);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), 5);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), 4);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), 3);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), 2);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), 1);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), 0);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), -1);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), -2);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), -3);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), -4);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), -5);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), -6);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), -7);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), -8);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), -9);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), -10);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), -11);
select trunc(CAST('0.123456789' AS DECIMAL(18,9)), 1);
select trunc(CAST('0.123456789' AS DECIMAL(18,9)), 0);
select trunc(CAST('0.123456789' AS DECIMAL(18,9)), 1);
select trunc(CAST('0.0' AS DECIMAL(18,9)), 1);
select trunc(CAST('10.0' AS DECIMAL(10,3)), 1);
select trunc(CAST('0.0' AS DECIMAL(10,4)), 0);
select trunc(CAST('0.0' AS DECIMAL(8,2)), 1);

-- trunc double
select trunc(CAST('123456789.123456789' AS DOUBLE), 11);
select trunc(CAST('123456789.123456789' AS DOUBLE), 10);
select trunc(CAST('123456789.123456789' AS DOUBLE), 9);
select trunc(CAST('123456789.123456789' AS DOUBLE), 8);
select trunc(CAST('123456789.123456789' AS DOUBLE), 7);
select trunc(CAST('123456789.123456789' AS DOUBLE), 6);
select trunc(CAST('123456789.123456789' AS DOUBLE), 5);
select trunc(CAST('123456789.123456789' AS DOUBLE), 4);
select trunc(CAST('123456789.123456789' AS DOUBLE), 3);
select trunc(CAST('123456789.123456789' AS DOUBLE), 2);
select trunc(CAST('123456789.123456789' AS DOUBLE), 1);
select trunc(CAST('123456789.123456789' AS DOUBLE), 0);
select trunc(CAST('123456789.123456789' AS DOUBLE), -1);
select trunc(CAST('123456789.123456789' AS DOUBLE), -2);
select trunc(CAST('123456789.123456789' AS DOUBLE), -3);
select trunc(CAST('123456789.123456789' AS DOUBLE), -4);
select trunc(CAST('123456789.123456789' AS DOUBLE), -5);
select trunc(CAST('123456789.123456789' AS DOUBLE), -6);
select trunc(CAST('123456789.123456789' AS DOUBLE), -7);
select trunc(CAST('123456789.123456789' AS DOUBLE), -8);
select trunc(CAST('123456789.123456789' AS DOUBLE), -9);
select trunc(CAST('123456789.123456789' AS DOUBLE), -10);
select trunc(CAST('123456789.123456789' AS DOUBLE), -11);
select trunc(CAST('123456789.0' AS DOUBLE), 2);

-- trunc float
select trunc(CAST('1234567.1' AS FLOAT), 1);
select trunc(CAST('1234567.12345' AS FLOAT), 1);
select trunc(CAST('1234567.1' AS FLOAT), 0);
select trunc(CAST('1234567.1' AS FLOAT));
select trunc(CAST('1234567.1' AS FLOAT), -1);
select trunc(CAST('1234567.1' AS FLOAT), -2);
select trunc(CAST('1234567.1' AS FLOAT), -3);
select trunc(CAST('1234567.1' AS FLOAT), -4);
select trunc(CAST('1234567.1' AS FLOAT), -5);
select trunc(CAST('1234567.1' AS FLOAT), -6);
select trunc(CAST('1234567.1' AS FLOAT), -7);
select trunc(CAST('1234567.1' AS FLOAT), -8);

-- trunc longCAST('
select trunc(CAST('123456789' AS BIGINT), 2);
select trunc(CAST('123456789' AS BIGINT), 2);
select trunc(CAST('123456789' AS BIGINT), 1);
select trunc(CAST('123456789' AS BIGINT), 0);
select trunc(CAST('123456789' AS BIGINT), -1);
select trunc(CAST('123456789' AS BIGINT), -2);
select trunc(CAST('123456789' AS BIGINT), -3);
select trunc(CAST('123456789' AS BIGINT), -4);
select trunc(CAST('123456789' AS BIGINT), -5);
select trunc(CAST('123456789' AS BIGINT), -6);
select trunc(CAST('123456789' AS BIGINT), -7);
select trunc(CAST('123456789' AS BIGINT), -8);
select trunc(CAST('123456789' AS BIGINT), -9);
select trunc(CAST('123456789' AS BIGINT), -10);
select trunc(CAST('123456789' AS BIGINT), -11);
select trunc(CAST('0' AS BIGINT), 1);
select trunc(CAST('0' AS BIGINT), 0);
select trunc(CAST('0' AS BIGINT), -1);

-- trunc Int
select trunc(CAST('123456789' AS INT), 3);
select trunc(CAST('123456789' AS INT), 2);
select trunc(CAST('123456789' AS INT), 1);
select trunc(CAST('123456789' AS INT), 0);
select trunc(CAST('123456789' AS INT), -1);
select trunc(CAST('123456789' AS INT), -2);
select trunc(CAST('123456789' AS INT), -3);
select trunc(CAST('123456789' AS INT), -4);
select trunc(CAST('123456789' AS INT), -5);
select trunc(CAST('123456789' AS INT), -6);
select trunc(CAST('123456789' AS INT), -7);
select trunc(CAST('123456789' AS INT), -8);
select trunc(CAST('123456789' AS INT), -9);
select trunc(CAST('123456789' AS INT), -10);
select trunc(CAST('123456789' AS INT), -11);
select trunc(CAST('0' AS BIGINT), 1);
select trunc(CAST('0' AS BIGINT), 0);
select trunc(CAST('0' AS BIGINT), -1);

-- trunc SMALLINT
select trunc(CAST('32767' AS SMALLINT), 1);
select trunc(CAST('32767' AS SMALLINT), 2);
select trunc(CAST('32767' AS SMALLINT), 1);
select trunc(CAST('32767' AS SMALLINT), 0);
select trunc(CAST('32767' AS SMALLINT), -1);
select trunc(CAST('32767' AS SMALLINT), -2);
select trunc(CAST('32767' AS SMALLINT), -3);
select trunc(CAST('32767' AS SMALLINT), -4);
select trunc(CAST('32767' AS SMALLINT), -5);
select trunc(CAST('0' AS SMALLINT), 1);
select trunc(CAST('0' AS SMALLINT), 0);
select trunc(CAST('0' AS SMALLINT), -1);

-- trunc TINYINT
select trunc(CAST('127' AS TINYINT), 2);
select trunc(CAST('127' AS TINYINT), 1);
select trunc(CAST('127' AS TINYINT), 1);
select trunc(CAST('127' AS TINYINT), 0);
select trunc(CAST('127' AS TINYINT), -1);
select trunc(CAST('127' AS TINYINT), -2);
select trunc(CAST('127' AS TINYINT), -3);
select trunc(CAST('127' AS TINYINT), -4);
select trunc(CAST('127' AS TINYINT), -5);
select trunc(CAST('0' AS TINYINT), 1);
select trunc(CAST('0' AS TINYINT), 0);
select trunc(CAST('0' AS TINYINT), -1);
