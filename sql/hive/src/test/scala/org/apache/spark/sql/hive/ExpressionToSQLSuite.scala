/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive

import scala.util.control.NonFatal

import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SQLTestUtils

class ExpressionToSQLSuite extends SQLBuilderTest with SQLTestUtils {
  import testImplicits._

  protected override def beforeAll(): Unit = {
    sql("DROP TABLE IF EXISTS t0")
    sql("DROP TABLE IF EXISTS t1")
    sql("DROP TABLE IF EXISTS t2")

    val bytes = Array[Byte](1, 2, 3, 4)
    Seq((bytes, "AQIDBA==")).toDF("a", "b").write.saveAsTable("t0")

    sqlContext
      .range(10)
      .select('id as 'key, concat(lit("val_"), 'id) as 'value)
      .write
      .saveAsTable("t1")

    sqlContext.range(10).select('id as 'a, 'id as 'b, 'id as 'c, 'id as 'd).write.saveAsTable("t2")
  }

  override protected def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS t0")
    sql("DROP TABLE IF EXISTS t1")
    sql("DROP TABLE IF EXISTS t2")
  }

  private def checkHiveQl(hiveQl: String): Unit = {
    val df = sql(hiveQl)

    val convertedSQL = try new SQLBuilder(df).toSQL catch {
      case NonFatal(e) =>
        fail(
          s"""Cannot convert the following HiveQL query plan back to SQL query string:
            |
            |# Original HiveQL query string:
            |$hiveQl
            |
            |# Resolved query plan:
            |${df.queryExecution.analyzed.treeString}
           """.stripMargin)
    }

    try {
      checkAnswer(sql(convertedSQL), df)
    } catch { case cause: Throwable =>
      fail(
        s"""Failed to execute converted SQL string or got wrong answer:
          |
          |# Converted SQL query string:
          |$convertedSQL
          |
          |# Original HiveQL query string:
          |$hiveQl
          |
          |# Resolved query plan:
          |${df.queryExecution.analyzed.treeString}
         """.stripMargin,
        cause)
    }
  }

  test("misc non-aggregate functions") {
    checkHiveQl("SELECT abs(15), abs(-15)")
    checkHiveQl("SELECT array(1,2,3)")
    checkHiveQl("SELECT coalesce(null, 1, 2)")
    // wait for resolution of JIRA SPARK-12719 SQL Generation for Generators
    // checkHiveQl("SELECT explode(array(1,2,3))")
    checkHiveQl("SELECT greatest(1,null,3)")
    checkHiveQl("SELECT if(1==2, 'yes', 'no')")
    checkHiveQl("SELECT isnan(15), isnan('invalid')")
    checkHiveQl("SELECT isnull(null), isnull('a')")
    checkHiveQl("SELECT isnotnull(null), isnotnull('a')")
    checkHiveQl("SELECT least(1,null,3)")
    checkHiveQl("SELECT named_struct('c1',1,'c2',2,'c3',3)")
    checkHiveQl("SELECT nanvl(a, 5), nanvl(b, 10), nanvl(d, c) from t2")
    checkHiveQl("SELECT nvl(null, 1, 2)")
    checkHiveQl("SELECT rand(1)")
    checkHiveQl("SELECT randn(3)")
    checkHiveQl("SELECT struct(1,2,3)")
  }

  test("math functions") {
    checkHiveQl("SELECT acos(-1)")
    checkHiveQl("SELECT asin(-1)")
    checkHiveQl("SELECT atan(1)")
    checkHiveQl("SELECT atan2(1, 1)")
    checkHiveQl("SELECT bin(10)")
    checkHiveQl("SELECT cbrt(1000.0)")
    checkHiveQl("SELECT ceil(2.333)")
    checkHiveQl("SELECT ceiling(2.333)")
    checkHiveQl("SELECT cos(1.0)")
    checkHiveQl("SELECT cosh(1.0)")
    checkHiveQl("SELECT conv(15, 10, 16)")
    checkHiveQl("SELECT degrees(pi())")
    checkHiveQl("SELECT e()")
    checkHiveQl("SELECT exp(1.0)")
    checkHiveQl("SELECT expm1(1.0)")
    checkHiveQl("SELECT floor(-2.333)")
    checkHiveQl("SELECT factorial(5)")
    checkHiveQl("SELECT hex(10)")
    checkHiveQl("SELECT hypot(3, 4)")
    checkHiveQl("SELECT log(10.0)")
    checkHiveQl("SELECT log10(1000.0)")
    checkHiveQl("SELECT log1p(0.0)")
    checkHiveQl("SELECT log2(8.0)")
    checkHiveQl("SELECT ln(10.0)")
    checkHiveQl("SELECT negative(-1)")
    checkHiveQl("SELECT pi()")
    checkHiveQl("SELECT pmod(3, 2)")
    checkHiveQl("SELECT positive(3)")
    checkHiveQl("SELECT pow(2, 3)")
    checkHiveQl("SELECT power(2, 3)")
    checkHiveQl("SELECT radians(180.0)")
    checkHiveQl("SELECT rint(1.63)")
    checkHiveQl("SELECT round(31.415, -1)")
    checkHiveQl("SELECT shiftleft(2, 3)")
    checkHiveQl("SELECT shiftright(16, 3)")
    checkHiveQl("SELECT shiftrightunsigned(16, 3)")
    checkHiveQl("SELECT sign(-2.63)")
    checkHiveQl("SELECT signum(-2.63)")
    checkHiveQl("SELECT sin(1.0)")
    checkHiveQl("SELECT sinh(1.0)")
    checkHiveQl("SELECT sqrt(100.0)")
    checkHiveQl("SELECT tan(1.0)")
    checkHiveQl("SELECT tanh(1.0)")
  }

  test("aggregate functions") {
    checkHiveQl("SELECT approx_count_distinct(value) FROM t1 GROUP BY key")
    checkHiveQl("SELECT avg(value) FROM t1 GROUP BY key")
    checkHiveQl("SELECT corr(value, key) FROM t1 GROUP BY key")
    checkHiveQl("SELECT count(value) FROM t1 GROUP BY key")
    checkHiveQl("SELECT covar_pop(value, key) FROM t1 GROUP BY key")
    checkHiveQl("SELECT covar_samp(value, key) FROM t1 GROUP BY key")
    checkHiveQl("SELECT first(value) FROM t1 GROUP BY key")
    checkHiveQl("SELECT first_value(value) FROM t1 GROUP BY key")
    checkHiveQl("SELECT kurtosis(value) FROM t1 GROUP BY key")
    checkHiveQl("SELECT last(value) FROM t1 GROUP BY key")
    checkHiveQl("SELECT last_value(value) FROM t1 GROUP BY key")
    checkHiveQl("SELECT max(value) FROM t1 GROUP BY key")
    checkHiveQl("SELECT mean(value) FROM t1 GROUP BY key")
    checkHiveQl("SELECT min(value) FROM t1 GROUP BY key")
    checkHiveQl("SELECT skewness(value) FROM t1 GROUP BY key")
    checkHiveQl("SELECT stddev(value) FROM t1 GROUP BY key")
    checkHiveQl("SELECT stddev_pop(value) FROM t1 GROUP BY key")
    checkHiveQl("SELECT stddev_samp(value) FROM t1 GROUP BY key")
    checkHiveQl("SELECT sum(value) FROM t1 GROUP BY key")
    checkHiveQl("SELECT variance(value) FROM t1 GROUP BY key")
    checkHiveQl("SELECT var_pop(value) FROM t1 GROUP BY key")
    checkHiveQl("SELECT var_samp(value) FROM t1 GROUP BY key")
  }

  test("string functions") {
    checkHiveQl("SELECT ascii('SparkSql')")
    checkHiveQl("SELECT base64(a) FROM t0")
    checkHiveQl("SELECT concat('This ', 'is ', 'a ', 'test')")
    checkHiveQl("SELECT concat_ws(' ', 'This', 'is', 'a', 'test')")
    checkHiveQl("SELECT decode(a, 'UTF-8') FROM t0")
    checkHiveQl("SELECT encode('SparkSql', 'UTF-8')")
    checkHiveQl("SELECT find_in_set('ab', 'abc,b,ab,c,def')")
    checkHiveQl("SELECT format_number(1234567.890, 2)")
    checkHiveQl("SELECT format_string('aa%d%s',123, 'cc')")
    checkHiveQl("SELECT get_json_object('{\"a\":\"bc\"}','$.a')")
    checkHiveQl("SELECT initcap('This is a test')")
    checkHiveQl("SELECT instr('This is a test', 'is')")
    checkHiveQl("SELECT lcase('SparkSql')")
    checkHiveQl("SELECT length('This is a test')")
    checkHiveQl("SELECT levenshtein('This is a test', 'Another test')")
    checkHiveQl("SELECT lower('SparkSql')")
    checkHiveQl("SELECT locate('is', 'This is a test', 3)")
    checkHiveQl("SELECT lpad('SparkSql', 16, 'Learning')")
    checkHiveQl("SELECT ltrim('  SparkSql ')")
    // wait for resolution of JIRA SPARK-12719 SQL Generation for Generators
    // checkHiveQl("SELECT json_tuple('{\"f1\": \"value1\", \"f2\": \"value2\"}','f1')")
    checkHiveQl("SELECT printf('aa%d%s', 123, 'cc')")
    checkHiveQl("SELECT regexp_extract('100-200', '(\\d+)-(\\d+)', 1)")
    checkHiveQl("SELECT regexp_replace('100-200', '(\\d+)', 'num')")
    checkHiveQl("SELECT repeat('SparkSql', 3)")
    checkHiveQl("SELECT reverse('SparkSql')")
    checkHiveQl("SELECT rpad('SparkSql', 16, ' is Cool')")
    checkHiveQl("SELECT rtrim('  SparkSql ')")
    checkHiveQl("SELECT soundex('SparkSql')")
    checkHiveQl("SELECT space(2)")
    checkHiveQl("SELECT split('aa2bb3cc', '[1-9]+')")
    checkHiveQl("SELECT space(2)")
    checkHiveQl("SELECT substr('This is a test', 'is')")
    checkHiveQl("SELECT substring('This is a test', 'is')")
    checkHiveQl("SELECT substring_index('www.apache.org','.',1)")
    checkHiveQl("SELECT translate('translate', 'rnlt', '123')")
    checkHiveQl("SELECT trim('  SparkSql ')")
    checkHiveQl("SELECT ucase('SparkSql')")
    checkHiveQl("SELECT unbase64('SparkSql')")
    checkHiveQl("SELECT unhex(41)")
    checkHiveQl("SELECT upper('SparkSql')")
  }

  test("datetime functions") {
    checkHiveQl("SELECT add_months('2001-03-31', 1)")
    checkHiveQl("SELECT count(current_date())")
    checkHiveQl("SELECT count(current_timestamp())")
    checkHiveQl("SELECT datediff('2001-01-02', '2001-01-01')")
    checkHiveQl("SELECT date_add('2001-01-02', 1)")
    checkHiveQl("SELECT date_format('2001-05-02', 'yyyy-dd')")
    checkHiveQl("SELECT date_sub('2001-01-02', 1)")
    checkHiveQl("SELECT day('2001-05-02')")
    checkHiveQl("SELECT dayofyear('2001-05-02')")
    checkHiveQl("SELECT dayofmonth('2001-05-02')")
    checkHiveQl("SELECT from_unixtime(1000, 'yyyy-MM-dd HH:mm:ss')")
    checkHiveQl("SELECT from_utc_timestamp('2015-07-24 00:00:00', 'PST')")
    checkHiveQl("SELECT hour('11:35:55')")
    checkHiveQl("SELECT last_day('2001-01-01')")
    checkHiveQl("SELECT minute('11:35:55')")
    checkHiveQl("SELECT month('2001-05-02')")
    checkHiveQl("SELECT months_between('2001-10-30 10:30:00', '1996-10-30')")
    checkHiveQl("SELECT next_day('2001-05-02', 'TU')")
    checkHiveQl("SELECT count(now())")
    checkHiveQl("SELECT quarter('2001-05-02')")
    checkHiveQl("SELECT second('11:35:55')")
    checkHiveQl("SELECT to_date('2001-10-30 10:30:00')")
    checkHiveQl("SELECT to_unix_timestamp('2015-07-24 00:00:00', 'yyyy-MM-dd HH:mm:ss')")
    checkHiveQl("SELECT to_utc_timestamp('2015-07-24 00:00:00', 'PST')")
    checkHiveQl("SELECT trunc('2001-10-30 10:30:00', 'YEAR')")
    checkHiveQl("SELECT unix_timestamp('2001-10-30 10:30:00')")
    checkHiveQl("SELECT weekofyear('2001-05-02')")
    checkHiveQl("SELECT year('2001-05-02')")

    checkHiveQl("SELECT interval 3 years - 3 month 7 week 123 microseconds as i")
  }

  test("collection functions") {
    checkHiveQl("SELECT array_contains(array(2, 9, 8), 9)")
    checkHiveQl("SELECT size(array('b', 'd', 'c', 'a'))")
    checkHiveQl("SELECT sort_array(array('b', 'd', 'c', 'a'))")
  }

  test("misc functions") {
    checkHiveQl("SELECT crc32('Spark')")
    checkHiveQl("SELECT md5('Spark')")
    checkHiveQl("SELECT hash('Spark')")
    checkHiveQl("SELECT sha('Spark')")
    checkHiveQl("SELECT sha1('Spark')")
    checkHiveQl("SELECT sha2('Spark', 0)")
    checkHiveQl("SELECT spark_partition_id()")
    checkHiveQl("SELECT input_file_name()")
    checkHiveQl("SELECT monotonically_increasing_id()")
  }
}
