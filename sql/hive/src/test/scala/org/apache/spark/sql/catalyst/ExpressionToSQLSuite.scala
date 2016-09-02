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

package org.apache.spark.sql.catalyst

import scala.util.control.NonFatal

import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SQLTestUtils

class ExpressionToSQLSuite extends SQLBuilderTest with SQLTestUtils {
  import testImplicits._

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    sql("DROP TABLE IF EXISTS t0")
    sql("DROP TABLE IF EXISTS t1")
    sql("DROP TABLE IF EXISTS t2")

    val bytes = Array[Byte](1, 2, 3, 4)
    Seq((bytes, "AQIDBA==")).toDF("a", "b").write.saveAsTable("t0")

    spark
      .range(10)
      .select('id as 'key, concat(lit("val_"), 'id) as 'value)
      .write
      .saveAsTable("t1")

    spark.range(10).select('id as 'a, 'id as 'b, 'id as 'c, 'id as 'd).write.saveAsTable("t2")
  }

  override protected def afterAll(): Unit = {
    try {
      sql("DROP TABLE IF EXISTS t0")
      sql("DROP TABLE IF EXISTS t1")
      sql("DROP TABLE IF EXISTS t2")
    } finally {
      super.afterAll()
    }
  }

  private def checkSqlGeneration(hiveQl: String): Unit = {
    val df = sql(hiveQl)

    val convertedSQL = try df.logicalPlan.sql catch {
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
    checkSqlGeneration("SELECT abs(15), abs(-15)")
    checkSqlGeneration("SELECT array(1,2,3)")
    checkSqlGeneration("SELECT coalesce(null, 1, 2)")
    checkSqlGeneration("SELECT explode(array(1,2,3))")
    checkSqlGeneration("SELECT greatest(1,null,3)")
    checkSqlGeneration("SELECT if(1==2, 'yes', 'no')")
    checkSqlGeneration("SELECT isnan(15), isnan('invalid')")
    checkSqlGeneration("SELECT isnull(null), isnull('a')")
    checkSqlGeneration("SELECT isnotnull(null), isnotnull('a')")
    checkSqlGeneration("SELECT least(1,null,3)")
    checkSqlGeneration("SELECT map(1, 'a', 2, 'b')")
    checkSqlGeneration("SELECT named_struct('c1',1,'c2',2,'c3',3)")
    checkSqlGeneration("SELECT nanvl(a, 5), nanvl(b, 10), nanvl(d, c) from t2")
    checkSqlGeneration("SELECT rand(1)")
    checkSqlGeneration("SELECT randn(3)")
    checkSqlGeneration("SELECT struct(1,2,3)")
  }

  test("math functions") {
    checkSqlGeneration("SELECT acos(-1)")
    checkSqlGeneration("SELECT asin(-1)")
    checkSqlGeneration("SELECT atan(1)")
    checkSqlGeneration("SELECT atan2(1, 1)")
    checkSqlGeneration("SELECT bin(10)")
    checkSqlGeneration("SELECT cbrt(1000.0)")
    checkSqlGeneration("SELECT ceil(2.333)")
    checkSqlGeneration("SELECT ceiling(2.333)")
    checkSqlGeneration("SELECT cos(1.0)")
    checkSqlGeneration("SELECT cosh(1.0)")
    checkSqlGeneration("SELECT conv(15, 10, 16)")
    checkSqlGeneration("SELECT degrees(pi())")
    checkSqlGeneration("SELECT e()")
    checkSqlGeneration("SELECT exp(1.0)")
    checkSqlGeneration("SELECT expm1(1.0)")
    checkSqlGeneration("SELECT floor(-2.333)")
    checkSqlGeneration("SELECT factorial(5)")
    checkSqlGeneration("SELECT hex(10)")
    checkSqlGeneration("SELECT hypot(3, 4)")
    checkSqlGeneration("SELECT log(10.0)")
    checkSqlGeneration("SELECT log10(1000.0)")
    checkSqlGeneration("SELECT log1p(0.0)")
    checkSqlGeneration("SELECT log2(8.0)")
    checkSqlGeneration("SELECT ln(10.0)")
    checkSqlGeneration("SELECT negative(-1)")
    checkSqlGeneration("SELECT pi()")
    checkSqlGeneration("SELECT pmod(3, 2)")
    checkSqlGeneration("SELECT positive(3)")
    checkSqlGeneration("SELECT pow(2, 3)")
    checkSqlGeneration("SELECT power(2, 3)")
    checkSqlGeneration("SELECT radians(180.0)")
    checkSqlGeneration("SELECT rint(1.63)")
    checkSqlGeneration("SELECT round(31.415, -1)")
    checkSqlGeneration("SELECT shiftleft(2, 3)")
    checkSqlGeneration("SELECT shiftright(16, 3)")
    checkSqlGeneration("SELECT shiftrightunsigned(16, 3)")
    checkSqlGeneration("SELECT sign(-2.63)")
    checkSqlGeneration("SELECT signum(-2.63)")
    checkSqlGeneration("SELECT sin(1.0)")
    checkSqlGeneration("SELECT sinh(1.0)")
    checkSqlGeneration("SELECT sqrt(100.0)")
    checkSqlGeneration("SELECT tan(1.0)")
    checkSqlGeneration("SELECT tanh(1.0)")
  }

  test("aggregate functions") {
    checkSqlGeneration("SELECT approx_count_distinct(value) FROM t1 GROUP BY key")
    checkSqlGeneration("SELECT percentile_approx(value, 0.25) FROM t1 GROUP BY key")
    checkSqlGeneration("SELECT percentile_approx(value, array(0.25, 0.75)) FROM t1 GROUP BY key")
    checkSqlGeneration("SELECT percentile_approx(value, 0.25, 100) FROM t1 GROUP BY key")
    checkSqlGeneration(
      "SELECT percentile_approx(value, array(0.25, 0.75), 100) FROM t1 GROUP BY key")
    checkSqlGeneration("SELECT avg(value) FROM t1 GROUP BY key")
    checkSqlGeneration("SELECT corr(value, key) FROM t1 GROUP BY key")
    checkSqlGeneration("SELECT count(value) FROM t1 GROUP BY key")
    checkSqlGeneration("SELECT covar_pop(value, key) FROM t1 GROUP BY key")
    checkSqlGeneration("SELECT covar_samp(value, key) FROM t1 GROUP BY key")
    checkSqlGeneration("SELECT first(value) FROM t1 GROUP BY key")
    checkSqlGeneration("SELECT first_value(value) FROM t1 GROUP BY key")
    checkSqlGeneration("SELECT kurtosis(value) FROM t1 GROUP BY key")
    checkSqlGeneration("SELECT last(value) FROM t1 GROUP BY key")
    checkSqlGeneration("SELECT last_value(value) FROM t1 GROUP BY key")
    checkSqlGeneration("SELECT max(value) FROM t1 GROUP BY key")
    checkSqlGeneration("SELECT mean(value) FROM t1 GROUP BY key")
    checkSqlGeneration("SELECT min(value) FROM t1 GROUP BY key")
    checkSqlGeneration("SELECT skewness(value) FROM t1 GROUP BY key")
    checkSqlGeneration("SELECT stddev(value) FROM t1 GROUP BY key")
    checkSqlGeneration("SELECT stddev_pop(value) FROM t1 GROUP BY key")
    checkSqlGeneration("SELECT stddev_samp(value) FROM t1 GROUP BY key")
    checkSqlGeneration("SELECT sum(value) FROM t1 GROUP BY key")
    checkSqlGeneration("SELECT variance(value) FROM t1 GROUP BY key")
    checkSqlGeneration("SELECT var_pop(value) FROM t1 GROUP BY key")
    checkSqlGeneration("SELECT var_samp(value) FROM t1 GROUP BY key")
  }

  test("string functions") {
    checkSqlGeneration("SELECT ascii('SparkSql')")
    checkSqlGeneration("SELECT base64(a) FROM t0")
    checkSqlGeneration("SELECT concat('This ', 'is ', 'a ', 'test')")
    checkSqlGeneration("SELECT concat_ws(' ', 'This', 'is', 'a', 'test')")
    checkSqlGeneration("SELECT decode(a, 'UTF-8') FROM t0")
    checkSqlGeneration("SELECT encode('SparkSql', 'UTF-8')")
    checkSqlGeneration("SELECT find_in_set('ab', 'abc,b,ab,c,def')")
    checkSqlGeneration("SELECT format_number(1234567.890, 2)")
    checkSqlGeneration("SELECT format_string('aa%d%s',123, 'cc')")
    checkSqlGeneration("SELECT get_json_object('{\"a\":\"bc\"}','$.a')")
    checkSqlGeneration("SELECT initcap('This is a test')")
    checkSqlGeneration("SELECT instr('This is a test', 'is')")
    checkSqlGeneration("SELECT lcase('SparkSql')")
    checkSqlGeneration("SELECT length('This is a test')")
    checkSqlGeneration("SELECT levenshtein('This is a test', 'Another test')")
    checkSqlGeneration("SELECT lower('SparkSql')")
    checkSqlGeneration("SELECT locate('is', 'This is a test', 3)")
    checkSqlGeneration("SELECT lpad('SparkSql', 16, 'Learning')")
    checkSqlGeneration("SELECT ltrim('  SparkSql ')")
    checkSqlGeneration("SELECT json_tuple('{\"f1\": \"value1\", \"f2\": \"value2\"}','f1')")
    checkSqlGeneration("SELECT printf('aa%d%s', 123, 'cc')")
    checkSqlGeneration("SELECT regexp_extract('100-200', '(\\d+)-(\\d+)', 1)")
    checkSqlGeneration("SELECT regexp_replace('100-200', '(\\d+)', 'num')")
    checkSqlGeneration("SELECT repeat('SparkSql', 3)")
    checkSqlGeneration("SELECT reverse('SparkSql')")
    checkSqlGeneration("SELECT rpad('SparkSql', 16, ' is Cool')")
    checkSqlGeneration("SELECT rtrim('  SparkSql ')")
    checkSqlGeneration("SELECT soundex('SparkSql')")
    checkSqlGeneration("SELECT space(2)")
    checkSqlGeneration("SELECT split('aa2bb3cc', '[1-9]+')")
    checkSqlGeneration("SELECT space(2)")
    checkSqlGeneration("SELECT substr('This is a test', 1)")
    checkSqlGeneration("SELECT substring('This is a test', 1)")
    checkSqlGeneration("SELECT substring_index('www.apache.org','.',1)")
    checkSqlGeneration("SELECT translate('translate', 'rnlt', '123')")
    checkSqlGeneration("SELECT trim('  SparkSql ')")
    checkSqlGeneration("SELECT ucase('SparkSql')")
    checkSqlGeneration("SELECT unbase64('SparkSql')")
    checkSqlGeneration("SELECT unhex(41)")
    checkSqlGeneration("SELECT upper('SparkSql')")
  }

  test("datetime functions") {
    checkSqlGeneration("SELECT add_months('2001-03-31', 1)")
    checkSqlGeneration("SELECT count(current_date())")
    checkSqlGeneration("SELECT count(current_timestamp())")
    checkSqlGeneration("SELECT datediff('2001-01-02', '2001-01-01')")
    checkSqlGeneration("SELECT date_add('2001-01-02', 1)")
    checkSqlGeneration("SELECT date_format('2001-05-02', 'yyyy-dd')")
    checkSqlGeneration("SELECT date_sub('2001-01-02', 1)")
    checkSqlGeneration("SELECT day('2001-05-02')")
    checkSqlGeneration("SELECT dayofyear('2001-05-02')")
    checkSqlGeneration("SELECT dayofmonth('2001-05-02')")
    checkSqlGeneration("SELECT from_unixtime(1000, 'yyyy-MM-dd HH:mm:ss')")
    checkSqlGeneration("SELECT from_utc_timestamp('2015-07-24 00:00:00', 'PST')")
    checkSqlGeneration("SELECT hour('11:35:55')")
    checkSqlGeneration("SELECT last_day('2001-01-01')")
    checkSqlGeneration("SELECT minute('11:35:55')")
    checkSqlGeneration("SELECT month('2001-05-02')")
    checkSqlGeneration("SELECT months_between('2001-10-30 10:30:00', '1996-10-30')")
    checkSqlGeneration("SELECT next_day('2001-05-02', 'TU')")
    checkSqlGeneration("SELECT count(now())")
    checkSqlGeneration("SELECT quarter('2001-05-02')")
    checkSqlGeneration("SELECT second('11:35:55')")
    checkSqlGeneration("SELECT to_date('2001-10-30 10:30:00')")
    checkSqlGeneration("SELECT to_unix_timestamp('2015-07-24 00:00:00', 'yyyy-MM-dd HH:mm:ss')")
    checkSqlGeneration("SELECT to_utc_timestamp('2015-07-24 00:00:00', 'PST')")
    checkSqlGeneration("SELECT trunc('2001-10-30 10:30:00', 'YEAR')")
    checkSqlGeneration("SELECT unix_timestamp('2001-10-30 10:30:00')")
    checkSqlGeneration("SELECT weekofyear('2001-05-02')")
    checkSqlGeneration("SELECT year('2001-05-02')")

    checkSqlGeneration("SELECT interval 3 years - 3 month 7 week 123 microseconds as i")
  }

  test("collection functions") {
    checkSqlGeneration("SELECT array_contains(array(2, 9, 8), 9)")
    checkSqlGeneration("SELECT size(array('b', 'd', 'c', 'a'))")
    checkSqlGeneration("SELECT sort_array(array('b', 'd', 'c', 'a'))")
  }

  test("misc functions") {
    checkSqlGeneration("SELECT crc32('Spark')")
    checkSqlGeneration("SELECT md5('Spark')")
    checkSqlGeneration("SELECT hash('Spark')")
    checkSqlGeneration("SELECT sha('Spark')")
    checkSqlGeneration("SELECT sha1('Spark')")
    checkSqlGeneration("SELECT sha2('Spark', 0)")
    checkSqlGeneration("SELECT spark_partition_id()")
    checkSqlGeneration("SELECT input_file_name()")
    checkSqlGeneration("SELECT monotonically_increasing_id()")
  }

  test("subquery") {
    checkSqlGeneration("SELECT 1 + (SELECT 2)")
    checkSqlGeneration("SELECT 1 + (SELECT 2 + (SELECT 3 as a))")
  }
}
