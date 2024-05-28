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

package org.apache.spark.sql

import org.apache.spark.{SparkConf, SparkRuntimeException}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.SchemaRequiredDataSource
import org.apache.spark.sql.connector.catalog.InMemoryPartitionTableCatalog
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.SimpleInsertSource
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types._

// The base trait for char/varchar tests that need to be run with different table implementations.
trait CharVarcharTestSuite extends QueryTest with SQLTestUtils {

  def format: String

  def checkColType(f: StructField, dt: DataType): Unit = {
    assert(f.dataType == CharVarcharUtils.replaceCharVarcharWithString(dt))
    assert(CharVarcharUtils.getRawType(f.metadata) == Some(dt))
  }

  def checkPlainResult(df: DataFrame, dt: String, insertVal: String): Unit = {
    val dataType = CatalystSqlParser.parseDataType(dt)
    checkColType(df.schema(1), dataType)
    dataType match {
      case CharType(len) =>
        // char value will be padded if (<= len) or trimmed if (> len)
        val fixLenStr = if (insertVal != null) {
          insertVal.take(len).padTo(len, " ").mkString
        } else null
        checkAnswer(df, Row("1", fixLenStr))
      case VarcharType(len) =>
        // varchar value will be remained if (<= len) or trimmed if (> len)
        val varLenStrWithUpperBound = if (insertVal != null) {
          insertVal.take(len)
        } else null
        checkAnswer(df, Row("1", varLenStrWithUpperBound))
    }
  }

  def assertLengthCheckFailure(query: String): Unit = {
    assertLengthCheckFailure(() => sql(query))
  }

  def assertLengthCheckFailure(func: () => Unit): Unit = {
    checkError(
      exception = intercept[SparkRuntimeException](func()),
      errorClass = "EXCEED_LIMIT_LENGTH",
      parameters = Map("limit" -> "5")
    )
  }

  test("apply char padding/trimming and varchar trimming: top-level columns") {
    Seq("CHAR(5)", "VARCHAR(5)").foreach { typ =>
      withTable("t") {
        sql(s"CREATE TABLE t(i STRING, c $typ) USING $format")
        (0 to 5).map(n => "a" + " " * n).foreach { v =>
          sql(s"INSERT OVERWRITE t VALUES ('1', '$v')")
          checkPlainResult(spark.table("t"), typ, v)
        }
        sql("INSERT OVERWRITE t VALUES ('1', null)")
        checkPlainResult(spark.table("t"), typ, null)
      }
    }
  }

  test("char type values should be padded or trimmed: partitioned columns") {
    // via dynamic partitioned columns
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c CHAR(5)) USING $format PARTITIONED BY (c)")
      (0 to 5).map(n => "a" + " " * n).foreach { v =>
        sql(s"INSERT OVERWRITE t VALUES ('1', '$v')")
        checkPlainResult(spark.table("t"), "CHAR(5)", v)
      }
    }

    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c CHAR(5)) USING $format PARTITIONED BY (c)")
      (0 to 5).map(n => "a" + " " * n).foreach { v =>
        // via dynamic partitioned columns with drop partition command
        sql(s"INSERT INTO t VALUES ('1', '$v')")
        checkPlainResult(spark.table("t"), "CHAR(5)", v)
        sql(s"ALTER TABLE t DROP PARTITION(c='a')")
        checkAnswer(spark.table("t"), Nil)

        // via static partitioned columns with drop partition command
        sql(s"INSERT INTO t PARTITION (c ='$v') VALUES ('1')")
        checkPlainResult(spark.table("t"), "CHAR(5)", v)
        sql(s"ALTER TABLE t DROP PARTITION(c='a')")
        checkAnswer(spark.table("t"), Nil)
      }
    }
  }

  test("char type values should not be padded when charVarcharAsString is true") {
    withSQLConf(SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING.key -> "true") {
      withTable("t") {
        sql(s"CREATE TABLE t(a STRING, b CHAR(5), c CHAR(5)) USING $format partitioned by (c)")
        sql("INSERT INTO t VALUES ('abc', 'abc', 'abc')")
        checkAnswer(sql("SELECT b FROM t WHERE b='abc'"), Row("abc"))
        checkAnswer(sql("SELECT b FROM t WHERE b in ('abc')"), Row("abc"))
        checkAnswer(sql("SELECT c FROM t WHERE c='abc'"), Row("abc"))
        checkAnswer(sql("SELECT c FROM t WHERE c in ('abc')"), Row("abc"))
      }
    }
  }

  test("varchar type values length check and trim: partitioned columns") {
    (0 to 5).foreach { n =>
      // SPARK-34192: we need to create a a new table for each round of test because of
      // trailing spaces in partition column will be treated differently.
      // This is because Mysql and Derby(used in tests) considers 'a' = 'a '
      // whereas others like (Postgres, Oracle) doesn't exhibit this problem.
      // see more at:
      // https://issues.apache.org/jira/browse/HIVE-13618
      // https://issues.apache.org/jira/browse/SPARK-34192
      withTable("t") {
        sql(s"CREATE TABLE t(i STRING, c VARCHAR(5)) USING $format PARTITIONED BY (c)")
        val v = "a" + " " * n
        // via dynamic partitioned columns
        sql(s"INSERT INTO t VALUES ('1', '$v')")
        checkPlainResult(spark.table("t"), "VARCHAR(5)", v)
        sql(s"ALTER TABLE t DROP PARTITION(c='$v')")
        checkAnswer(spark.table("t"), Nil)

        // via static partitioned columns
        sql(s"INSERT INTO t PARTITION (c='$v') VALUES ('1')")
        checkPlainResult(spark.table("t"), "VARCHAR(5)", v)
        sql(s"ALTER TABLE t DROP PARTITION(c='$v')")
        checkAnswer(spark.table("t"), Nil)
      }
    }
  }

  test("oversize char/varchar values for alter table partition operations") {
    Seq("CHAR(5)", "VARCHAR(5)").foreach { typ =>
      withTable("t") {
        sql(s"CREATE TABLE t(i STRING, c $typ) USING $format PARTITIONED BY (c)")
        Seq("ADD", "DROP").foreach { op =>
          assertLengthCheckFailure(s"ALTER TABLE t $op PARTITION(c='abcdef')")
        }
        assertLengthCheckFailure(
          "ALTER TABLE t PARTITION (c='abcdef') RENAME TO PARTITION (c='2')")
        assertLengthCheckFailure(
          "ALTER TABLE t PARTITION (c='1') RENAME TO PARTITION (c='abcdef')")
      }
    }
  }

  test("SPARK-34233: char/varchar with null value for partitioned columns") {
    Seq("CHAR(5)", "VARCHAR(5)").foreach { typ =>
      withTable("t") {
        sql(s"CREATE TABLE t(i STRING, c $typ) USING $format PARTITIONED BY (c)")
        sql("INSERT INTO t VALUES ('1', null)")
        checkPlainResult(spark.table("t"), typ, null)
        sql("INSERT OVERWRITE t VALUES ('1', null)")
        checkPlainResult(spark.table("t"), typ, null)
        sql("INSERT OVERWRITE t PARTITION (c=null) VALUES ('1')")
        checkPlainResult(spark.table("t"), typ, null)
        sql("ALTER TABLE t DROP PARTITION(c=null)")
        checkAnswer(spark.table("t"), Nil)
      }
    }
  }

  test("char type values should be padded: nested in struct") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c STRUCT<c: CHAR(5)>) USING $format")
      sql("INSERT INTO t VALUES ('1', struct('a'))")
      checkAnswer(spark.table("t"), Row("1", Row("a" + " " * 4)))
      checkColType(spark.table("t").schema(1), new StructType().add("c", CharType(5)))

      sql("INSERT OVERWRITE t VALUES ('1', null)")
      checkAnswer(spark.table("t"), Row("1", null))
      sql("INSERT OVERWRITE t VALUES ('1', struct(null))")
      checkAnswer(spark.table("t"), Row("1", Row(null)))
    }
  }

  test("char type values should be padded: nested in array") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c ARRAY<CHAR(5)>) USING $format")
      sql("INSERT INTO t VALUES ('1', array('a', 'ab'))")
      checkAnswer(spark.table("t"), Row("1", Seq("a" + " " * 4, "ab" + " " * 3)))
      checkColType(spark.table("t").schema(1), ArrayType(CharType(5)))

      sql("INSERT OVERWRITE t VALUES ('1', null)")
      checkAnswer(spark.table("t"), Row("1", null))
      sql("INSERT OVERWRITE t VALUES ('1', array(null))")
      checkAnswer(spark.table("t"), Row("1", Seq(null)))
    }
  }

  test("char type values should be padded: nested in map key") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c MAP<CHAR(5), STRING>) USING $format")
      sql("INSERT INTO t VALUES ('1', map('a', 'ab'))")
      checkAnswer(spark.table("t"), Row("1", Map(("a" + " " * 4, "ab"))))
      checkColType(spark.table("t").schema(1), MapType(CharType(5), StringType))

      sql("INSERT OVERWRITE t VALUES ('1', null)")
      checkAnswer(spark.table("t"), Row("1", null))
    }
  }

  test("char type values should be padded: nested in map value") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c MAP<STRING, CHAR(5)>) USING $format")
      sql("INSERT INTO t VALUES ('1', map('a', 'ab'))")
      checkAnswer(spark.table("t"), Row("1", Map(("a", "ab" + " " * 3))))
      checkColType(spark.table("t").schema(1), MapType(StringType, CharType(5)))

      sql("INSERT OVERWRITE t VALUES ('1', null)")
      checkAnswer(spark.table("t"), Row("1", null))
      sql("INSERT OVERWRITE t VALUES ('1', map('a', null))")
      checkAnswer(spark.table("t"), Row("1", Map("a" -> null)))
    }
  }

  test("char type values should be padded: nested in both map key and value") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c MAP<CHAR(5), CHAR(10)>) USING $format")
      sql("INSERT INTO t VALUES ('1', map('a', 'ab'))")
      checkAnswer(spark.table("t"), Row("1", Map(("a" + " " * 4, "ab" + " " * 8))))
      checkColType(spark.table("t").schema(1), MapType(CharType(5), CharType(10)))

      sql("INSERT OVERWRITE t VALUES ('1', null)")
      checkAnswer(spark.table("t"), Row("1", null))
    }
  }

  test("char type values should be padded: nested in struct of array") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c STRUCT<c: ARRAY<CHAR(5)>>) USING $format")
      sql("INSERT INTO t VALUES ('1', struct(array('a', 'ab')))")
      checkAnswer(spark.table("t"), Row("1", Row(Seq("a" + " " * 4, "ab" + " " * 3))))
      checkColType(spark.table("t").schema(1),
        new StructType().add("c", ArrayType(CharType(5))))

      sql("INSERT OVERWRITE t VALUES ('1', null)")
      checkAnswer(spark.table("t"), Row("1", null))
      sql("INSERT OVERWRITE t VALUES ('1', struct(null))")
      checkAnswer(spark.table("t"), Row("1", Row(null)))
      sql("INSERT OVERWRITE t VALUES ('1', struct(array(null)))")
      checkAnswer(spark.table("t"), Row("1", Row(Seq(null))))
    }
  }

  test("char type values should be padded: nested in array of struct") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c ARRAY<STRUCT<c: CHAR(5)>>) USING $format")
      sql("INSERT INTO t VALUES ('1', array(struct('a'), struct('ab')))")
      checkAnswer(spark.table("t"), Row("1", Seq(Row("a" + " " * 4), Row("ab" + " " * 3))))
      checkColType(spark.table("t").schema(1),
        ArrayType(new StructType().add("c", CharType(5))))

      sql("INSERT OVERWRITE t VALUES ('1', null)")
      checkAnswer(spark.table("t"), Row("1", null))
      sql("INSERT OVERWRITE t VALUES ('1', array(null))")
      checkAnswer(spark.table("t"), Row("1", Seq(null)))
      sql("INSERT OVERWRITE t VALUES ('1', array(struct(null)))")
      checkAnswer(spark.table("t"), Row("1", Seq(Row(null))))
    }
  }

  test("char type values should be padded: nested in array of array") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c ARRAY<ARRAY<CHAR(5)>>) USING $format")
      sql("INSERT INTO t VALUES ('1', array(array('a', 'ab')))")
      checkAnswer(spark.table("t"), Row("1", Seq(Seq("a" + " " * 4, "ab" + " " * 3))))
      checkColType(spark.table("t").schema(1), ArrayType(ArrayType(CharType(5))))

      sql("INSERT OVERWRITE t VALUES ('1', null)")
      checkAnswer(spark.table("t"), Row("1", null))
      sql("INSERT OVERWRITE t VALUES ('1', array(null))")
      checkAnswer(spark.table("t"), Row("1", Seq(null)))
      sql("INSERT OVERWRITE t VALUES ('1', array(array(null)))")
      checkAnswer(spark.table("t"), Row("1", Seq(Seq(null))))
    }
  }

  private def testTableWrite(f: String => Unit): Unit = {
    withTable("t") { f("char") }
    withTable("t") { f("varchar") }
  }

  test("length check for input string values: top-level columns") {
    testTableWrite { typeName =>
      sql(s"CREATE TABLE t(c $typeName(5)) USING $format")
      sql("INSERT INTO t VALUES (null)")
      checkAnswer(spark.table("t"), Row(null))
      assertLengthCheckFailure("INSERT INTO t VALUES ('123456')")
    }
  }

  test("length check for input string values: partitioned columns") {
    // DS V2 doesn't support partitioned table.
    if (!conf.contains(SQLConf.DEFAULT_CATALOG.key)) {
      val tableName = "t"
      testTableWrite { typeName =>
        sql(s"CREATE TABLE $tableName(i INT, c $typeName(5)) USING $format PARTITIONED BY (c)")
        sql(s"INSERT INTO $tableName VALUES (1, null)")
        checkAnswer(spark.table(tableName), Row(1, null))
        assertLengthCheckFailure(s"INSERT INTO $tableName VALUES (1, '123456')")
      }
    }
  }

  test("length check for input string values: nested in struct") {
    testTableWrite { typeName =>
      sql(s"CREATE TABLE t(c STRUCT<c: $typeName(5)>) USING $format")
      sql("INSERT INTO t SELECT struct(null)")
      checkAnswer(spark.table("t"), Row(Row(null)))
      assertLengthCheckFailure("INSERT INTO t SELECT struct('123456')")
    }
  }

  test("length check for input string values: nested in array") {
    testTableWrite { typeName =>
      sql(s"CREATE TABLE t(c ARRAY<$typeName(5)>) USING $format")
      sql("INSERT INTO t VALUES (array(null))")
      checkAnswer(spark.table("t"), Row(Seq(null)))
      assertLengthCheckFailure("INSERT INTO t VALUES (array('a', '123456'))")
    }
  }

  test("length check for input string values: nested in map key") {
    testTableWrite { typeName =>
      sql(s"CREATE TABLE t(c MAP<$typeName(5), STRING>) USING $format")
      assertLengthCheckFailure("INSERT INTO t VALUES (map('123456', 'a'))")
    }
  }

  test("length check for input string values: nested in map value") {
    testTableWrite { typeName =>
      sql(s"CREATE TABLE t(c MAP<STRING, $typeName(5)>) USING $format")
      sql("INSERT INTO t VALUES (map('a', null))")
      checkAnswer(spark.table("t"), Row(Map("a" -> null)))
      assertLengthCheckFailure("INSERT INTO t VALUES (map('a', '123456'))")
    }
  }

  test("length check for input string values: nested in both map key and value") {
    testTableWrite { typeName =>
      sql(s"CREATE TABLE t(c MAP<$typeName(5), $typeName(5)>) USING $format")
      assertLengthCheckFailure("INSERT INTO t VALUES (map('123456', 'a'))")
      assertLengthCheckFailure("INSERT INTO t VALUES (map('a', '123456'))")
    }
  }

  test("length check for input string values: nested in struct of array") {
    testTableWrite { typeName =>
      sql(s"CREATE TABLE t(c STRUCT<c: ARRAY<$typeName(5)>>) USING $format")
      sql("INSERT INTO t SELECT struct(array(null))")
      checkAnswer(spark.table("t"), Row(Row(Seq(null))))
      assertLengthCheckFailure("INSERT INTO t SELECT struct(array('123456'))")
    }
  }

  test("length check for input string values: nested in array of struct") {
    testTableWrite { typeName =>
      sql(s"CREATE TABLE t(c ARRAY<STRUCT<c: $typeName(5)>>) USING $format")
      sql("INSERT INTO t VALUES (array(struct(null)))")
      checkAnswer(spark.table("t"), Row(Seq(Row(null))))
      assertLengthCheckFailure("INSERT INTO t VALUES (array(struct('123456')))")
    }
  }

  test("length check for input string values: nested in array of array") {
    testTableWrite { typeName =>
      sql(s"CREATE TABLE t(c ARRAY<ARRAY<$typeName(5)>>) USING $format")
      sql("INSERT INTO t VALUES (array(array(null)))")
      checkAnswer(spark.table("t"), Row(Seq(Seq(null))))
      assertLengthCheckFailure("INSERT INTO t VALUES (array(array('123456')))")
    }
  }

  test("length check for input string values: with trailing spaces") {
    withTable("t") {
      sql(s"CREATE TABLE t(c1 CHAR(5), c2 VARCHAR(5)) USING $format")
      sql("INSERT INTO t VALUES ('12 ', '12 ')")
      sql("INSERT INTO t VALUES ('1234  ', '1234  ')")
      checkAnswer(spark.table("t"), Seq(
        Row("12" + " " * 3, "12 "),
        Row("1234 ", "1234 ")))
    }
  }

  test("length check for input string values: with implicit cast") {
    withTable("t") {
      sql(s"CREATE TABLE t(c1 CHAR(5), c2 VARCHAR(5)) USING $format")
      sql("INSERT INTO t VALUES (1234, 1234)")
      checkAnswer(spark.table("t"), Row("1234 ", "1234"))
      assertLengthCheckFailure("INSERT INTO t VALUES (123456, 1)")
      assertLengthCheckFailure("INSERT INTO t VALUES (1, 123456)")
    }
  }

  private def testConditions(df: DataFrame, conditions: Seq[(String, Boolean)]): Unit = {
    checkAnswer(df.selectExpr(conditions.map(_._1): _*), Row.fromSeq(conditions.map(_._2)))
  }

  test("char type comparison: top-level columns") {
    withTable("t") {
      sql(s"CREATE TABLE t(c1 CHAR(2), c2 CHAR(5)) USING $format")
      sql("INSERT INTO t VALUES ('a', 'a')")
      testConditions(spark.table("t"), Seq(
        ("c1 = 'a'", true),
        ("'a' = c1", true),
        ("c1 = 'a  '", true),
        ("c1 > 'a'", false),
        ("c1 IN ('a', 'b')", true),
        ("c1 = c2", true),
        ("c1 < c2", false),
        ("c1 IN (c2)", true),
        ("c1 <=> null", false)))
    }
  }

  test("char type comparison: partitioned columns") {
    withTable("t") {
      sql(s"CREATE TABLE t(i INT, c1 CHAR(2), c2 CHAR(5)) USING $format PARTITIONED BY (c1, c2)")
      sql("INSERT INTO t VALUES (1, 'a', 'a')")
      testConditions(spark.table("t"), Seq(
        ("c1 = 'a'", true),
        ("'a' = c1", true),
        ("c1 = 'a  '", true),
        ("c1 > 'a'", false),
        ("c1 IN ('a', 'b')", true),
        ("c1 = c2", true),
        ("c1 < c2", false),
        ("c1 IN (c2)", true),
        ("c1 <=> null", false)))
    }
  }

  private def testNullConditions(df: DataFrame, conditions: Seq[String]): Unit = {
    conditions.foreach { cond =>
      checkAnswer(df.selectExpr(cond), Row(null))
    }
  }

  test("SPARK-34233: char type comparison with null values") {
    val conditions = Seq("c = null", "c IN ('e', null)", "c IN (null)")
    withTable("t") {
      sql(s"CREATE TABLE t(c CHAR(2)) USING $format")
      sql("INSERT INTO t VALUES ('a')")
      testNullConditions(spark.table("t"), conditions)
    }

    withTable("t") {
      sql(s"CREATE TABLE t(i INT, c CHAR(2)) USING $format PARTITIONED BY (c)")
      sql("INSERT INTO t VALUES (1, 'a')")
      testNullConditions(spark.table("t"), conditions)
    }
  }

  test("char type comparison: partition pruning") {
    withTable("t") {
      sql(s"CREATE TABLE t(i INT, c1 CHAR(2), c2 VARCHAR(5)) USING $format PARTITIONED BY (c1, c2)")
      sql("INSERT INTO t VALUES (1, 'a', 'a')")
      Seq(("c1 = 'a'", true),
        ("'a' = c1", true),
        ("c1 = 'a  '", true),
        ("c1 > 'a'", false),
        ("c1 IN ('a', 'b')", true),
        ("c2 = 'a  '", false),
        ("c2 = 'a'", true),
        ("c2 IN ('a', 'b')", true)).foreach { case (con, res) =>
        val df = spark.table("t")
        withClue(con) {
          checkAnswer(df.where(con), df.where(res.toString))
        }
      }
    }
  }

  test("char type comparison: join") {
    withTable("t1", "t2") {
      sql(s"CREATE TABLE t1(c CHAR(2)) USING $format")
      sql(s"CREATE TABLE t2(c CHAR(5)) USING $format")
      sql("INSERT INTO t1 VALUES ('a')")
      sql("INSERT INTO t2 VALUES ('a')")
      checkAnswer(sql("SELECT t1.c FROM t1 JOIN t2 ON t1.c = t2.c"), Row("a "))
    }
  }

  test("char type comparison: nested in struct") {
    withTable("t") {
      sql(s"CREATE TABLE t(c1 STRUCT<c: CHAR(2)>, c2 STRUCT<c: CHAR(5)>) USING $format")
      sql("INSERT INTO t VALUES (struct('a'), struct('a'))")
      testConditions(spark.table("t"), Seq(
        ("c1 = c2", true),
        ("c1 < c2", false),
        ("c1 IN (c2)", true)))
    }
  }

  test("char type comparison: nested in array") {
    withTable("t") {
      sql(s"CREATE TABLE t(c1 ARRAY<CHAR(2)>, c2 ARRAY<CHAR(5)>) USING $format")
      sql("INSERT INTO t VALUES (array('a', 'b'), array('a', 'b'))")
      testConditions(spark.table("t"), Seq(
        ("c1 = c2", true),
        ("c1 < c2", false),
        ("c1 IN (c2)", true)))
    }
  }

  test("char type comparison: nested in struct of array") {
    withTable("t") {
      sql("CREATE TABLE t(c1 STRUCT<a: ARRAY<CHAR(2)>>, c2 STRUCT<a: ARRAY<CHAR(5)>>) " +
        s"USING $format")
      sql("INSERT INTO t VALUES (struct(array('a', 'b')), struct(array('a', 'b')))")
      testConditions(spark.table("t"), Seq(
        ("c1 = c2", true),
        ("c1 < c2", false),
        ("c1 IN (c2)", true)))
    }
  }

  test("char type comparison: nested in array of struct") {
    withTable("t") {
      sql("CREATE TABLE t(c1 ARRAY<STRUCT<c: CHAR(2)>>, c2 ARRAY<STRUCT<c: CHAR(5)>>) " +
        s"USING $format")
      sql("INSERT INTO t VALUES (array(struct('a')), array(struct('a')))")
      testConditions(spark.table("t"), Seq(
        ("c1 = c2", true),
        ("c1 < c2", false),
        ("c1 IN (c2)", true)))
    }
  }

  test("char type comparison: nested in array of array") {
    withTable("t") {
      sql("CREATE TABLE t(c1 ARRAY<ARRAY<CHAR(2)>>, c2 ARRAY<ARRAY<CHAR(5)>>) " +
        s"USING $format")
      sql("INSERT INTO t VALUES (array(array('a')), array(array('a')))")
      testConditions(spark.table("t"), Seq(
        ("c1 = c2", true),
        ("c1 < c2", false),
        ("c1 IN (c2)", true)))
    }
  }

  test("SPARK-33892: DESCRIBE TABLE w/ char/varchar") {
    withTable("t") {
      sql(s"CREATE TABLE t(v VARCHAR(3), c CHAR(5)) USING $format")
      checkAnswer(sql("desc t").selectExpr("data_type").where("data_type like '%char%'"),
        Seq(Row("char(5)"), Row("varchar(3)")))
    }
  }

  test("SPARK-34003: fix char/varchar fails w/ both group by and order by ") {
    withTable("t") {
      sql(s"CREATE TABLE t(v VARCHAR(3), i INT) USING $format")
      sql("INSERT INTO t VALUES ('c', 1)")
      checkAnswer(sql("SELECT v, sum(i) FROM t GROUP BY v ORDER BY v"), Row("c", 1))
    }
  }

  test("SPARK-34003: fix char/varchar fails w/ order by functions") {
    withTable("t") {
      sql(s"CREATE TABLE t(v VARCHAR(3), i INT) USING $format")
      sql("INSERT INTO t VALUES ('c', 1)")
      checkAnswer(sql("SELECT substr(v, 1, 2), sum(i) FROM t GROUP BY v ORDER BY substr(v, 1, 2)"),
        Row("c", 1))
      checkAnswer(sql("SELECT sum(i) FROM t GROUP BY v ORDER BY substr(v, 1, 2)"),
        Row(1))
    }
  }

  test("SPARK-34114: varchar type will strip tailing spaces to certain length at write time") {
    withTable("t") {
      sql(s"CREATE TABLE t(v VARCHAR(3)) USING $format")
      sql("INSERT INTO t VALUES ('c      ')")
      checkAnswer(spark.table("t"), Row("c  "))
    }
  }

  test("SPARK-34114: varchar type will remain the value length with spaces at read time") {
    withTable("t") {
      sql(s"CREATE TABLE t(v VARCHAR(3)) USING $format")
      sql("INSERT INTO t VALUES ('c ')")
      checkAnswer(spark.table("t"), Row("c "))
    }
  }

  test("SPARK-34833: right-padding applied correctly for correlated subqueries - join keys") {
    withTable("t1", "t2") {
      sql(s"CREATE TABLE t1(v VARCHAR(3), c CHAR(5)) USING $format")
      sql(s"CREATE TABLE t2(v VARCHAR(5), c CHAR(8)) USING $format")
      sql("INSERT INTO t1 VALUES ('c', 'b')")
      sql("INSERT INTO t2 VALUES ('a', 'b')")
      Seq("t1.c = t2.c", "t2.c = t1.c",
        "t1.c = 'b'", "'b' = t1.c", "t1.c = 'b    '", "'b    ' = t1.c",
        "t1.c = 'b      '", "'b      ' = t1.c").foreach { predicate =>
        checkAnswer(sql(
          s"""
             |SELECT v FROM t1
             |WHERE 'a' IN (SELECT v FROM t2 WHERE $predicate)
           """.stripMargin),
          Row("c"))
      }
    }
  }

  test("SPARK-34833: right-padding applied correctly for correlated subqueries - other preds") {
    withTable("t") {
      sql(s"CREATE TABLE t(c0 INT, c1 CHAR(5), c2 CHAR(7)) USING $format")
      sql("INSERT INTO t VALUES (1, 'abc', 'abc')")
      Seq("c1 = 'abc'", "'abc' = c1", "c1 = 'abc  '", "'abc  ' = c1",
        "c1 = 'abc    '", "'abc    ' = c1", "c1 = c2", "c2 = c1",
        "c1 IN ('xxx', 'abc', 'xxxxx')", "c1 IN ('xxx', 'abc  ', 'xxxxx')",
        "c1 IN ('xxx', 'abc    ', 'xxxxx')",
        "c1 IN (c2)", "c2 IN (c1)").foreach { predicate =>
        checkAnswer(sql(
          s"""
             |SELECT c0 FROM t t1
             |WHERE (
             |  SELECT count(*) AS c
             |  FROM t
             |  WHERE c0 = t1.c0 AND $predicate
             |) > 0
         """.stripMargin),
          Row(1))
      }
    }
  }

  test("SPARK-35359: create table and insert data over length values") {
    Seq("char", "varchar").foreach { typ =>
      withSQLConf((SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING.key, "true")) {
        withTable("t") {
          sql(s"CREATE TABLE t (col $typ(2)) using $format")
          sql("INSERT INTO t SELECT 'aaa'")
          checkAnswer(sql("select * from t"), Row("aaa"))
        }
      }
    }
  }
}

// Some basic char/varchar tests which doesn't rely on table implementation.
class BasicCharVarcharTestSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("user-specified schema in cast") {
    def assertNoCharType(df: DataFrame): Unit = {
      checkAnswer(df, Row("0"))
      assert(df.schema.map(_.dataType) == Seq(StringType))
    }

    val logAppender = new LogAppender("The Spark cast operator does not support char/varchar" +
      " type and simply treats them as string type. Please use string type directly to avoid" +
      " confusion.")
    withLogAppender(logAppender) {
      assertNoCharType(spark.range(1).select($"id".cast("char(5)")))
      assertNoCharType(spark.range(1).select($"id".cast(CharType(5))))
      assertNoCharType(spark.range(1).selectExpr("CAST(id AS CHAR(5))"))
      assertNoCharType(sql("SELECT CAST(id AS CHAR(5)) FROM range(1)"))
    }
  }

  test("invalidate char/varchar in functions") {
    checkError(
      exception = intercept[AnalysisException] {
        sql("""SELECT from_json('{"a": "str"}', 'a CHAR(5)')""")
      },
      errorClass = "UNSUPPORTED_CHAR_OR_VARCHAR_AS_STRING",
      parameters = Map.empty,
      context = ExpectedContext(
        fragment = "from_json('{\"a\": \"str\"}', 'a CHAR(5)')",
        start = 7,
        stop = 44)
    )
    withSQLConf((SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING.key, "true")) {
      val df = sql("""SELECT from_json('{"a": "str"}', 'a CHAR(5)')""")
      checkAnswer(df, Row(Row("str")))
      val schema = df.schema.head.dataType.asInstanceOf[StructType]
      assert(schema.map(_.dataType) == Seq(StringType))
    }
  }

  test("invalidate char/varchar in SparkSession createDataframe") {
    val df = spark.range(10).map(_.toString).toDF()
    val schema = new StructType().add("id", CharType(5))
    checkError(
      exception = intercept[AnalysisException] {
        spark.createDataFrame(df.collectAsList(), schema)
      },
      errorClass = "UNSUPPORTED_CHAR_OR_VARCHAR_AS_STRING"
    )
    checkError(
      exception = intercept[AnalysisException] {
        spark.createDataFrame(df.rdd, schema)
      },
      errorClass = "UNSUPPORTED_CHAR_OR_VARCHAR_AS_STRING"
    )
    checkError(
      exception = intercept[AnalysisException] {
        spark.createDataFrame(df.toJavaRDD, schema)
      },
      errorClass = "UNSUPPORTED_CHAR_OR_VARCHAR_AS_STRING"
    )
    withSQLConf((SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING.key, "true")) {
      val df1 = spark.createDataFrame(df.collectAsList(), schema)
      checkAnswer(df1, df)
      assert(df1.schema.head.dataType === StringType)
    }
  }

  test("invalidate char/varchar in spark.read.schema") {
    checkError(
      exception = intercept[AnalysisException] {
        spark.read.schema(new StructType().add("id", CharType(5)))
      },
      errorClass = "UNSUPPORTED_CHAR_OR_VARCHAR_AS_STRING")
    checkError(
      exception = intercept[AnalysisException] {
        spark.read.schema("id char(5)")
      },
      errorClass = "UNSUPPORTED_CHAR_OR_VARCHAR_AS_STRING"
    )
    withSQLConf((SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING.key, "true")) {
      val ds = spark.range(10).map(_.toString)
      val df1 = spark.read.schema(new StructType().add("id", CharType(5))).csv(ds)
      assert(df1.schema.map(_.dataType) == Seq(StringType))
      val df2 = spark.read.schema("id char(5)").csv(ds)
      assert(df2.schema.map(_.dataType) == Seq(StringType))

      def checkSchema(df: DataFrame): Unit = {
        val schemas = df.queryExecution.analyzed.collect {
          case l: LogicalRelation => l.relation.schema
          case d: DataSourceV2Relation => d.table.schema()
        }
        assert(schemas.length == 1)
        assert(schemas.head.map(_.dataType) == Seq(StringType))
      }

      // user-specified schema in DataFrameReader: DSV1
      checkSchema(spark.read.schema(new StructType().add("id", CharType(5)))
        .format(classOf[SimpleInsertSource].getName).load())
      checkSchema(spark.read.schema("id char(5)")
        .format(classOf[SimpleInsertSource].getName).load())

      // user-specified schema in DataFrameReader: DSV2
      checkSchema(spark.read.schema(new StructType().add("id", CharType(5)))
        .format(classOf[SchemaRequiredDataSource].getName).load())
      checkSchema(spark.read.schema("id char(5)")
        .format(classOf[SchemaRequiredDataSource].getName).load())
    }
  }

  test("invalidate char/varchar in udf's result type") {
    checkError(
      exception = intercept[AnalysisException] {
        spark.udf.register("testchar", () => "B", VarcharType(1))
      },
      errorClass = "UNSUPPORTED_CHAR_OR_VARCHAR_AS_STRING"
    )
    checkError(
      exception = intercept[AnalysisException] {
        spark.udf.register("testchar2", (x: String) => x, VarcharType(1))
      },
      errorClass = "UNSUPPORTED_CHAR_OR_VARCHAR_AS_STRING"
    )
    withSQLConf((SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING.key, "true")) {
      spark.udf.register("testchar", () => "B", VarcharType(1))
      spark.udf.register("testchar2", (x: String) => x, VarcharType(1))
      val df1 = spark.sql("select testchar()")
      checkAnswer(df1, Row("B"))
      assert(df1.schema.head.dataType === StringType)
      val df2 = spark.sql("select testchar2('abc')")
      checkAnswer(df2, Row("abc"))
      assert(df2.schema.head.dataType === StringType)
    }
  }

  test("invalidate char/varchar in spark.readStream.schema") {
    checkError(
      exception = intercept[AnalysisException] {
        spark.readStream.schema(new StructType().add("id", CharType(5)))
      },
      errorClass = "UNSUPPORTED_CHAR_OR_VARCHAR_AS_STRING"
    )
    checkError(
      exception = intercept[AnalysisException] {
        spark.readStream.schema("id char(5)")
      },
      errorClass = "UNSUPPORTED_CHAR_OR_VARCHAR_AS_STRING"
    )
    withSQLConf((SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING.key, "true")) {
      withTempPath { dir =>
        spark.range(2).write.save(dir.toString)
        val df1 = spark.readStream.schema(new StructType().add("id", CharType(5)))
          .load(dir.toString)
        assert(df1.schema.map(_.dataType) == Seq(StringType))
        val df2 = spark.readStream.schema("id char(5)").load(dir.toString)
        assert(df2.schema.map(_.dataType) == Seq(StringType))
      }
    }
  }

  test("SPARK-44409: Handle char/varchar in Dataset.to to keep consistent with others") {
    val newSchema = StructType.fromDDL("v varchar(255), c char(10)")
    withTable("t") {
      sql("CREATE TABLE t(c char(10), v varchar(255)) USING parquet")
      sql("INSERT INTO t VALUES('spark', 'awesome')")
      val df = sql("SELECT * FROM t")
      checkError(exception = intercept[AnalysisException] {
        df.to(newSchema)
      }, errorClass = "UNSUPPORTED_CHAR_OR_VARCHAR_AS_STRING", parameters = Map.empty)
      withSQLConf((SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING.key, "true")) {
        val df1 = df.to(newSchema)
        checkAnswer(df1, df.select("v", "c"))
        assert(df1.schema.last.dataType === StringType)
      }
    }
  }
}

class FileSourceCharVarcharTestSuite extends CharVarcharTestSuite with SharedSparkSession {
  override def format: String = "parquet"
  override protected def sparkConf: SparkConf = {
    super.sparkConf.set(SQLConf.USE_V1_SOURCE_LIST, "parquet")
  }

  test("create table w/ location and fit length values") {
    withTempPath { dir =>
      withTable("t") {
        sql("SELECT '12' as col1, '12' as col2").write.format(format).save(dir.toString)
        sql(s"CREATE TABLE t (col1 char(3), col2 varchar(3)) using $format LOCATION '$dir'")
        checkAnswer(sql("select * from t"), Row("12 ", "12"))
      }
    }
  }

  test("create table w/ location and over length values") {
    Seq("char", "varchar").foreach { typ =>
      withTempPath { dir =>
        withTable("t") {
          sql("SELECT '123456' as col").write.format(format).save(dir.toString)
          sql(s"CREATE TABLE t (col $typ(2)) using $format LOCATION '$dir'")
          checkAnswer(sql("select * from t"), Row("123456"))
        }
      }
    }
  }

  test("alter table set location w/ fit length values") {
    withTempPath { dir =>
      withTable("t") {
        sql("SELECT '12' as col1, '12' as col2").write.format(format).save(dir.toString)
        sql(s"CREATE TABLE t (col1 char(3), col2 varchar(3)) using $format")
        sql(s"ALTER TABLE t SET LOCATION '$dir'")
        checkAnswer(spark.table("t"), Row("12 ", "12"))
      }
    }
  }

  test("alter table set location w/ over length values") {
    Seq("char", "varchar").foreach { typ =>
      withTempPath { dir =>
        withTable("t") {
          sql("SELECT '123456' as col").write.format(format).save(dir.toString)
          sql(s"CREATE TABLE t (col $typ(2)) using $format")
          sql(s"ALTER TABLE t SET LOCATION '$dir'")
          checkAnswer(spark.table("t"), Row("123456"))
        }
      }
    }
  }

  test("SPARK-34114: should not trim right for read-side length check and char padding") {
    Seq("char", "varchar").foreach { typ =>
      withTempPath { dir =>
        withTable("t") {
          sql("SELECT '12  ' as col").write.format(format).save(dir.toString)
          sql(s"CREATE TABLE t (col $typ(2)) using $format LOCATION '$dir'")
          checkAnswer(spark.table("t"), Row("12  "))
        }
      }
    }
  }

  test("SPARK-40697: read-side char padding should only be applied if necessary") {
    withTable("t") {
      sql(
        s"""
          |CREATE TABLE t (
          |  c1 CHAR(5),
          |  c2 STRUCT<i VARCHAR(5)>,
          |  c3 ARRAY<VARCHAR(5)>,
          |  c4 MAP<INT, VARCHAR(5)>
          |) USING $format
          |""".stripMargin)
      spark.read.table("t").queryExecution.analyzed.foreach {
        case Project(projectList, _) =>
          assert(projectList.length == 4)
          assert(projectList.drop(1).forall(_.isInstanceOf[Attribute]))
        case _ =>
      }
    }
  }

  test("char/varchar type values length check: partitioned columns of other types") {
    val tableName = "t"
    Seq("CHAR(5)", "VARCHAR(5)").foreach { typ =>
      withTable(tableName) {
        sql(s"CREATE TABLE $tableName(i STRING, c $typ) USING $format PARTITIONED BY (c)")
        Seq(1, 10, 100, 1000, 10000).foreach { v =>
          sql(s"INSERT OVERWRITE $tableName VALUES ('1', $v)")
          checkPlainResult(spark.table(tableName), typ, v.toString)
          sql(s"ALTER TABLE $tableName DROP PARTITION(c=$v)")
          checkAnswer(spark.table(tableName), Nil)
        }
        assertLengthCheckFailure(s"INSERT OVERWRITE $tableName VALUES ('1', 100000)")
        assertLengthCheckFailure("ALTER TABLE t DROP PARTITION(c=100000)")
      }
    }
  }
}

class DSV2CharVarcharTestSuite extends CharVarcharTestSuite
  with SharedSparkSession {
  override def format: String = "foo"
  protected override def sparkConf = {
    super.sparkConf
      .set("spark.sql.catalog.testcat", classOf[InMemoryPartitionTableCatalog].getName)
      .set(SQLConf.DEFAULT_CATALOG.key, "testcat")
  }

  test("char/varchar type values length check: partitioned columns of other types") {
    Seq("CHAR(5)", "VARCHAR(5)").foreach { typ =>
      withTable("t") {
        sql(s"CREATE TABLE t(i STRING, c $typ) USING $format PARTITIONED BY (c)")
        Seq(1, 10, 100, 1000, 10000).foreach { v =>
          sql(s"INSERT OVERWRITE t VALUES ('1', $v)")
          checkPlainResult(spark.table("t"), typ, v.toString)
          sql(s"ALTER TABLE t DROP PARTITION(c=$v)")
          checkAnswer(spark.table("t"), Nil)
        }
        assertLengthCheckFailure(s"INSERT OVERWRITE t VALUES ('1', 100000)")
        assertLengthCheckFailure("ALTER TABLE t DROP PARTITION(c=100000)")
      }
    }
  }

  test("SPARK-42611: check char/varchar length in reordered nested structs") {
    Seq("CHAR(5)", "VARCHAR(5)").foreach { typ =>
      withTable("t") {
        sql(s"CREATE TABLE t(s STRUCT<n_c: $typ, n_i: INT>) USING $format")
        val inputDF = sql("SELECT named_struct('n_i', 1, 'n_c', '123456') AS s")
        assertLengthCheckFailure(() => inputDF.writeTo("t").append())
      }
    }
  }

  test("SPARK-42611: check char/varchar length in reordered structs within arrays") {
    Seq("CHAR(5)", "VARCHAR(5)").foreach { typ =>
      withTable("t") {
        sql(s"CREATE TABLE t(a ARRAY<STRUCT<n_c: $typ, n_i: INT>>) USING $format")
        val inputDF = sql("SELECT array(named_struct('n_i', 1, 'n_c', '123456')) AS a")
        assertLengthCheckFailure(() => inputDF.writeTo("t").append())
      }
    }
  }

  test("SPARK-42611: check char/varchar length in reordered structs within map keys") {
    Seq("CHAR(5)", "VARCHAR(5)").foreach { typ =>
      withTable("t") {
        sql(s"CREATE TABLE t(m MAP<STRUCT<n_c: $typ, n_i: INT>, INT>) USING $format")
        val inputDF = sql("SELECT map(named_struct('n_i', 1, 'n_c', '123456'), 1) AS m")
        assertLengthCheckFailure(() => inputDF.writeTo("t").append())
      }
    }
  }

  test("SPARK-42611: check char/varchar length in reordered structs within map values") {
    Seq("CHAR(5)", "VARCHAR(5)").foreach { typ =>
      withTable("t") {
        sql(s"CREATE TABLE t(m MAP<INT, STRUCT<n_c: $typ, n_i: INT>>) USING $format")
        val inputDF = sql("SELECT map(1, named_struct('n_i', 1, 'n_c', '123456')) AS m")
        assertLengthCheckFailure(() => inputDF.writeTo("t").append())
      }
    }
  }
}
