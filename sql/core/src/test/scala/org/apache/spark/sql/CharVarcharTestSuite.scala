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

import scala.util.Try

import org.apache.spark.{SparkConf, SparkException, SparkRuntimeException}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.{Attribute, EqualTo, GreaterThan, Literal, ScalarSubquery, StringRPad}
import org.apache.spark.sql.catalyst.expressions.Cast.toSQLId
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, ParseException}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Project}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.SchemaRequiredDataSource
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, InMemoryPartitionTableCatalog}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.functions
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.SimpleInsertSource
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

// The base trait for char/varchar tests that need to be run with different table implementations.
trait CharVarcharTestSuite extends QueryTest {

  def format: String

  def checkColType(f: StructField, dt: DataType): Unit = {
    assert(f.dataType == CharVarcharUtils.replaceCharVarcharWithString(dt))
    assert(CharVarcharUtils.getRawType(f.metadata) == Some(dt))
  }

  def checkPlainResult(df: DataFrame, dt: String, insertVal: String): Unit = {
    val dataType = CatalystSqlParser.parseDataType(dt)
    checkColType(df.schema(1), dataType)
    dataType match {
      case c: CharType =>
        // char value will be padded if (<= len) or trimmed if (> len)
        val fixLenStr = if (insertVal != null) {
          insertVal.take(c.length).padTo(c.length, " ").mkString
        } else null
        checkAnswer(df, Row("1", fixLenStr))
      case v: VarcharType =>
        // varchar value will be remained if (<= len) or trimmed if (> len)
        val varLenStrWithUpperBound = if (insertVal != null) {
          insertVal.take(v.length)
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
      condition = "EXCEED_LIMIT_LENGTH",
      parameters = Map("limit" -> "5")
    )
  }

  test("apply char padding/trimming and varchar trimming: top-level columns") {
    Seq("CHAR(5)", "VARCHAR(5)").foreach { typ =>
      withTable("t") {
        sql(s"CREATE TABLE t(i STRING, c $typ) USING $format")
        (0 to 5).map(n => "a" + " ".repeat(n)).foreach { v =>
          sql(s"INSERT OVERWRITE t VALUES ('1', '$v')")
          checkPlainResult(spark.table("t"), typ, v)
        }
        sql("INSERT OVERWRITE t VALUES ('1', null)")
        checkPlainResult(spark.table("t"), typ, null)
      }
    }
  }

  test("preserve char/varchar type info") {
    Seq(CharType(5), VarcharType(5)).foreach { typ =>
      for {
        char_varchar_as_string <- Seq(false, true)
        preserve_char_varchar <- Seq(false, true)
      } {
        withSQLConf(SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING.key -> char_varchar_as_string.toString,
          SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> preserve_char_varchar.toString) {
          withTable("t") {
            val name = typ.typeName
            sql(s"CREATE TABLE t(i STRING, c $name) USING $format")
            val schema = spark.table("t").schema
            assert(schema.fields(0).dataType == StringType)
            val expectedType = if (preserve_char_varchar) typ else StringType
            assert(schema.fields(1).dataType == expectedType)
          }
        }
      }
    }
  }

  test("char type values should be padded or trimmed: partitioned columns") {
    // via dynamic partitioned columns
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c CHAR(5)) USING $format PARTITIONED BY (c)")
      (0 to 5).map(n => "a" + " ".repeat(n)).foreach { v =>
        sql(s"INSERT OVERWRITE t VALUES ('1', '$v')")
        checkPlainResult(spark.table("t"), "CHAR(5)", v)
      }
    }

    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c CHAR(5)) USING $format PARTITIONED BY (c)")
      (0 to 5).map(n => "a" + " ".repeat(n)).foreach { v =>
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
        val v = "a" + " ".repeat(n)
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
      checkAnswer(spark.table("t"), Row("1", Row("a" + " ".repeat(4))))
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
      checkAnswer(spark.table("t"), Row("1", Seq("a" + " ".repeat(4), "ab" + " ".repeat(3))))
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
      checkAnswer(spark.table("t"), Row("1", Map(("a" + " ".repeat(4), "ab"))))
      checkColType(spark.table("t").schema(1), MapType(CharType(5), StringType))

      sql("INSERT OVERWRITE t VALUES ('1', null)")
      checkAnswer(spark.table("t"), Row("1", null))
    }
  }

  test("char type values should be padded: nested in map value") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c MAP<STRING, CHAR(5)>) USING $format")
      sql("INSERT INTO t VALUES ('1', map('a', 'ab'))")
      checkAnswer(spark.table("t"), Row("1", Map(("a", "ab" + " ".repeat(3)))))
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
      checkAnswer(spark.table("t"), Row("1", Map(("a" + " ".repeat(4), "ab" + " ".repeat(8)))))
      checkColType(spark.table("t").schema(1), MapType(CharType(5), CharType(10)))

      sql("INSERT OVERWRITE t VALUES ('1', null)")
      checkAnswer(spark.table("t"), Row("1", null))
    }
  }

  test("char type values should be padded: nested in struct of array") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c STRUCT<c: ARRAY<CHAR(5)>>) USING $format")
      sql("INSERT INTO t VALUES ('1', struct(array('a', 'ab')))")
      checkAnswer(spark.table("t"), Row("1", Row(Seq("a" + " ".repeat(4), "ab" + " ".repeat(3)))))
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
      checkAnswer(spark.table("t"),
        Row("1", Seq(Row("a" + " ".repeat(4)), Row("ab" + " ".repeat(3)))))
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
      checkAnswer(spark.table("t"), Row("1", Seq(Seq("a" + " ".repeat(4), "ab" + " ".repeat(3)))))
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
        Row("12" + " ".repeat(3), "12 "),
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

  test("SPARK-48792: Fix INSERT with partial column list to a table with char/varchar") {
    assume(format != "foo",
      "TODO: TableOutputResolver.resolveOutputColumns supportColDefaultValue is false")
    Seq("char", "varchar").foreach { typ =>
      withTable("students") {
        sql(s"CREATE TABLE students (name $typ(64), address $typ(64)) USING $format")
        sql("INSERT INTO students VALUES ('Kent Yao', 'Hangzhou')")
        sql("INSERT INTO students (address) VALUES ('<unknown>')")
        checkAnswer(sql("SELECT count(*) FROM students"), Row(2))
      }
    }
  }

  test(s"insert string literal into char/varchar column when " +
    s"${SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key} is true") {
    withSQLConf(SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> "true") {
      withTable("t") {
        sql(s"CREATE TABLE t(c1 CHAR(5), c2 VARCHAR(5)) USING $format")
        sql("INSERT INTO t VALUES ('1234', '1234')")
        checkAnswer(spark.table("t"), Row("1234 ", "1234"))
        assertLengthCheckFailure("INSERT INTO t VALUES ('123456', '1')")
        assertLengthCheckFailure("INSERT INTO t VALUES ('1', '123456')")
      }
    }
  }

  test(s"insert from string column into char/varchar column when " +
    s"${SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key} is true") {
    withSQLConf(SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> "true") {
      withTable("a", "b") {
        sql(s"CREATE TABLE a AS SELECT '1234' as c1, '1234' as c2")
        sql(s"CREATE TABLE b(c1 CHAR(5), c2 VARCHAR(5)) USING $format")
        sql("INSERT INTO b SELECT * FROM a")
        checkAnswer(spark.table("b"), Row("1234 ", "1234"))
        spark.table("b").show()
      }
    }
  }

  test(s"cast from char/varchar when ${SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key} is true") {
    withSQLConf(SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> "true") {
      Seq("char(5)", "varchar(5)").foreach { typ =>
        Seq(
          "int" -> ("123", 123),
          "long" -> ("123 ", 123L),
          "boolean" -> ("true ", true),
          "boolean" -> ("false", false),
          "double" -> ("1.2", 1.2)
        ).foreach { case (toType, (from, to)) =>
          assert(sql(s"select cast($from :: $typ as $toType)").collect() === Array(Row(to)))
        }
      }
    }
  }

  test(s"cast to char/varchar when ${SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key} is true") {
    withSQLConf(SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> "true") {
      Seq("char(10)", "varchar(10)").foreach { typ =>
        Seq(
          123 -> "123",
          123L-> "123",
          true -> "true",
          false -> "false",
          1.2 -> "1.2"
        ).foreach { case (from, to) =>
          val paddedTo = if (typ == "char(10)") {
            to.padTo(10, ' ')
          } else {
            to
          }
          sql(s"select cast($from as $typ)").collect() === Array(Row(paddedTo))
        }
      }
    }
  }

  test("implicitly cast char/varchar into atomics") {
    Seq("char", "varchar").foreach { typ =>
      withSQLConf(SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> "true",
        SQLConf.ANSI_ENABLED.key -> "true") {
        checkAnswer(sql(
          s"""
             |SELECT
             |NOT('false'::$typ(5)),
             |1 + ('4'::$typ(5)),
             |2L + ('4'::$typ(5)),
             |3S + ('4'::$typ(5)),
             |4Y - ('4'::$typ(5)),
             |1.2 / ('0.6'::$typ(5)),
             |MINUTE('2009-07-30 12:58:59'::$typ(30)),
             |if(true, '0'::$typ(5), 1),
             |if(false, '0'::$typ(5), 1)
          """.stripMargin), Row(true, 5, 6, 7, 0, 2.0, 58, 0, 1))
      }
    }
  }

  test("SPARK-50847: Deny ApplyCharTypePadding from applying on specific In expressions") {
    withTable("mytable") {
      sql(s"CREATE TABLE mytable(col CHAR(10)) USING $format")
      checkError(
        exception = intercept[AnalysisException] {
          sql("SELECT * FROM mytable where col IN (ARRAY('a'))")
        },
        condition = "DATATYPE_MISMATCH.DATA_DIFF_TYPES",
        parameters = Map(
          "functionName" -> toSQLId("in"),
          "dataType" -> "[\"STRING\", \"ARRAY<STRING>\"]",
          "sqlExpr" -> s""""(col IN (array(a)))""""
        ),
        queryContext = Array(ExpectedContext(fragment = "IN (ARRAY('a'))", start = 32, stop = 46))
      )
    }
  }

  test(
    "SPARK-51732: rpad should be applied on attributes with same ExprId if those attributes " +
      "should be deduplicated 2"
  ) {
    withSQLConf(
      SQLConf.READ_SIDE_CHAR_PADDING.key -> "false",
      SQLConf.LEGACY_NO_CHAR_PADDING_IN_PREDICATE.key -> "false"
    ) {
      withTable("mytable") {
        sql(s"CREATE TABLE mytable(col CHAR(10))")
        val plan = sql(
          """
            |SELECT t1.col
            |FROM mytable t1
            |WHERE (SELECT count(*) AS cnt FROM mytable t2 WHERE (t1.col = t2.col)) > 0
          """.stripMargin).queryExecution.analyzed
        val subquery = plan.asInstanceOf[Project]
          .child.asInstanceOf[Filter]
          .condition.asInstanceOf[GreaterThan]
          .left.asInstanceOf[ScalarSubquery]
        val subqueryFilterCondition = subquery.plan.asInstanceOf[Aggregate]
          .child.asInstanceOf[Filter]
          .condition.asInstanceOf[EqualTo]

        // rpad should  be applied to both left and right hand side of t1.col = t2.col because the
        // attributes are deduplicated.
        assert(subqueryFilterCondition.left.isInstanceOf[StringRPad])
        assert(subqueryFilterCondition.right.isInstanceOf[StringRPad])
      }
    }
  }
}

// Some basic char/varchar tests which doesn't rely on table implementation.
class BasicCharVarcharTestSuite extends SharedSparkSession {
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

  test("SPARK-58797: CAST to CHAR/VARCHAR with standardSemantics") {
    withSQLConf(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true") {
      val charDf = sql("SELECT CAST('ab' AS CHAR(5)) AS c")
      assert(charDf.schema.head.dataType === CharType(5))
      checkAnswer(charDf, Row("ab   "))

      val varcharDf = sql("SELECT CAST('hello' AS VARCHAR(5)) AS v")
      assert(varcharDf.schema.head.dataType === VarcharType(5))
      checkAnswer(varcharDf, Row("hello"))

      // ISO 6.13: character-to-character CAST truncates rather than erroring.
      checkAnswer(sql("SELECT CAST('hello!' AS VARCHAR(5)) AS v"), Row("hello"))
      checkAnswer(sql("SELECT CAST('abcdef' AS CHAR(2)) AS c"), Row("ab"))
      checkAnswer(sql("SELECT CAST('abcdef' AS VARCHAR(2)) AS v"), Row("ab"))
      checkAnswer(sql("SELECT try_cast('abcdef' AS CHAR(2)) AS c"), Row("ab"))
      checkAnswer(sql("SELECT try_cast('abcdef' AS VARCHAR(2)) AS v"), Row("ab"))

      // Multi-byte characters: length is in characters, not octets.
      // scalastyle:off nonascii
      checkAnswer(sql("SELECT CAST('你好' AS VARCHAR(2)) AS v"), Row("你好"))
      checkAnswer(sql("SELECT CAST('你好啊' AS VARCHAR(2)) AS v"), Row("你好"))
      // scalastyle:on nonascii

      // ISO 6.13 numeric-to-character CAST still errors when the literal does not fit.
      checkError(
        exception = intercept[SparkRuntimeException] {
          sql("SELECT CAST(12345 AS VARCHAR(4))").collect()
        },
        condition = "EXCEED_LIMIT_LENGTH",
        parameters = Map("limit" -> "4")
      )
      checkAnswer(sql("SELECT CAST(12345 AS VARCHAR(5)) AS v"), Row("12345"))
      checkAnswer(sql("SELECT try_cast(12345 AS VARCHAR(4)) AS v"), Row(null))

      // LCT must wrap the inner CAST, not retarget it (truncation / overflow stay).
      checkAnswer(
        sql("SELECT coalesce(CAST('abcdef' AS VARCHAR(2)), CAST('x' AS VARCHAR(4))) AS c"),
        Row("ab"))
      checkAnswer(
        sql("""SELECT CASE WHEN true THEN CAST('abcdef' AS VARCHAR(2))
          |ELSE CAST('x' AS VARCHAR(4)) END AS c""".stripMargin),
        Row("ab"))
      checkAnswer(
        sql("SELECT CAST('abcdef' AS VARCHAR(2)) IN (CAST('ab' AS VARCHAR(4)))"),
        Row(true))
      checkAnswer(
        sql("""SELECT coalesce(
          |  CAST('abcdef' AS VARCHAR(2) COLLATE UTF8_LCASE),
          |  CAST('x' AS VARCHAR(4) COLLATE UTF8_LCASE)) AS c""".stripMargin),
        Row("ab"))
      checkAnswer(
        sql("SELECT coalesce(try_cast(12345 AS VARCHAR(4)), CAST('x' AS VARCHAR(5))) AS c"),
        Row("x"))
      checkError(
        exception = intercept[SparkRuntimeException] {
          sql("SELECT coalesce(CAST(12345 AS VARCHAR(4)), CAST('x' AS VARCHAR(5)))").collect()
        },
        condition = "EXCEED_LIMIT_LENGTH",
        parameters = Map("limit" -> "4")
      )
    }
  }

  test("SPARK-58797: store assignment with standardSemantics and charVarcharAsString") {
    withSQLConf(
        SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true",
        SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING.key -> "true") {
      val wide = new StructType().add("c", CharType(10))
      val df = spark.createDataFrame(java.util.Arrays.asList(Row("spark")), wide)
      checkError(
        exception = intercept[SparkRuntimeException] {
          df.to(new StructType().add("c", CharType(3))).collect()
        },
        condition = "EXCEED_LIMIT_LENGTH",
        parameters = Map("limit" -> "3"))
      withTable("std_and_as_string") {
        sql("CREATE TABLE std_and_as_string (v VARCHAR(2)) USING parquet")
        checkError(
          exception = intercept[SparkRuntimeException] {
            sql("INSERT INTO std_and_as_string VALUES ('abc')")
          },
          condition = "EXCEED_LIMIT_LENGTH",
          parameters = Map("limit" -> "2"))
      }
    }
  }

  test("SPARK-58798: least common type for COALESCE/CASE with CHAR/VARCHAR") {
    withSQLConf(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true") {
      assert(sql(
        "SELECT coalesce(cast('hello' AS VARCHAR(5)), cast('world' AS VARCHAR(10))) AS c")
        .schema.head.dataType === VarcharType(10))
      assert(sql(
        "SELECT coalesce(cast('hello' AS VARCHAR(5)), cast('world!' AS CHAR(6))) AS c")
        .schema.head.dataType === VarcharType(6))
      assert(sql(
        "SELECT coalesce(cast('hello' AS CHAR(5)), cast('world!' AS CHAR(6))) AS c")
        .schema.head.dataType === CharType(6))
      assert(sql(
        "SELECT coalesce(cast('hello' AS VARCHAR(5)), 'world') AS c")
        .schema.head.dataType === StringType)
      assert(sql(
        """SELECT CASE WHEN true THEN cast('a' AS CHAR(2))
          |ELSE cast('bb' AS CHAR(4)) END AS c""".stripMargin)
        .schema.head.dataType === CharType(4))
      // LCT(NULL, T) = T
      assert(sql("SELECT coalesce(null, cast('a' AS CHAR(5))) AS c")
        .schema.head.dataType === CharType(5))
    }
  }

  test("SPARK-58799: transforming string functions return STRING") {
    withSQLConf(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true") {
      assert(sql("SELECT upper(cast('ab' AS CHAR(2))) AS c")
        .schema.head.dataType === StringType)
      assert(sql("SELECT lower(cast('AB' AS VARCHAR(2))) AS c")
        .schema.head.dataType === StringType)
      assert(sql(
        "SELECT cast('a' AS CHAR(1)) || cast('b' AS VARCHAR(1)) AS c")
        .schema.head.dataType === StringType)
      // Pads from CHAR participate in the concatenated value.
      checkAnswer(
        sql("SELECT cast('he' AS CHAR(4)) || cast('llo' AS CHAR(3)) AS c"),
        Row("he  llo"))
      assert(sql("SELECT substr(cast('hello' AS VARCHAR(5)), 1, 2) AS c")
        .schema.head.dataType === StringType)
      assert(sql(
        "SELECT upper(coalesce(cast('a' AS CHAR(2)), cast('b' AS CHAR(4)))) AS c")
        .schema.head.dataType === StringType)
    }
  }

  test("SPARK-58798: LCT preserves collation on CHAR/VARCHAR") {
    withSQLConf(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true") {
      val c1 = CharType(2, "UTF8_LCASE")
      val c2 = CharType(4, "UTF8_LCASE")
      assert(StringHelper.tightestCommonString(c1, c2).contains(CharType(4, "UTF8_LCASE")))
      val v1 = VarcharType(3, "UTF8_LCASE")
      val v2 = VarcharType(5, "UTF8_LCASE")
      assert(StringHelper.tightestCommonString(v1, v2).contains(VarcharType(5, "UTF8_LCASE")))
      assert(StringHelper.tightestCommonString(c1, v2).contains(VarcharType(5, "UTF8_LCASE")))
    }
  }

  test("SPARK-58799: regexp/mask/split return STRING under standardSemantics") {
    withSQLConf(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true") {
      assert(sql("SELECT regexp_replace(cast('ab' AS CHAR(2)), 'a', 'x') AS c")
        .schema.head.dataType === StringType)
      assert(sql("SELECT regexp_extract(cast('ab' AS VARCHAR(2)), '(a)', 1) AS c")
        .schema.head.dataType === StringType)
      assert(sql("SELECT split(cast('a,b' AS CHAR(3)), ',') AS c")
        .schema.head.dataType === ArrayType(StringType, containsNull = false))
      assert(sql("SELECT mask(cast('ab' AS CHAR(2))) AS c")
        .schema.head.dataType === StringType)
    }
  }

  test("SPARK-58796: preserve vs standardSemantics R1 matrix") {
    // preserve-only: transforming ops may keep Char/Varchar (leaky experimental path).
    withSQLConf(SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> "true") {
      assert(sql("SELECT upper(cast('ab' AS CHAR(2))) AS c")
        .schema.head.dataType === CharType(2))
    }
    // standardSemantics: R1 forces STRING even if preserve is also on.
    withSQLConf(
        SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> "true",
        SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true") {
      assert(sql("SELECT upper(cast('ab' AS CHAR(2))) AS c")
        .schema.head.dataType === StringType)
    }
  }

  test("SPARK-58797: standardSemantics wins over charVarcharAsString") {
    withSQLConf(
        SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING.key -> "true",
        SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true") {
      val df = sql("SELECT CAST('ab' AS CHAR(5)) AS c")
      assert(df.schema.head.dataType === CharType(5))
      checkAnswer(df, Row("ab   "))
    }
  }

  test("SPARK-58796: createDataFrame allows CHAR/VARCHAR when standardSemantics") {
    withSQLConf(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true") {
      val df = spark.range(1).map(_.toString).toDF()
      val schema = new StructType().add("id", CharType(5))
      val created = spark.createDataFrame(df.collectAsList(), schema)
      assert(created.schema.head.dataType === CharType(5))
      checkAnswer(created, Row("0    "))

      // RowEncoder must retain a declared collation on the constrained type, not rebuild
      // CharType(length) / VarcharType(length) with the default collation.
      val collated = new StructType()
        .add("c", CharType(5, "UTF8_LCASE"))
        .add("v", VarcharType(5, "UTF8_LCASE"))
      val collatedDf = spark.createDataFrame(
        java.util.Arrays.asList(Row("ab", "cd")), collated)
      assert(collatedDf.schema("c").dataType === CharType(5, "UTF8_LCASE"))
      assert(collatedDf.schema("v").dataType === VarcharType(5, "UTF8_LCASE"))
      checkAnswer(collatedDf, Row("ab   ", "cd"))
    }
  }

  test("SPARK-58803: Dataset/encoder/UDF CHAR/VARCHAR under standardSemantics") {
    withSQLConf(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true") {
      // createDataFrame / RowEncoder write-side: pad CHAR, reject oversize.
      val charSchema = new StructType().add("c", CharType(3))
      checkAnswer(
        spark.createDataFrame(java.util.Arrays.asList(Row("ab")), charSchema),
        Row("ab "))
      checkError(
        exception = intercept[SparkRuntimeException] {
          spark.createDataFrame(java.util.Arrays.asList(Row("abcd")), charSchema).collect()
        },
        condition = "EXCEED_LIMIT_LENGTH",
        parameters = Map("limit" -> "3"))
      val varcharSchema = new StructType().add("v", VarcharType(3))
      checkAnswer(
        spark.createDataFrame(java.util.Arrays.asList(Row("ab")), varcharSchema),
        Row("ab"))
      // Oversize by trailing blanks only: trim just enough to fit the limit.
      checkAnswer(
        spark.createDataFrame(java.util.Arrays.asList(Row("abc ")), varcharSchema),
        Row("abc"))
      checkError(
        exception = intercept[SparkRuntimeException] {
          spark.createDataFrame(java.util.Arrays.asList(Row("abcd")), varcharSchema).collect()
        },
        condition = "EXCEED_LIMIT_LENGTH",
        parameters = Map("limit" -> "3"))

      // Explicit Encoders.CHAR / VARCHAR: typed Dataset write-side checks.
      val charDs = spark.createDataset(Seq("ab"))(Encoders.CHAR(4))
      assert(charDs.schema.head.dataType === CharType(4))
      checkAnswer(charDs.toDF(), Row("ab  "))
      checkError(
        exception = intercept[SparkRuntimeException] {
          spark.createDataset(Seq("abcde"))(Encoders.VARCHAR(3)).collect()
        },
        condition = "EXCEED_LIMIT_LENGTH",
        parameters = Map("limit" -> "3"))

      // UDF register: return type stays CHAR/VARCHAR; write-side pad / length apply.
      spark.udf.register("std_char_udf", () => "B", CharType(3))
      spark.udf.register("std_varchar_udf", (x: String) => x, VarcharType(3))
      val charUdf = sql("SELECT std_char_udf() AS c")
      assert(charUdf.schema.head.dataType === CharType(3))
      checkAnswer(charUdf, Row("B  "))
      val varcharUdf = sql("SELECT std_varchar_udf('ab') AS v")
      assert(varcharUdf.schema.head.dataType === VarcharType(3))
      checkAnswer(varcharUdf, Row("ab"))
      checkError(
        exception = intercept[SparkException] {
          sql("SELECT std_varchar_udf('abcd')").collect()
        }.getCause.asInstanceOf[SparkRuntimeException],
        condition = "EXCEED_LIMIT_LENGTH",
        parameters = Map("limit" -> "3"))

      // Java udf(..., returnType) path and Dataset.encoder from CHAR result schema.
      val javaUdf = functions.udf(
        new org.apache.spark.sql.api.java.UDF0[String] {
          override def call(): String = "a"
        },
        CharType(5))
      val javaUdfDf = spark.range(1).select(javaUdf().as("c"))
      assert(javaUdfDf.schema.head.dataType === CharType(5))
      checkAnswer(javaUdfDf, Row("a    "))
      assert(javaUdfDf.encoder.schema.head.dataType === CharType(5))

      // Dataset.to: CHAR/VARCHAR target schema allowed; Cast applies store assignment.
      withTable("std_cv_to") {
        sql("CREATE TABLE std_cv_to (c CHAR(10), v VARCHAR(255)) USING parquet")
        sql("INSERT INTO std_cv_to VALUES ('spark', 'awesome')")
        val df = sql("SELECT * FROM std_cv_to")
        assert(df.schema("c").dataType === CharType(10))
        assert(df.schema("v").dataType === VarcharType(255))
        val reordered = StructType.fromDDL("v VARCHAR(255), c CHAR(10)")
        val toDf = df.to(reordered)
        assert(toDf.schema.map(_.dataType) === Seq(VarcharType(255), CharType(10)))
        checkAnswer(toDf, Row("awesome", "spark     "))
        // Narrowing CHAR length is store assignment and must enforce length.
        checkError(
          exception = intercept[SparkRuntimeException] {
            df.select($"c").to(new StructType().add("c", CharType(3))).collect()
          },
          condition = "EXCEED_LIMIT_LENGTH",
          parameters = Map("limit" -> "3"))
      }

      // DataFrameReader / DataStreamReader user schemas keep CHAR/VARCHAR.
      val readerSchema = new StructType().add("id", CharType(5))
      val csvInput = spark.range(1).map(_.toString)
      val csvDf = spark.read.schema(readerSchema).csv(csvInput)
      assert(csvDf.schema.head.dataType === CharType(5))
      checkAnswer(csvDf, Row("0    "))
      val csvDfDdl = spark.read.schema("id VARCHAR(5)").csv(csvInput)
      assert(csvDfDdl.schema.head.dataType === VarcharType(5))
      withTempPath { dir =>
        spark.range(1).write.save(dir.toString)
        val streamDf = spark.readStream.schema(readerSchema).load(dir.toString)
        assert(streamDf.schema.head.dataType === CharType(5))
        val streamDdl = spark.readStream.schema("id VARCHAR(5)").load(dir.toString)
        assert(streamDdl.schema.head.dataType === VarcharType(5))
      }
    }
  }

  test("SPARK-58794: R1 promotion unifies CHAR/VARCHAR with STRING at plain-string inputs") {
    withSQLConf(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true") {
      withTable("std_promote") {
        sql("CREATE TABLE std_promote (c CHAR(5), v VARCHAR(5)) USING parquet")
        sql("INSERT INTO std_promote VALUES ('ab', 'ab')")

        // Expressions requiring all their string inputs to share one type must accept a
        // CHAR/VARCHAR argument alongside a STRING one by promoting it to STRING.
        Seq(
          "overlay(c PLACING 'x' FROM 1)" -> "xb   ",
          "overlay(v PLACING 'x' FROM 1)" -> "xb",
          "string_agg(c, '-')" -> "ab   ",
          "listagg(c, '-')" -> "ab   ",
          "elt(1, c, 'x')" -> "ab   ",
          // right() is RuntimeReplaceable; its literal branches must agree with the substring
          // branch, which R1 has already reduced to STRING.
          "right(c, 2)" -> "  ",
          "left(c, 2)" -> "ab").foreach { case (expr, expected) =>
          val df = sql(s"SELECT $expr AS r FROM std_promote")
          assert(df.schema.head.dataType === StringType, s"$expr should return STRING")
          checkAnswer(df, Row(expected))
        }

        // Transforming expressions must not inherit the input's length constraint: each of these
        // produces a value whose length differs from the CHAR(5) input.
        Seq(
          "reverse(c)" -> "   ba",
          "hex(c)" -> "6162202020",
          "array_join(array(c, c), '-')" -> "ab   -ab   ").foreach { case (expr, expected) =>
          val df = sql(s"SELECT $expr AS r FROM std_promote")
          assert(df.schema.head.dataType === StringType, s"$expr should return STRING")
          checkAnswer(df, Row(expected))
        }

        // Promotion must not reach pass-through / LCT sites, which preserve CHAR/VARCHAR (R2/R3).
        Seq(
          "c", "coalesce(c, c)", "case when true then c else c end", "max(c)",
          "element_at(array(c), 1)", "transform(array(c), x -> x)[0]",
          "first_value(c) over (order by 1)").foreach { expr =>
          val df = sql(s"SELECT $expr AS r FROM std_promote")
          assert(df.schema.head.dataType === CharType(5), s"$expr should stay CHAR(5)")
        }

        // reverse() on non-string inputs is unaffected by the R1 change.
        assert(sql("SELECT reverse(array(1, 2)) AS r").schema.head.dataType ===
          ArrayType(IntegerType, containsNull = false))
      }
    }
  }

  // Allowlist for the inventory below: R2/R3 pass-through and container cases that may keep
  // CHAR(n)/VARCHAR(n): aggregates/ordering that return an input unchanged, null-handling,
  // element access, array/map/struct constructors, and collection rearrangements that keep
  // element types. Coverage is limited to the seven fixed argumentShapes templates in the test;
  // a leak only at another arity or nested shape would not fail here. For those shapes,
  // anything not listed must reduce to plain STRING (R1).
  private val charVarcharPassThroughFunctions = Set(
    "any_value", "approx_top_k", "approx_top_k_accumulate", "array", "array_agg", "array_compact",
    "array_distinct", "array_max", "array_min", "array_repeat", "array_sort", "arrays_zip",
    "coalesce", "collect_list", "collect_set", "collect_union", "concat", "explode",
    "explode_outer", "first", "first_value", "get", "greatest", "ifnull", "last", "last_value",
    "least", "map", "max", "max_by", "measure", "min", "min_by", "mode", "named_struct", "nullif",
    "nullifzero", "nvl", "reverse", "shuffle", "sort_array", "struct", "when")

  test("SPARK-58794: inventoried shapes do not leak CHAR/VARCHAR under standardSemantics") {
    val argumentShapes = Seq(
      "%s(c)", "%s(c, c)", "%s(c, 'x')", "%s('x', c)", "%s(c, 1)", "%s(array(c))",
      "%s(array(c), '-')")

    withTable("std_inventory") {
      sql("CREATE TABLE std_inventory (c CHAR(5)) USING parquet")
      withSQLConf(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true") {
        val leaks = FunctionRegistry.functionSet.map(_.funcName).toSeq.sorted
          .filterNot(charVarcharPassThroughFunctions.contains)
          .flatMap { name =>
            argumentShapes.map(_.format(name)).filter { call =>
              // Most shapes do not typecheck for a given function; those are simply not evidence.
              Try(sql(s"SELECT $call AS r FROM std_inventory").schema.head.dataType)
                .toOption
                .exists(CharVarcharUtils.hasCharVarchar)
            }
          }

        assert(leaks.isEmpty,
          "these inventoried calls returned a CHAR/VARCHAR type; either fix the expression to " +
            "return plain STRING or add it to charVarcharPassThroughFunctions: " +
            leaks.mkString(", "))
      }
    }
  }

  test("SPARK-58794: collated mixed-length LCT ignores collation strength for length") {
    val mixedLength =
      """SELECT coalesce(
        |  cast('a' AS CHAR(2) COLLATE UTF8_LCASE),
        |  cast('bb' AS CHAR(4) COLLATE UTF8_LCASE)) AS c""".stripMargin
    val mixedStrength =
      """SELECT coalesce(
        |  cast('a' AS CHAR(2) COLLATE UTF8_LCASE),
        |  cast(1 AS CHAR(4) COLLATE UTF8_LCASE)) AS c""".stripMargin

    Seq(
      SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true",
      SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key -> "true").foreach { case (key, value) =>
      withSQLConf(key -> value) {
        assert(sql(mixedLength).schema.head.dataType === CharType(4, "UTF8_LCASE"),
          s"$key=$value same-strength mixed CHAR lengths")
        assert(sql(mixedStrength).schema.head.dataType === CharType(4, "UTF8_LCASE"),
          s"$key=$value Implicit CHAR(2) vs Default CHAR(4) must widen, not narrow")
      }
    }
  }

  test("SPARK-58794: typed CHAR Literal is re-padded when LCT widens the length") {
    // CollationTypeCoercion.changeType used to `copy(dataType)` on Literal, which would
    // leave CHAR(2) "a " as a CHAR(4) value without the extra pad. SQL CAST is a Cast
    // node so goldens do not cover this; Literal.create does.
    withSQLConf(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true") {
      val c2 = Column(Literal.create("a", CharType(2, "UTF8_LCASE")))
      val c4 = Column(Literal.create("bb", CharType(4, "UTF8_LCASE")))
      val coalesced = functions.coalesce(c2, c4)
      val df = spark.range(1).select(coalesced.as("c"))
      assert(df.schema.head.dataType === CharType(4, "UTF8_LCASE"))
      checkAnswer(df, Row("a   "))
    }
  }

  test("SPARK-58794: parameterized CHAR/VARCHAR lengths under standardSemantics") {
    // Length positions accept parameter markers (`integerValue` -> `parameterMarker`). Under
    // standardSemantics the bound type stays first-class: CAST keeps CHAR/VARCHAR, pads and
    // enforces length, and DDL schemas retain the substituted n.
    withSQLConf(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true") {
      val charDf = spark.sql("SELECT cast('ab' AS CHAR(:n)) AS c", Map("n" -> 5))
      assert(charDf.schema.head.dataType === CharType(5))
      checkAnswer(
        spark.sql("SELECT concat('<', cast('ab' AS CHAR(:n)), '>')", Map("n" -> 5)),
        Row("<ab   >"))

      val varcharDf = spark.sql("SELECT cast('hello' AS VARCHAR(?)) AS c", Array(5))
      assert(varcharDf.schema.head.dataType === VarcharType(5))
      checkAnswer(
        spark.sql("SELECT cast('abcdef' AS VARCHAR(?))", Array(2)),
        Row("ab"))

      withTable("param_varchar", "param_char") {
        spark.sql(
          "CREATE TABLE param_varchar (c VARCHAR(:n)) USING parquet", Map("n" -> 7))
        assert(spark.table("param_varchar").schema.head.dataType === VarcharType(7))
        spark.sql("CREATE TABLE param_char (c CHAR(?)) USING parquet", Array(4))
        assert(spark.table("param_char").schema.head.dataType === CharType(4))
      }

      // Non-integral / negative lengths fail when substituted into the length position.
      checkError(
        exception = intercept[ParseException] {
          spark.sql("SELECT cast('a' AS CHAR(:n))", Map("n" -> -1))
        },
        condition = "PARSE_SYNTAX_ERROR",
        parameters = Map("error" -> "'-'", "hint" -> ""),
        context = ExpectedContext(
          fragment = "SELECT cast('a' AS CHAR(:n))",
          start = 0,
          stop = 27))
      checkError(
        exception = intercept[ParseException] {
          spark.sql("SELECT cast('a' AS CHAR(:n))", Map("n" -> 1.5))
        },
        condition = "PARSE_SYNTAX_ERROR",
        parameters = Map("error" -> "'1.5D'", "hint" -> ""),
        context = ExpectedContext(
          fragment = "SELECT cast('a' AS CHAR(:n))",
          start = 0,
          stop = 27))
    }
  }

  test("SPARK-58802: single-pass resolver agrees with fixed-point under standardSemantics") {
    // Dual run defaults to on under tests, but pin it explicitly so this coverage cannot be
    // silently lost: the HybridAnalyzer compares output schema and normalized plan across the
    // two analyzers and fails with HYBRID_ANALYZER_EXCEPTION on any divergence. Resolver has no
    // Char/Varchar-specific logic; it inherits Expression.dataType and shared TypeCoercion, so
    // this matrix is the proof that LCT/CAST/R1 stay aligned (D19).
    withSQLConf(
        SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true",
        SQLConf.ANALYZER_DUAL_RUN_LEGACY_AND_SINGLE_PASS_RESOLVER.key -> "true",
        SQLConf.ANALYZER_DUAL_RUN_SAMPLE_RATE.key -> "1.0",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED_TENTATIVELY.key -> "false",
        SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_EXPOSE_RESOLVER_GUARD_FAILURE.key -> "true") {
      // CAST / try_cast introduce the type (R3).
      assert(sql("SELECT CAST('ab' AS CHAR(5)) AS c").schema.head.dataType === CharType(5))
      assert(sql("SELECT CAST('hello' AS VARCHAR(5)) AS c").schema.head.dataType ===
        VarcharType(5))
      assert(sql("SELECT try_cast('abcdef' AS CHAR(2)) AS c").schema.head.dataType ===
        CharType(2))
      checkAnswer(sql("SELECT try_cast('abcdef' AS VARCHAR(2)) AS c"), Row("ab"))
      checkAnswer(
        sql("SELECT coalesce(CAST('abcdef' AS VARCHAR(2)), CAST('x' AS VARCHAR(4))) AS c"),
        Row("ab"))
      checkAnswer(
        sql("SELECT CAST('abcdef' AS VARCHAR(2)) IN (CAST('ab' AS VARCHAR(4)))"),
        Row(true))
      checkAnswer(
        sql("""SELECT coalesce(
          |  CAST('abcdef' AS VARCHAR(2) COLLATE UTF8_LCASE),
          |  CAST('x' AS VARCHAR(4) COLLATE UTF8_LCASE)) AS c""".stripMargin),
        Row("ab"))

      // Least common type (R2): COALESCE / CASE / NULL / CHAR+VARCHAR / CHAR+STRING.
      assert(sql(
        "SELECT coalesce(CAST('a' AS VARCHAR(3)), CAST('bb' AS VARCHAR(7))) AS c")
        .schema.head.dataType === VarcharType(7))
      assert(sql(
        "SELECT coalesce(CAST('a' AS CHAR(2)), CAST('bb' AS VARCHAR(4))) AS c")
        .schema.head.dataType === VarcharType(4))
      assert(sql(
        "SELECT coalesce(CAST('a' AS CHAR(2)), 'bb') AS c")
        .schema.head.dataType === StringType)
      assert(sql(
        "SELECT coalesce(CAST('a' AS CHAR(5)), CAST(NULL AS CHAR(5))) AS c")
        .schema.head.dataType === CharType(5))
      assert(sql(
        "SELECT CASE WHEN true THEN CAST('a' AS CHAR(2)) ELSE CAST('bb' AS CHAR(4)) END AS c")
        .schema.head.dataType === CharType(4))
      assert(sql(
        "SELECT CASE WHEN false THEN CAST('a' AS VARCHAR(2)) ELSE CAST('bb' AS CHAR(4)) END AS c")
        .schema.head.dataType === VarcharType(4))

      // IN-list common type (side condition uses LCT; result is boolean).
      checkAnswer(
        sql("SELECT CAST('a' AS CHAR(2)) IN (CAST('a ' AS CHAR(2)), CAST('bbb' AS VARCHAR(3)))"),
        Row(true))

      // Transforming operators return STRING (R1).
      assert(sql("SELECT upper(CAST('ab' AS CHAR(2))) AS c").schema.head.dataType === StringType)
      assert(sql("SELECT lower(CAST('AB' AS VARCHAR(2))) AS c").schema.head.dataType ===
        StringType)
      assert(sql("SELECT CAST('a' AS CHAR(1)) || CAST('b' AS VARCHAR(1)) AS c")
        .schema.head.dataType === StringType)
      assert(sql("SELECT concat(CAST('a' AS CHAR(2)), CAST('b' AS CHAR(3))) AS c")
        .schema.head.dataType === StringType)
      assert(sql("SELECT substr(CAST('hello' AS VARCHAR(5)), 1, 2) AS c")
        .schema.head.dataType === StringType)
      assert(sql("SELECT trim(CAST('ab  ' AS CHAR(4))) AS c").schema.head.dataType === StringType)
      assert(sql("SELECT regexp_replace(CAST('ab' AS CHAR(2)), 'a', 'x') AS c")
        .schema.head.dataType === StringType)
      assert(sql("SELECT mask(CAST('ab' AS CHAR(2))) AS c").schema.head.dataType === StringType)
      assert(sql("SELECT split(CAST('a,b' AS CHAR(3)), ',') AS c").schema.head.dataType ===
        ArrayType(StringType, containsNull = false))
      // R1 after LCT: coalesce stays CHAR, upper widens to STRING.
      assert(sql(
        "SELECT upper(coalesce(CAST('a' AS CHAR(2)), CAST('b' AS CHAR(4)))) AS c")
        .schema.head.dataType === StringType)

      // Set-operation LCT.
      val union = sql(
        """SELECT CAST('a' AS VARCHAR(3)) AS c
          |UNION ALL
          |SELECT CAST('abcd' AS VARCHAR(8)) AS c""".stripMargin)
      assert(union.schema.head.dataType === VarcharType(8))
      checkAnswer(union, Seq(Row("a"), Row("abcd")))

      val intersect = sql(
        """SELECT CAST('ab' AS CHAR(2)) AS c
          |INTERSECT
          |SELECT CAST('ab' AS CHAR(4)) AS c""".stripMargin)
      assert(intersect.schema.head.dataType === CharType(4))
      checkAnswer(intersect, Seq(Row("ab  ")))

      // Nested types keep CHAR/VARCHAR through analysis.
      assert(sql("SELECT array(CAST('a' AS CHAR(2)), CAST('bb' AS CHAR(3))) AS c")
        .schema.head.dataType === ArrayType(CharType(3), containsNull = false))
      assert(sql("SELECT struct(CAST('a' AS CHAR(2)) AS f) AS c")
        .schema.head.dataType ===
        StructType(Seq(StructField("f", CharType(2), nullable = false))))

      // Collated mixed-length LCT (the CollationTypeCoercion equal-strength and
      // mixed-strength paths). Set ops have a separate resolver path.
      assert(sql(
        """SELECT coalesce(
          |  CAST('a' AS CHAR(2) COLLATE UTF8_LCASE),
          |  CAST('bb' AS CHAR(4) COLLATE UTF8_LCASE)) AS c""".stripMargin)
        .schema.head.dataType === CharType(4, "UTF8_LCASE"))
      assert(sql(
        """SELECT coalesce(
          |  CAST('a' AS CHAR(2) COLLATE UTF8_LCASE),
          |  CAST(1 AS CHAR(4) COLLATE UTF8_LCASE)) AS c""".stripMargin)
        .schema.head.dataType === CharType(4, "UTF8_LCASE"))
      checkAnswer(
        sql("SELECT CAST('a' AS CHAR(2) COLLATE UTF8_LCASE) = " +
          "CAST('a' AS CHAR(4) COLLATE UTF8_LCASE)"),
        Row(true))
      checkAnswer(
        sql("SELECT CAST('a' AS CHAR(2) COLLATE UTF8_LCASE) IN " +
          "(CAST('a' AS CHAR(4) COLLATE UTF8_LCASE))"),
        Row(true))
      checkAnswer(
        sql("SELECT CAST('a' AS CHAR(2) COLLATE UTF8_LCASE) = " +
          "CAST('a' AS VARCHAR(2) COLLATE UTF8_LCASE)"),
        Row(false))
      checkAnswer(
        sql("SELECT CAST('a' AS CHAR(2) COLLATE UTF8_LCASE) IN " +
          "(CAST('a' AS VARCHAR(2) COLLATE UTF8_LCASE))"),
        Row(false))

      val mixedCharUnion = sql(
        """SELECT CAST('a' AS CHAR(2)) AS c
          |UNION ALL
          |SELECT CAST('bb' AS CHAR(4)) AS c""".stripMargin)
      assert(mixedCharUnion.schema.head.dataType === CharType(4))
      checkAnswer(mixedCharUnion, Seq(Row("a   "), Row("bb  ")))

      // Bare column references keep the declared type (R3) through dual-run analysis.
      withTable("char_varchar_dual_run") {
        sql("CREATE TABLE char_varchar_dual_run (c CHAR(5), v VARCHAR(5)) USING parquet")
        sql("INSERT INTO char_varchar_dual_run VALUES ('ab', 'ab')")
        val df = sql("SELECT c, v FROM char_varchar_dual_run")
        assert(df.schema("c").dataType === CharType(5))
        assert(df.schema("v").dataType === VarcharType(5))
        checkAnswer(df, Row("ab   ", "ab"))
      }
    }
  }

  test("invalidate char/varchar in functions") {
    checkError(
      exception = intercept[AnalysisException] {
        sql("""SELECT from_json('{"a": "str"}', 'a CHAR(5)')""")
      },
      condition = "UNSUPPORTED_CHAR_OR_VARCHAR_AS_STRING",
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
      condition = "UNSUPPORTED_CHAR_OR_VARCHAR_AS_STRING"
    )
    checkError(
      exception = intercept[AnalysisException] {
        spark.createDataFrame(df.rdd, schema)
      },
      condition = "UNSUPPORTED_CHAR_OR_VARCHAR_AS_STRING"
    )
    checkError(
      exception = intercept[AnalysisException] {
        spark.createDataFrame(df.toJavaRDD, schema)
      },
      condition = "UNSUPPORTED_CHAR_OR_VARCHAR_AS_STRING"
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
      condition = "UNSUPPORTED_CHAR_OR_VARCHAR_AS_STRING")
    checkError(
      exception = intercept[AnalysisException] {
        spark.read.schema("id char(5)")
      },
      condition = "UNSUPPORTED_CHAR_OR_VARCHAR_AS_STRING"
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
          case d: DataSourceV2Relation => CatalogV2Util.v2ColumnsToStructType(d.table.columns())
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
      condition = "UNSUPPORTED_CHAR_OR_VARCHAR_AS_STRING"
    )
    checkError(
      exception = intercept[AnalysisException] {
        spark.udf.register("testchar2", (x: String) => x, VarcharType(1))
      },
      condition = "UNSUPPORTED_CHAR_OR_VARCHAR_AS_STRING"
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
      condition = "UNSUPPORTED_CHAR_OR_VARCHAR_AS_STRING"
    )
    checkError(
      exception = intercept[AnalysisException] {
        spark.readStream.schema("id char(5)")
      },
      condition = "UNSUPPORTED_CHAR_OR_VARCHAR_AS_STRING"
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
      }, condition = "UNSUPPORTED_CHAR_OR_VARCHAR_AS_STRING", parameters = Map.empty)
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

  test("SPARK-58801: standardSemantics scan pads CHAR and errors on oversize") {
    withSQLConf(SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key -> "true") {
      withTempPath { dir =>
        withTable("t") {
          sql("SELECT '12' as col").write.format(format).save(dir.toString)
          sql(s"CREATE TABLE t (col CHAR(3)) using $format LOCATION '$dir'")
          checkAnswer(sql("SELECT * FROM t"), Row("12 "))
        }
      }
      Seq("CHAR", "VARCHAR").foreach { typ =>
        withTempPath { dir =>
          withTable("t") {
            sql("SELECT '123456' as col").write.format(format).save(dir.toString)
            sql(s"CREATE TABLE t (col $typ(2)) using $format LOCATION '$dir'")
            checkError(
              exception = intercept[SparkRuntimeException] {
                sql("SELECT * FROM t").collect()
              },
              condition = "EXCEED_LIMIT_LENGTH",
              parameters = Map("limit" -> "2")
            )
          }
        }
      }
      // An oversized value consisting only of trailing blanks is trimmed successfully.
      withTempPath { dir =>
        withTable("t") {
          sql("SELECT '12  ' as col").write.format(format).save(dir.toString)
          sql(s"CREATE TABLE t (col VARCHAR(2)) using $format LOCATION '$dir'")
          checkAnswer(sql("SELECT * FROM t"), Row("12"))
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

  test("SPARK-48498: always do char padding in predicates") {
    import testImplicits._
    withSQLConf(SQLConf.READ_SIDE_CHAR_PADDING.key -> "false") {
      withTempPath { dir =>
        withTable("t1", "t2") {
          Seq(
            "12" -> "12",
            "12" -> "12 ",
            "12 " -> "12",
            "12 " -> "12 "
          ).toDF("c1", "c2").write.format(format).save(dir.toString)

          sql(s"CREATE TABLE t1 (c1 CHAR(3), c2 STRING) USING $format LOCATION '$dir'")
          // Comparing CHAR column with STRING column directly compares the stored value.
          checkAnswer(
            sql("SELECT c1 = c2 FROM t1"),
            Seq(Row(true), Row(false), Row(false), Row(true))
          )
          checkAnswer(
            sql("SELECT c1 IN (c2) FROM t1"),
            Seq(Row(true), Row(false), Row(false), Row(true))
          )
          // No matter the CHAR type value is padded or not in the storage, we should always pad it
          // before comparison with STRING literals.
          checkAnswer(
            sql("SELECT c1 = '12', c1 = '12 ', c1 = '12  ' FROM t1 WHERE c2 = '12'"),
            Seq(Row(true, true, true), Row(true, true, true))
          )
          checkAnswer(
            sql("SELECT c1 IN ('12'), c1 IN ('12 '), c1 IN ('12  ') FROM t1 WHERE c2 = '12'"),
            Seq(Row(true, true, true), Row(true, true, true))
          )

          sql(s"CREATE TABLE t2 (c1 CHAR(3), c2 CHAR(5)) USING $format LOCATION '$dir'")
          // Comparing CHAR column with CHAR column compares the padded values.
          checkAnswer(
            sql("SELECT c1 = c2, c2 = c1 FROM t2"),
            Seq(Row(true, true), Row(true, true), Row(true, true), Row(true, true))
          )
          checkAnswer(
            sql("SELECT c1 IN (c2), c2 IN (c1) FROM t2"),
            Seq(Row(true, true), Row(true, true), Row(true, true), Row(true, true))
          )
        }
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
