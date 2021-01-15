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

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.{InMemoryPartitionTableCatalog, SchemaRequiredDataSource}
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

  test("char type values should be padded: top-level columns") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c CHAR(5)) USING $format")
      sql("INSERT INTO t VALUES ('1', 'a')")
      checkAnswer(spark.table("t"), Row("1", "a" + " " * 4))
      checkColType(spark.table("t").schema(1), CharType(5))

      sql("INSERT OVERWRITE t VALUES ('1', null)")
      checkAnswer(spark.table("t"), Row("1", null))
    }
  }

  test("char type values should be padded: partitioned columns") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c CHAR(5)) USING $format PARTITIONED BY (c)")
      sql("INSERT INTO t VALUES ('1', 'a')")
      checkAnswer(spark.table("t"), Row("1", "a" + " " * 4))
      checkColType(spark.table("t").schema(1), CharType(5))

      sql("ALTER TABLE t DROP PARTITION(c='a')")
      sql("INSERT OVERWRITE t VALUES ('1', null)")
      checkAnswer(spark.table("t"), Row("1", null))
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
      val e = intercept[SparkException](sql("INSERT INTO t VALUES ('123456')"))
      assert(e.getCause.getMessage.contains(s"Exceeds $typeName type length limitation: 5"))
    }
  }

  test("length check for input string values: partitioned columns") {
    // DS V2 doesn't support partitioned table.
    if (!conf.contains(SQLConf.DEFAULT_CATALOG.key)) {
      testTableWrite { typeName =>
        sql(s"CREATE TABLE t(i INT, c $typeName(5)) USING $format PARTITIONED BY (c)")
        sql("INSERT INTO t VALUES (1, null)")
        checkAnswer(spark.table("t"), Row(1, null))
        val e = intercept[SparkException](sql("INSERT INTO t VALUES (1, '123456')"))
        assert(e.getCause.getMessage.contains(s"Exceeds $typeName type length limitation: 5"))
      }
    }
  }

  test("length check for input string values: nested in struct") {
    testTableWrite { typeName =>
      sql(s"CREATE TABLE t(c STRUCT<c: $typeName(5)>) USING $format")
      sql("INSERT INTO t SELECT struct(null)")
      checkAnswer(spark.table("t"), Row(Row(null)))
      val e = intercept[SparkException](sql("INSERT INTO t SELECT struct('123456')"))
      assert(e.getCause.getMessage.contains(s"Exceeds $typeName type length limitation: 5"))
    }
  }

  test("length check for input string values: nested in array") {
    testTableWrite { typeName =>
      sql(s"CREATE TABLE t(c ARRAY<$typeName(5)>) USING $format")
      sql("INSERT INTO t VALUES (array(null))")
      checkAnswer(spark.table("t"), Row(Seq(null)))
      val e = intercept[SparkException](sql("INSERT INTO t VALUES (array('a', '123456'))"))
      assert(e.getCause.getMessage.contains(s"Exceeds $typeName type length limitation: 5"))
    }
  }

  test("length check for input string values: nested in map key") {
    testTableWrite { typeName =>
      sql(s"CREATE TABLE t(c MAP<$typeName(5), STRING>) USING $format")
      val e = intercept[SparkException](sql("INSERT INTO t VALUES (map('123456', 'a'))"))
      assert(e.getCause.getMessage.contains(s"Exceeds $typeName type length limitation: 5"))
    }
  }

  test("length check for input string values: nested in map value") {
    testTableWrite { typeName =>
      sql(s"CREATE TABLE t(c MAP<STRING, $typeName(5)>) USING $format")
      sql("INSERT INTO t VALUES (map('a', null))")
      checkAnswer(spark.table("t"), Row(Map("a" -> null)))
      val e = intercept[SparkException](sql("INSERT INTO t VALUES (map('a', '123456'))"))
      assert(e.getCause.getMessage.contains(s"Exceeds $typeName type length limitation: 5"))
    }
  }

  test("length check for input string values: nested in both map key and value") {
    testTableWrite { typeName =>
      sql(s"CREATE TABLE t(c MAP<$typeName(5), $typeName(5)>) USING $format")
      val e1 = intercept[SparkException](sql("INSERT INTO t VALUES (map('123456', 'a'))"))
      assert(e1.getCause.getMessage.contains(s"Exceeds $typeName type length limitation: 5"))
      val e2 = intercept[SparkException](sql("INSERT INTO t VALUES (map('a', '123456'))"))
      assert(e2.getCause.getMessage.contains(s"Exceeds $typeName type length limitation: 5"))
    }
  }

  test("length check for input string values: nested in struct of array") {
    testTableWrite { typeName =>
      sql(s"CREATE TABLE t(c STRUCT<c: ARRAY<$typeName(5)>>) USING $format")
      sql("INSERT INTO t SELECT struct(array(null))")
      checkAnswer(spark.table("t"), Row(Row(Seq(null))))
      val e = intercept[SparkException](sql("INSERT INTO t SELECT struct(array('123456'))"))
      assert(e.getCause.getMessage.contains(s"Exceeds $typeName type length limitation: 5"))
    }
  }

  test("length check for input string values: nested in array of struct") {
    testTableWrite { typeName =>
      sql(s"CREATE TABLE t(c ARRAY<STRUCT<c: $typeName(5)>>) USING $format")
      sql("INSERT INTO t VALUES (array(struct(null)))")
      checkAnswer(spark.table("t"), Row(Seq(Row(null))))
      val e = intercept[SparkException](sql("INSERT INTO t VALUES (array(struct('123456')))"))
      assert(e.getCause.getMessage.contains(s"Exceeds $typeName type length limitation: 5"))
    }
  }

  test("length check for input string values: nested in array of array") {
    testTableWrite { typeName =>
      sql(s"CREATE TABLE t(c ARRAY<ARRAY<$typeName(5)>>) USING $format")
      sql("INSERT INTO t VALUES (array(array(null)))")
      checkAnswer(spark.table("t"), Row(Seq(Seq(null))))
      val e = intercept[SparkException](sql("INSERT INTO t VALUES (array(array('123456')))"))
      assert(e.getCause.getMessage.contains(s"Exceeds $typeName type length limitation: 5"))
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
      val e1 = intercept[SparkException](sql("INSERT INTO t VALUES (123456, 1)"))
      assert(e1.getCause.getMessage.contains("Exceeds char type length limitation: 5"))
      val e2 = intercept[SparkException](sql("INSERT INTO t VALUES (1, 123456)"))
      assert(e2.getCause.getMessage.contains("Exceeds varchar type length limitation: 5"))
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
        ("c1 IN (c2)", true)))
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
        ("c1 IN (c2)", true)))
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

  test("SPARK-33992: char/varchar resolution in correlated sub query") {
    withTable("t1", "t2") {
      sql(s"CREATE TABLE t1(v VARCHAR(3), c CHAR(5)) USING $format")
      sql(s"CREATE TABLE t2(v VARCHAR(3), c CHAR(5)) USING $format")
      sql("INSERT INTO t1 VALUES ('c', 'b')")
      sql("INSERT INTO t2 VALUES ('a', 'b')")

      checkAnswer(sql(
        """
          |SELECT v FROM t1
          |WHERE 'a' IN (SELECT v FROM t2 WHERE t1.c = t2.c )""".stripMargin),
        Row("c"))
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

  def failWithInvalidCharUsage[T](fn: => T): Unit = {
    val e = intercept[AnalysisException](fn)
    assert(e.getMessage contains "char/varchar type can only be used in the table schema")
  }

  test("invalidate char/varchar in functions") {
    failWithInvalidCharUsage(sql("""SELECT from_json('{"a": "str"}', 'a CHAR(5)')"""))
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
    failWithInvalidCharUsage(spark.createDataFrame(df.collectAsList(), schema))
    failWithInvalidCharUsage(spark.createDataFrame(df.rdd, schema))
    failWithInvalidCharUsage(spark.createDataFrame(df.toJavaRDD, schema))
    withSQLConf((SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING.key, "true")) {
      val df1 = spark.createDataFrame(df.collectAsList(), schema)
      checkAnswer(df1, df)
      assert(df1.schema.head.dataType === StringType)
    }
  }

  test("invalidate char/varchar in spark.read.schema") {
    failWithInvalidCharUsage(spark.read.schema(new StructType().add("id", CharType(5))))
    failWithInvalidCharUsage(spark.read.schema("id char(5)"))
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
    failWithInvalidCharUsage(spark.udf.register("testchar", () => "B", VarcharType(1)))
    failWithInvalidCharUsage(spark.udf.register("testchar2", (x: String) => x, VarcharType(1)))
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
    failWithInvalidCharUsage(spark.readStream.schema(new StructType().add("id", CharType(5))))
    failWithInvalidCharUsage(spark.readStream.schema("id char(5)"))
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
}

class FileSourceCharVarcharTestSuite extends CharVarcharTestSuite with SharedSparkSession {
  override def format: String = "parquet"
  override protected def sparkConf: SparkConf = {
    super.sparkConf.set(SQLConf.USE_V1_SOURCE_LIST, "parquet")
  }

  test("create table w/ location and fit length values") {
    Seq("char", "varchar").foreach { typ =>
      withTempPath { dir =>
        withTable("t") {
          sql("SELECT '12' as col").write.format(format).save(dir.toString)
          sql(s"CREATE TABLE t (col $typ(2)) using $format LOCATION '$dir'")
          val df = sql("select * from t")
          checkAnswer(sql("select * from t"), Row("12"))
        }
      }
    }
  }

  test("create table w/ location and over length values") {
    Seq("char", "varchar").foreach { typ =>
      withTempPath { dir =>
        withTable("t") {
          sql("SELECT '123456' as col").write.format(format).save(dir.toString)
          sql(s"CREATE TABLE t (col $typ(2)) using $format LOCATION '$dir'")
          val e = intercept[SparkException] { sql("select * from t").collect() }
          assert(e.getCause.getMessage.contains(s"Exceeds $typ type length limitation: 2"))
        }
      }
    }
  }

  test("alter table set location w/ fit length values") {
    Seq("char", "varchar").foreach { typ =>
      withTempPath { dir =>
        withTable("t") {
          sql("SELECT '12' as col").write.format(format).save(dir.toString)
          sql(s"CREATE TABLE t (col $typ(2)) using $format")
          sql(s"ALTER TABLE t SET LOCATION '$dir'")
          checkAnswer(spark.table("t"), Row("12"))
        }
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
          val e = intercept[SparkException] { spark.table("t").collect() }
          assert(e.getCause.getMessage.contains(s"Exceeds $typ type length limitation: 2"))
        }
      }
    }
  }

  // TODO(SPARK-33875): Move these tests to super after DESCRIBE COLUMN v2 implemented
  test("SPARK-33892: DESCRIBE COLUMN w/ char/varchar") {
    withTable("t") {
      sql(s"CREATE TABLE t(v VARCHAR(3), c CHAR(5)) USING $format")
      checkAnswer(sql("desc t v").selectExpr("info_value").where("info_value like '%char%'"),
        Row("varchar(3)"))
      checkAnswer(sql("desc t c").selectExpr("info_value").where("info_value like '%char%'"),
        Row("char(5)"))
    }
  }

  // TODO(SPARK-33898): Move these tests to super after SHOW CREATE TABLE for v2 implemented
  test("SPARK-33892: SHOW CREATE TABLE w/ char/varchar") {
    withTable("t") {
      sql(s"CREATE TABLE t(v VARCHAR(3), c CHAR(5)) USING $format")
      val rest = sql("SHOW CREATE TABLE t").head().getString(0)
      assert(rest.contains("VARCHAR(3)"))
      assert(rest.contains("CHAR(5)"))
    }
  }

  test("SPARK-34114: should not trim right for read-side length check and char padding") {
    Seq("char", "varchar").foreach { typ =>
      withTempPath { dir =>
        withTable("t") {
          sql("SELECT '12  ' as col").write.format(format).save(dir.toString)
          sql(s"CREATE TABLE t (col $typ(2)) using $format LOCATION '$dir'")
          val e = intercept[SparkException] { sql("select * from t").collect() }
          assert(e.getCause.getMessage.contains(s"Exceeds $typ type length limitation: 2"))
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
}
