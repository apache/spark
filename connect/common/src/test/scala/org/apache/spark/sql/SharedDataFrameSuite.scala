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

import java.io.{ByteArrayOutputStream, File}
import java.nio.charset.StandardCharsets
import java.util.Locale

import scala.collection.immutable.ListMap
import scala.util.Random

import org.scalatest.matchers.should.Matchers._

import org.apache.spark.{SparkException, SparkFunSuite, SparkThrowable}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SqlApiConf
import org.apache.spark.sql.types._

case class StringWrapper(s: String) extends AnyVal
case class ArrayStringWrapper(wrappers: Seq[StringWrapper])
case class ContainerStringWrapper(wrapper: StringWrapper)

trait SharedDataFrameSuite extends SparkFunSuite {
  protected val testImplicits: SQLImplicits
  protected val testData: DataFrame
  protected val testData2: DataFrame
  protected val person: DataFrame
  protected val salary: DataFrame
  protected val decimalData: DataFrame
  protected val upperCaseData: DataFrame

  protected def checkAnswer(df: => DataFrame, expectedAnswer: Seq[Row]): Unit
  protected def checkAnswer(df: => DataFrame, expectedAnswer: Row): Unit
  protected def checkAnswer(df: => DataFrame, expectedAnswer: DataFrame): Unit
  protected def checkAnswer(df: => DataFrame, expectedAnswer: Array[Row]): Unit

  protected def withSQLConf[T](pairs: (String, String)*)(f: => T): T
  protected def withTempView(viewNames: String*)(f: => Unit): Unit
  protected def withTempPath(f: File => Unit): Unit
  protected def withTable(tableNames: String*)(f: => Unit): Unit
  protected def withUserDefinedFunction(functions: (String, Boolean)*)(f: => Unit): Unit

  protected def getCurrentClassCallSitePattern: String

  protected implicit def spark: SparkSession
  protected lazy val sql: String => DataFrame = spark.sql _

  import testImplicits._

  test("analysis error should be eagerly reported") {
    intercept[Exception] { testData.select("nonExistentName") }
    intercept[Exception] {
      testData.groupBy("key").agg(Map("nonExistentName" -> "sum"))
    }
    intercept[Exception] {
      testData.groupBy("nonExistentName").agg(Map("key" -> "sum"))
    }
    intercept[Exception] {
      testData.groupBy($"abcd").agg(Map("key" -> "sum"))
    }
  }

  test("rename nested groupby") {
    val df = Seq((1, (1, 1))).toDF()

    checkAnswer(
      df.groupBy("_1").agg(sum("_2._1")).toDF("key", "total"),
      Row(1, 1) :: Nil)
  }

  test("table scan") {
    checkAnswer(
      testData,
      testData.collect().toSeq)
  }

  test("empty data frame") {
    assert(spark.emptyDataFrame.columns.toSeq === Seq.empty[String])
    assert(spark.emptyDataFrame.count() === 0)
  }

  test("head, take and tail") {
    assert(testData.take(2) === testData.collect().take(2))
    assert(testData.head(2) === testData.collect().take(2))
    assert(testData.tail(2) === testData.collect().takeRight(2))
    assert(testData.head(2).head.schema === testData.schema)
  }

  test("dataframe alias") {
    val df = Seq(Tuple1(1)).toDF("c").as("t")
    val dfAlias = df.alias("t2")
    df.col("t.c")
    dfAlias.col("t2.c")
  }

  test("simple explode") {
    val df = Seq(Tuple1("a b c"), Tuple1("d e")).toDF("words")

    checkAnswer(
      df.explode("words", "word") { word: String => word.split(" ").toSeq }.select($"word"),
      Row("a") :: Row("b") :: Row("c") :: Row("d") ::Row("e") :: Nil
    )
  }

  test("explode") {
    val df = Seq((1, "a b c"), (2, "a b"), (3, "a")).toDF("number", "letters")
    val df2 =
      df.explode($"letters") {
        case Row(letters: String) => letters.split(" ").map(Tuple1(_)).toSeq
      }

    checkAnswer(
      df2
        .select($"_1" as Symbol("letter"), $"number")
        .groupBy($"letter")
        .agg(count_distinct($"number")),
      Row("a", 3) :: Row("b", 2) :: Row("c", 1) :: Nil
    )
  }

  test("Star Expansion - CreateStruct and CreateArray") {
    val structDf = testData2.select("a", "b").as("record")
    // CreateStruct and CreateArray in aggregateExpressions
    assert(structDf.groupBy($"a").agg(min(struct($"record.*"))).
      sort("a").first() == Row(1, Row(1, 1)))
    assert(structDf.groupBy($"a").agg(min(array($"record.*"))).
      sort("a").first() == Row(1, Seq(1, 1)))

    // CreateStruct and CreateArray in project list (unresolved alias)
    assert(structDf.select(struct($"record.*")).first() == Row(Row(1, 1)))
    assert(structDf.select(array($"record.*")).first().getAs[Seq[Int]](0) === Seq(1, 1))

    // CreateStruct and CreateArray in project list (alias)
    assert(structDf.select(struct($"record.*").as("a")).first() == Row(Row(1, 1)))
    assert(structDf.select(array($"record.*").as("a")).first().getAs[Seq[Int]](0) === Seq(1, 1))
  }

  test("Star Expansion - hash") {
    val structDf = testData2.select("a", "b").as("record")
    checkAnswer(
      structDf.groupBy($"a", $"b").agg(min(hash($"a", $"*"))),
      structDf.groupBy($"a", $"b").agg(min(hash($"a", $"a", $"b"))))

    checkAnswer(
      structDf.groupBy($"a", $"b").agg(hash($"a", $"*")),
      structDf.groupBy($"a", $"b").agg(hash($"a", $"a", $"b")))

    checkAnswer(
      structDf.select(hash($"*")),
      structDf.select(hash($"record.*")))

    checkAnswer(
      structDf.select(hash($"a", $"*")),
      structDf.select(hash($"a", $"record.*")))
  }

  test("Star Expansion - xxhash64") {
    val structDf = testData2.select("a", "b").as("record")
    checkAnswer(
      structDf.groupBy($"a", $"b").agg(min(xxhash64($"a", $"*"))),
      structDf.groupBy($"a", $"b").agg(min(xxhash64($"a", $"a", $"b"))))

    checkAnswer(
      structDf.groupBy($"a", $"b").agg(xxhash64($"a", $"*")),
      structDf.groupBy($"a", $"b").agg(xxhash64($"a", $"a", $"b")))

    checkAnswer(
      structDf.select(xxhash64($"*")),
      structDf.select(xxhash64($"record.*")))

    checkAnswer(
      structDf.select(xxhash64($"a", $"*")),
      structDf.select(xxhash64($"a", $"record.*")))
  }

  test("SPARK-28067: sum of null decimal values") {
    Seq("true", "false").foreach { wholeStageEnabled =>
      withSQLConf((SqlApiConf.WHOLESTAGE_CODEGEN_ENABLED_KEY, wholeStageEnabled)) {
        Seq("true", "false").foreach { ansiEnabled =>
          withSQLConf((SqlApiConf.ANSI_ENABLED_KEY, ansiEnabled)) {
            val df = spark.range(1, 4, 1).select(expr(s"cast(null as decimal(38,18)) as d"))
            checkAnswer(df.agg(sum($"d")), Row(null))
          }
        }
      }
    }
  }

  test("Star Expansion - ds.explode should fail with a meaningful message if it takes a star") {
    val df = Seq(("1", "1,2"), ("2", "4"), ("3", "7,8,9")).toDF("prefix", "csv")
    val e = intercept[AnalysisException] {
      df.explode($"*") { case Row(prefix: String, csv: String) =>
        csv.split(",").map(v => Tuple1(prefix + ":" + v)).toSeq
      }.queryExecution.assertAnalyzed()
    }
    assert(e.getMessage.contains("Invalid usage of '*' in explode/json_tuple/UDTF"))

    checkAnswer(
      df.explode($"prefix", $"csv") { case Row(prefix: String, csv: String) =>
        csv.split(",").map(v => Tuple1(prefix + ":" + v)).toSeq
      },
      Row("1", "1,2", "1:1") ::
        Row("1", "1,2", "1:2") ::
        Row("2", "4", "2:4") ::
        Row("3", "7,8,9", "3:7") ::
        Row("3", "7,8,9", "3:8") ::
        Row("3", "7,8,9", "3:9") :: Nil)
  }

  test("Star Expansion - explode should fail with a meaningful message if it takes a star") {
    val df = Seq(("1,2"), ("4"), ("7,8,9")).toDF("csv")
    checkError(
      exception = intercept[AnalysisException] {
        df.select(explode($"*"))
      },
      errorClass = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      parameters = Map(
        "sqlExpr" -> "\"explode(csv)\"",
        "paramIndex" -> "first",
        "inputSql"-> "\"csv\"",
        "inputType" -> "\"STRING\"",
        "requiredType" -> "(\"ARRAY\" or \"MAP\")"),
      context = ExpectedContext(fragment = "explode", getCurrentClassCallSitePattern)
    )

    val df2 = Seq(Array("1", "2"), Array("4"), Array("7", "8", "9")).toDF("csv")
    checkAnswer(
      df2.select(explode($"*")),
      Row("1") :: Row("2") :: Row("4") :: Row("7") :: Row("8") :: Row("9") :: Nil)
  }

  test("explode on output of array-valued function") {
    val df = Seq(("1,2"), ("4"), ("7,8,9")).toDF("csv")
    checkAnswer(
      df.select(explode(split($"csv", pattern = ","))),
      Row("1") :: Row("2") :: Row("4") :: Row("7") :: Row("8") :: Row("9") :: Nil)
  }

  test("Star Expansion - explode alias and star") {
    val df = Seq((Array("a"), 1)).toDF("a", "b")

    checkAnswer(
      df.select(explode($"a").as("a"), $"*"),
      Row("a", Seq("a"), 1) :: Nil)
  }

  test("sort after generate with join=true") {
    val df = Seq((Array("a"), 1)).toDF("a", "b")

    checkAnswer(
      df.select($"*", explode($"a").as("c")).sortWithinPartitions("b", "c"),
      Row(Seq("a"), 1, "a") :: Nil)
  }

  test("selectExpr") {
    checkAnswer(
      testData.selectExpr("abs(key)", "value"),
      testData.collect().map(row => Row(math.abs(row.getInt(0)), row.getString(1))).toSeq)
  }

  test("selectExpr with alias") {
    checkAnswer(
      testData.selectExpr("key as k").select("k"),
      testData.select("key").collect().toSeq)
  }

  test("selectExpr with udtf") {
    val df = Seq((Map("1" -> 1), 1)).toDF("a", "b")
    checkAnswer(
      df.selectExpr("explode(a)"),
      Row("1", 1) :: Nil)
  }

  test("filterExpr") {
    val res = testData.collect().filter(_.getInt(0) > 90).toSeq
    checkAnswer(testData.filter("key > 90"), res)
    checkAnswer(testData.filter("key > 9.0e1"), res)
    checkAnswer(testData.filter("key > .9e+2"), res)
    checkAnswer(testData.filter("key > 0.9e+2"), res)
    checkAnswer(testData.filter("key > 900e-1"), res)
    checkAnswer(testData.filter("key > 900.0E-1"), res)
    checkAnswer(testData.filter("key > 9.e+1"), res)
  }

  test("filterExpr using where") {
    checkAnswer(
      testData.where("key > 50"),
      testData.collect().filter(_.getInt(0) > 50).toSeq)
  }

  test("repartition") {
    intercept[IllegalArgumentException] {
      testData.select("key").repartition(0)
    }

    checkAnswer(
      testData.select("key").repartition(10).select("key"),
      testData.select("key").collect().toSeq)
  }

  test("repartition with SortOrder") {
    // passing SortOrder expressions to .repartition() should result in an informative error

    def checkSortOrderErrorMsg[T](data: => Dataset[T]): Unit = {
      val ex = intercept[IllegalArgumentException](data)
      assert(ex.getMessage.contains("repartitionByRange"))
    }

    checkSortOrderErrorMsg {
      Seq(0).toDF("a").repartition(2, $"a".asc)
    }

    checkSortOrderErrorMsg {
      Seq((0, 0)).toDF("a", "b").repartition(2, $"a".asc, $"b")
    }
  }

  test("repartitionByRange") {
    val data1d = Random.shuffle(0.to(9))
    val data2d = data1d.map(i => (i, data1d.size - i))

    checkAnswer(
      data1d.toDF("val").repartitionByRange(data1d.size, $"val".asc)
        .select(spark_partition_id().as("id"), $"val"),
      data1d.map(i => Row(i, i)))

    checkAnswer(
      data1d.toDF("val").repartitionByRange(data1d.size, $"val".desc)
        .select(spark_partition_id().as("id"), $"val"),
      data1d.map(i => Row(i, data1d.size - 1 - i)))

    checkAnswer(
      data1d.toDF("val").repartitionByRange(data1d.size, lit(42))
        .select(spark_partition_id().as("id"), $"val"),
      data1d.map(i => Row(0, i)))

    checkAnswer(
      data1d.toDF("val").repartitionByRange(data1d.size, lit(null), $"val".asc, rand())
        .select(spark_partition_id().as("id"), $"val"),
      data1d.map(i => Row(i, i)))

    // .repartitionByRange() assumes .asc by default if no explicit sort order is specified
    checkAnswer(
      data2d.toDF("a", "b").repartitionByRange(data2d.size, $"a".desc, $"b")
        .select(spark_partition_id().as("id"), $"a", $"b"),
      data2d.toDF("a", "b").repartitionByRange(data2d.size, $"a".desc, $"b".asc)
        .select(spark_partition_id().as("id"), $"a", $"b"))

    // at least one partition-by expression must be specified
    intercept[IllegalArgumentException] {
      data1d.toDF("val").repartitionByRange(data1d.size)
    }
    intercept[IllegalArgumentException] {
      data1d.toDF("val").repartitionByRange(data1d.size, Seq.empty: _*)
    }
  }

  test("coalesce") {
    intercept[IllegalArgumentException] {
      testData.select("key").coalesce(0)
    }

    assert(testData.select("key").coalesce(1).rdd.partitions.length === 1)

    checkAnswer(
      testData.select("key").coalesce(1).select("key"),
      testData.select("key").collect().toSeq)

    assert(spark.emptyDataFrame.coalesce(1).rdd.partitions.length === 1)
  }

  test("convert $\"attribute name\" into unresolved attribute") {
    checkAnswer(
      testData.where($"key" === lit(1)).select($"value"),
      Row("1"))
  }

  test("convert Scala Symbol 'attrname into unresolved attribute") {
    checkAnswer(
      testData.where($"key" === lit(1)).select("value"),
      Row("1"))
  }

  test("select *") {
    checkAnswer(
      testData.select($"*"),
      testData.collect().toSeq)
  }

  test("simple select") {
    checkAnswer(
      testData.where($"key" === lit(1)).select("value"),
      Row("1"))
  }

  test("select with functions") {
    checkAnswer(
      testData.select(sum("value"), avg("value"), count(lit(1))),
      Row(5050.0, 50.5, 100))

    checkAnswer(
      testData2.select($"a" + $"b", $"a" < $"b"),
      Seq(
        Row(2, false),
        Row(3, true),
        Row(3, false),
        Row(4, false),
        Row(4, false),
        Row(5, false)))

    checkAnswer(
      testData2.select(sum_distinct($"a")),
      Row(6))
  }

  test("sorting with null ordering") {
    val data = Seq[java.lang.Integer](2, 1, null).toDF("key")

    checkAnswer(data.orderBy($"key".asc), Row(null) :: Row(1) :: Row(2) :: Nil)
    checkAnswer(data.orderBy(asc("key")), Row(null) :: Row(1) :: Row(2) :: Nil)
    checkAnswer(data.orderBy($"key".asc_nulls_first), Row(null) :: Row(1) :: Row(2) :: Nil)
    checkAnswer(data.orderBy(asc_nulls_first("key")), Row(null) :: Row(1) :: Row(2) :: Nil)
    checkAnswer(data.orderBy($"key".asc_nulls_last), Row(1) :: Row(2) :: Row(null) :: Nil)
    checkAnswer(data.orderBy(asc_nulls_last("key")), Row(1) :: Row(2) :: Row(null) :: Nil)

    checkAnswer(data.orderBy($"key".desc), Row(2) :: Row(1) :: Row(null) :: Nil)
    checkAnswer(data.orderBy(desc("key")), Row(2) :: Row(1) :: Row(null) :: Nil)
    checkAnswer(data.orderBy($"key".desc_nulls_first), Row(null) :: Row(2) :: Row(1) :: Nil)
    checkAnswer(data.orderBy(desc_nulls_first("key")), Row(null) :: Row(2) :: Row(1) :: Nil)
    checkAnswer(data.orderBy($"key".desc_nulls_last), Row(2) :: Row(1) :: Row(null) :: Nil)
    checkAnswer(data.orderBy(desc_nulls_last("key")), Row(2) :: Row(1) :: Row(null) :: Nil)
  }

  test("global sorting") {
    checkAnswer(
      testData2.orderBy($"a".asc, $"b".asc),
      Seq(Row(1, 1), Row(1, 2), Row(2, 1), Row(2, 2), Row(3, 1), Row(3, 2)))

    checkAnswer(
      testData2.orderBy(asc("a"), desc("b")),
      Seq(Row(1, 2), Row(1, 1), Row(2, 2), Row(2, 1), Row(3, 2), Row(3, 1)))

    checkAnswer(
      testData2.orderBy($"a".asc, $"b".desc),
      Seq(Row(1, 2), Row(1, 1), Row(2, 2), Row(2, 1), Row(3, 2), Row(3, 1)))

    checkAnswer(
      testData2.orderBy($"a".desc, $"b".desc),
      Seq(Row(3, 2), Row(3, 1), Row(2, 2), Row(2, 1), Row(1, 2), Row(1, 1)))

    checkAnswer(
      testData2.orderBy($"a".desc, $"b".asc),
      Seq(Row(3, 1), Row(3, 2), Row(2, 1), Row(2, 2), Row(1, 1), Row(1, 2)))
  }

  test("limit") {
    checkAnswer(
      testData.limit(10),
      testData.take(10).toSeq)
  }

  test("offset") {
    checkAnswer(
      testData.offset(90),
      testData.collect().drop(90).toSeq)
  }

  test("limit with offset") {
    checkAnswer(
      testData.limit(10).offset(5),
      testData.take(10).drop(5).toSeq)

    checkAnswer(
      testData.offset(5).limit(10),
      testData.take(15).drop(5).toSeq)
  }

  test("withColumn") {
    val df = testData.toDF().withColumn("newCol", col("key") + 1)
    checkAnswer(
      df,
      testData.collect().map { case Row(key: Int, value: String) =>
        Row(key, value, key + 1)
      }.toSeq)
    assert(df.schema.map(_.name) === Seq("key", "value", "newCol"))
  }

  test("withColumns: public API, with Map input") {
    val df = testData.toDF().withColumns(Map(
      "newCol1" -> (col("key") + 1), "newCol2" -> (col("key")  + 2)
    ))
    checkAnswer(
      df,
      testData.collect().map { case Row(key: Int, value: String) =>
        Row(key, value, key + 1, key + 2)
      }.toSeq)
    assert(df.schema.map(_.name) === Seq("key", "value", "newCol1", "newCol2"))
  }

  test("withColumns: internal method") {
    val df = testData.toDF().withColumns(Seq("newCol1", "newCol2"),
      Seq(col("key") + 1, col("key") + 2))
    checkAnswer(
      df,
      testData.collect().map { case Row(key: Int, value: String) =>
        Row(key, value, key + 1, key + 2)
      }.toSeq)
    assert(df.schema.map(_.name) === Seq("key", "value", "newCol1", "newCol2"))

    val err = intercept[IllegalArgumentException] {
      testData.toDF().withColumns(Seq("newCol1"),
        Seq(col("key") + 1, col("key") + 2))
    }
    assert(
      err.getMessage.contains("The size of column names: 1 isn't equal to the size of columns: 2"))

    checkError(
      exception = intercept[AnalysisException] {
        testData.toDF().withColumns(Seq("newCol1", "newCOL1"),
          Seq(col("key") + 1, col("key") + 2))
      },
      errorClass = "COLUMN_ALREADY_EXISTS",
      parameters = Map("columnName" -> "`newcol1`"))
  }

  test("withColumns: internal method, case sensitive") {
    withSQLConf(SqlApiConf.CASE_SENSITIVE_KEY -> "true") {
      val df = testData.toDF().withColumns(Seq("newCol1", "newCOL1"),
        Seq(col("key") + 1, col("key") + 2))
      checkAnswer(
        df,
        testData.collect().map { case Row(key: Int, value: String) =>
          Row(key, value, key + 1, key + 2)
        }.toSeq)
      assert(df.schema.map(_.name) === Seq("key", "value", "newCol1", "newCOL1"))

      checkError(
        exception = intercept[AnalysisException] {
          testData.toDF().withColumns(Seq("newCol1", "newCol1"),
            Seq(col("key") + 1, col("key") + 2))
        },
        errorClass = "COLUMN_ALREADY_EXISTS",
        parameters = Map("columnName" -> "`newCol1`"))
    }
  }

  test("withColumns: internal method, given metadata") {
    def buildMetadata(num: Int): Seq[Metadata] = {
      (0 until num).map { n =>
        val builder = new MetadataBuilder
        builder.putLong("key", n.toLong)
        builder.build()
      }
    }

    val df = testData.toDF().withColumns(
      Seq("newCol1", "newCol2"),
      Seq(col("key") + 1, col("key") + 2),
      buildMetadata(2))

    df.select("newCol1", "newCol2").schema.zipWithIndex.foreach { case (col, idx) =>
      assert(col.metadata.getLong("key").toInt === idx)
    }

    val err = intercept[IllegalArgumentException] {
      testData.toDF().withColumns(
        Seq("newCol1", "newCol2"),
        Seq(col("key") + 1, col("key") + 2),
        buildMetadata(1))
    }
    assert(err.getMessage.contains(
      "The size of column names: 2 isn't equal to the size of metadata elements: 1"))
  }

  test("drop column using drop") {
    val df = testData.drop("key")
    checkAnswer(
      df,
      testData.collect().map(x => Row(x.getString(1))).toSeq)
    assert(df.schema.map(_.name) === Seq("value"))
  }

  test("drop columns using drop") {
    val src = Seq((0, 2, 3)).toDF("a", "b", "c")
    val df = src.drop("a", "b")
    checkAnswer(df, Row(3))
    assert(df.schema.map(_.name) === Seq("c"))
  }

  test("drop unknown column (no-op)") {
    val df = testData.drop("random")
    checkAnswer(
      df,
      testData.collect().toSeq)
    assert(df.schema.map(_.name) === Seq("key", "value"))
  }

  test("drop column using drop with column reference") {
    val col = testData("key")
    val df = testData.drop(col)
    checkAnswer(
      df,
      testData.collect().map(x => Row(x.getString(1))).toSeq)
    assert(df.schema.map(_.name) === Seq("value"))
  }

  test("SPARK-28189 drop column using drop with column reference with case-insensitive names") {
    // With SQL config caseSensitive OFF, case insensitive column name should work
    withSQLConf(SqlApiConf.CASE_SENSITIVE_KEY -> "false") {
      val col1 = testData("KEY")
      val df1 = testData.drop(col1)
      checkAnswer(df1, testData.selectExpr("value"))
      assert(df1.schema.map(_.name) === Seq("value"))

      val col2 = testData("Key")
      val df2 = testData.drop(col2)
      checkAnswer(df2, testData.selectExpr("value"))
      assert(df2.schema.map(_.name) === Seq("value"))
    }
  }

  test("drop unknown column (no-op) with column reference") {
    val col = Column("random")
    val df = testData.drop(col)
    checkAnswer(
      df,
      testData.collect().toSeq)
    assert(df.schema.map(_.name) === Seq("key", "value"))
  }

  test("SPARK-39895: drop two column references") {
    val col = Column("key")
    val randomCol = Column("random")
    val df = testData.drop(col, randomCol)
    checkAnswer(
      df,
      testData.collect().map(x => Row(x.getString(1))).toSeq)
    assert(df.schema.map(_.name) === Seq("value"))
  }

  test("drop unknown column with same name with column reference") {
    val col = Column("key")
    val df = testData.drop(col)
    checkAnswer(
      df,
      testData.collect().map(x => Row(x.getString(1))).toSeq)
    assert(df.schema.map(_.name) === Seq("value"))
  }

  test("drop column after join with duplicate columns using column reference") {
    val newSalary = salary.withColumnRenamed("personId", "id")
    val col = newSalary("id")
    // this join will result in duplicate "id" columns
    val joinedDf = person.join(newSalary,
      person("id") === newSalary("id"), "inner")
    // remove only the "id" column that was associated with newSalary
    val df = joinedDf.drop(col)
    checkAnswer(
      df,
      joinedDf.collect().map {
        case Row(id: Int, name: String, age: Int, idToDrop: Int, salary: Double) =>
          Row(id, name, age, salary)
      }.toSeq)
    assert(df.schema.map(_.name) === Seq("id", "name", "age", "salary"))
    assert(df("id") == person("id"))
  }

  test("drop top level columns that contains dot") {
    val df1 = Seq((1, 2)).toDF("a.b", "a.c")
    checkAnswer(df1.drop("a.b"), Row(2))

    // Creates data set: {"a.b": 1, "a": {"b": 3}}
    val df2 = Seq((1)).toDF("a.b").withColumn("a", struct(lit(3) as "b"))
    // Not like select(), drop() parses the column name "a.b" literally without interpreting "."
    checkAnswer(df2.drop("a.b").select("a.b"), Row(3))

    // "`" is treated as a normal char here with no interpreting, "`a`b" is a valid column name.
    assert(df2.drop("`a.b`").columns.length == 2)
  }

  test("drop(name: String) search and drop all top level columns that matches the name") {
    val df1 = Seq((1, 2)).toDF("a", "b")
    val df2 = Seq((3, 4)).toDF("a", "b")
    checkAnswer(df1.crossJoin(df2), Row(1, 2, 3, 4))
    // Finds and drops all columns that match the name (case insensitive).
    checkAnswer(df1.crossJoin(df2).drop("A"), Row(2, 4))
  }

  test("withColumnRenamed") {
    val df = testData.toDF().withColumn("newCol", col("key") + 1)
      .withColumnRenamed("value", "valueRenamed")
    checkAnswer(
      df,
      testData.collect().map { case Row(key: Int, value: String) =>
        Row(key, value, key + 1)
      }.toSeq)
    assert(df.schema.map(_.name) === Seq("key", "valueRenamed", "newCol"))
  }

  test("SPARK-40311: withColumnsRenamed") {
      val df = testData.toDF().withColumns(Seq("newCol1", "newCOL2"),
        Seq(col("key") + 1, col("key") + 2))
        .withColumnsRenamed(Map("newCol1" -> "renamed1", "newCol2" -> "renamed2"))
      checkAnswer(
        df,
        testData.collect().map { case Row(key: Int, value: String) =>
          Row(key, value, key + 1, key + 2)
        }.toSeq)
      assert(df.columns === Array("key", "value", "renamed1", "renamed2"))
  }

  test("SPARK-46260: withColumnsRenamed should respect the Map ordering") {
    val df = spark.range(10).toDF()
    assert(df.withColumnsRenamed(ListMap("id" -> "a", "a" -> "b")).columns === Array("b"))
    assert(df.withColumnsRenamed(ListMap("a" -> "b", "id" -> "a")).columns === Array("a"))
  }

  test("SPARK-20384: Value class filter") {
    val df = spark.sparkContext
      .parallelize(Seq(StringWrapper("a"), StringWrapper("b"), StringWrapper("c")))
      .toDF()
    val filtered = df.where("s = \"a\"")
    checkAnswer(filtered, spark.sparkContext.parallelize(Seq(StringWrapper("a"))).toDF())
  }

  test("SPARK-20384: Tuple2 of value class filter") {
    val df = spark.sparkContext
      .parallelize(Seq(
        (StringWrapper("a1"), StringWrapper("a2")),
        (StringWrapper("b1"), StringWrapper("b2"))))
      .toDF()
    val filtered = df.where("_2.s = \"a2\"")
    checkAnswer(filtered,
      spark.sparkContext.parallelize(Seq((StringWrapper("a1"), StringWrapper("a2")))).toDF())
  }

  test("SPARK-20384: Tuple3 of value class filter") {
    val df = spark.sparkContext
      .parallelize(Seq(
        (StringWrapper("a1"), StringWrapper("a2"), StringWrapper("a3")),
        (StringWrapper("b1"), StringWrapper("b2"), StringWrapper("b3"))))
      .toDF()
    val filtered = df.where("_3.s = \"a3\"")
    checkAnswer(filtered,
      spark.sparkContext.parallelize(
        Seq((StringWrapper("a1"), StringWrapper("a2"), StringWrapper("a3")))).toDF())
  }

  test("SPARK-20384: Array value class filter") {
    val ab = ArrayStringWrapper(Seq(StringWrapper("a"), StringWrapper("b")))
    val cd = ArrayStringWrapper(Seq(StringWrapper("c"), StringWrapper("d")))

    val df = spark.sparkContext.parallelize(Seq(ab, cd)).toDF()
    val filtered = df.where(array_contains(col("wrappers.s"), "b"))
    checkAnswer(filtered, spark.sparkContext.parallelize(Seq(ab)).toDF())
  }

  test("SPARK-20384: Nested value class filter") {
    val a = ContainerStringWrapper(StringWrapper("a"))
    val b = ContainerStringWrapper(StringWrapper("b"))

    val df = spark.sparkContext.parallelize(Seq(a, b)).toDF()
    // flat value class, `s` field is not in schema
    val filtered = df.where("wrapper = \"a\"")
    checkAnswer(filtered, spark.sparkContext.parallelize(Seq(a)).toDF())
  }

  private lazy val person2: DataFrame = Seq(
    ("Bob", 16, 176),
    ("Alice", 32, 164),
    ("David", 60, 192),
    ("Amy", 24, 180)).toDF("name", "age", "height")

  test("describe") {
    val describeResult = Seq(
      Row("count", "4", "4", "4"),
      Row("mean", null, "33.0", "178.0"),
      Row("stddev", null, "19.148542155126762", "11.547005383792516"),
      Row("min", "Alice", "16", "164"),
      Row("max", "David", "60", "192"))

    val emptyDescribeResult = Seq(
      Row("count", "0", "0", "0"),
      Row("mean", null, null, null),
      Row("stddev", null, null, null),
      Row("min", null, null, null),
      Row("max", null, null, null))

    def getSchemaAsSeq(df: DataFrame): Seq[String] = df.schema.map(_.name)

    Seq("true", "false").foreach { ansiEnabled =>
      withSQLConf(SqlApiConf.ANSI_ENABLED_KEY -> ansiEnabled) {
        val describeAllCols = person2.describe()
        assert(getSchemaAsSeq(describeAllCols) === Seq("summary", "name", "age", "height"))
        checkAnswer(describeAllCols, describeResult)
        // All aggregate value should have been cast to string
        describeAllCols.collect().foreach { row =>
          row.toSeq.foreach { value =>
            if (value != null) {
              assert(value.isInstanceOf[String], "expected string but found " + value.getClass)
            }
          }
        }

        val describeOneCol = person2.describe("age")
        assert(getSchemaAsSeq(describeOneCol) === Seq("summary", "age"))
        checkAnswer(describeOneCol, describeResult.map { case Row(s, _, d, _) => Row(s, d) })

        val describeNoCol = person2.select().describe()
        assert(getSchemaAsSeq(describeNoCol) === Seq("summary"))
        checkAnswer(describeNoCol, describeResult.map { case Row(s, _, _, _) => Row(s) })

        val emptyDescription = person2.limit(0).describe()
        assert(getSchemaAsSeq(emptyDescription) === Seq("summary", "name", "age", "height"))
        checkAnswer(emptyDescription, emptyDescribeResult)
      }
    }
  }

  test("summary") {
    val summaryResult = Seq(
      Row("count", "4", "4", "4"),
      Row("mean", null, "33.0", "178.0"),
      Row("stddev", null, "19.148542155126762", "11.547005383792516"),
      Row("min", "Alice", "16", "164"),
      Row("25%", null, "16", "164"),
      Row("50%", null, "24", "176"),
      Row("75%", null, "32", "180"),
      Row("max", "David", "60", "192"))

    val emptySummaryResult = Seq(
      Row("count", "0", "0", "0"),
      Row("mean", null, null, null),
      Row("stddev", null, null, null),
      Row("min", null, null, null),
      Row("25%", null, null, null),
      Row("50%", null, null, null),
      Row("75%", null, null, null),
      Row("max", null, null, null))

    def getSchemaAsSeq(df: DataFrame): Seq[String] = df.schema.map(_.name)

    Seq("true", "false").foreach { ansiEnabled =>
      withSQLConf(SqlApiConf.ANSI_ENABLED_KEY -> ansiEnabled) {
        val summaryAllCols = person2.summary()

        assert(getSchemaAsSeq(summaryAllCols) === Seq("summary", "name", "age", "height"))
        checkAnswer(summaryAllCols, summaryResult)
        // All aggregate value should have been cast to string
        summaryAllCols.collect().foreach { row =>
          row.toSeq.foreach { value =>
            if (value != null) {
              assert(value.isInstanceOf[String], "expected string but found " + value.getClass)
            }
          }
        }

        val summaryOneCol = person2.select("age").summary()
        assert(getSchemaAsSeq(summaryOneCol) === Seq("summary", "age"))
        checkAnswer(summaryOneCol, summaryResult.map { case Row(s, _, d, _) => Row(s, d) })

        val summaryNoCol = person2.select().summary()
        assert(getSchemaAsSeq(summaryNoCol) === Seq("summary"))
        checkAnswer(summaryNoCol, summaryResult.map { case Row(s, _, _, _) => Row(s) })

        val emptyDescription = person2.limit(0).summary()
        assert(getSchemaAsSeq(emptyDescription) === Seq("summary", "name", "age", "height"))
        checkAnswer(emptyDescription, emptySummaryResult)
      }
    }
  }

  test("SPARK-41391: Correct the output column name of groupBy.agg(count_distinct)") {
    withTempView("person") {
      person.createOrReplaceTempView("person")
      val df1 = person.groupBy("id").agg(count_distinct(col("name")))
      val df2 = spark.sql("SELECT id, COUNT(DISTINCT name) FROM person GROUP BY id")
      assert(df1.columns === df2.columns)
      val df3 = person.groupBy("id").agg(count_distinct(col("*")))
      val df4 = spark.sql("SELECT id, COUNT(DISTINCT *) FROM person GROUP BY id")
      assert(df3.columns === df4.columns)
    }
  }

  test("apply on query results (SPARK-5462)") {
    val df = testData.sparkSession.sql("select key from testData")
    checkAnswer(df.select(df("key")), testData.select("key").collect().toSeq)
  }

  test("inputFiles") {
    Seq("csv", "").foreach { useV1List =>
      withSQLConf(SqlApiConf.USE_V1_SOURCE_LIST_KEY -> useV1List) {
        withTempDir { dir =>
          val df = Seq((1, 22)).toDF("a", "b")

          val parquetDir = new File(dir, "parquet").getCanonicalPath
          df.write.parquet(parquetDir)
          val parquetDF = spark.read.parquet(parquetDir)
          assert(parquetDF.inputFiles.nonEmpty)

          val csvDir = new File(dir, "csv").getCanonicalPath
          df.write.json(csvDir)
          val csvDF = spark.read.json(csvDir)
          assert(csvDF.inputFiles.nonEmpty)

          val unioned = csvDF.union(parquetDF).inputFiles.sorted
          val allFiles = (csvDF.inputFiles ++ parquetDF.inputFiles).distinct.sorted
          assert(unioned === allFiles)
        }
      }
    }
  }

  test("getRows: numRows = 0") {
    val expectedAnswer = Seq(Seq("key", "value"), Seq("1", "1"))
    assert(testData.select($"*").getRows(0, 20) === expectedAnswer)
  }

  test("getRows: array") {
    val df = Seq(
      (Array(1, 2, 3), Array(1, 2, 3)),
      (Array(2, 3, 4), Array(2, 3, 4))
    ).toDF()
    val expectedAnswer = Seq(
      Seq("_1", "_2"),
      Seq("[1, 2, 3]", "[1, 2, 3]"),
      Seq("[2, 3, 4]", "[2, 3, 4]"))
    assert(df.getRows(10, 20) === expectedAnswer)
  }

  test("getRows: binary") {
    val df = Seq(
      ("12".getBytes(StandardCharsets.UTF_8), "ABC.".getBytes(StandardCharsets.UTF_8)),
      ("34".getBytes(StandardCharsets.UTF_8), "12346".getBytes(StandardCharsets.UTF_8))
    ).toDF()

    withSQLConf(SqlApiConf.BINARY_OUTPUT_STYLE_KEY -> "HEX_DISCRETE") {
      val expectedAnswer = Seq(
        Seq("_1", "_2"),
        Seq("[31 32]", "[41 42 43 2E]"),
        Seq("[33 34]", "[31 32 33 34 36]"))
      assert(df.getRows(10, 20) === expectedAnswer)
    }
    withSQLConf(SqlApiConf.BINARY_OUTPUT_STYLE_KEY -> "HEX") {
      val expectedAnswer = Seq(
        Seq("_1", "_2"),
        Seq("3132", "4142432E"),
        Seq("3334", "3132333436")
      )
      assert(df.getRows(10, 20) === expectedAnswer)
    }
    withSQLConf(SqlApiConf.BINARY_OUTPUT_STYLE_KEY -> "BASE64") {
      val expectedAnswer = Seq(
        Seq("_1", "_2"),
        Seq("MTI", "QUJDLg"),
        Seq("MzQ", "MTIzNDY")
      )
      assert(df.getRows(10, 20) === expectedAnswer)
    }
    withSQLConf(SqlApiConf.BINARY_OUTPUT_STYLE_KEY -> "UTF8") {
      val expectedAnswer = Seq(
        Seq("_1", "_2"),
        Seq("12", "ABC."),
        Seq("34", "12346")
      )
      assert(df.getRows(10, 20) === expectedAnswer)
    }
    withSQLConf(SqlApiConf.BINARY_OUTPUT_STYLE_KEY -> "BASIC") {
      val expectedAnswer = Seq(
        Seq("_1", "_2"),
        Seq("[49, 50]", "[65, 66, 67, 46]"),
        Seq("[51, 52]", "[49, 50, 51, 52, 54]")
      )
      assert(df.getRows(10, 20) === expectedAnswer)
    }
  }

  test("SPARK-6899: type should match when using codegen") {
    checkAnswer(decimalData.agg(avg("a")), Row(new java.math.BigDecimal(2)))
  }

  test("SPARK-7551: support backticks for DataFrame attribute resolution") {
    withSQLConf(SqlApiConf.SUPPORT_QUOTED_REGEX_COLUMN_NAME_KEY -> "false") {
      val df = spark.read.json(Seq("""{"a.b": {"c": {"d..e": {"f": 1}}}}""").toDS())
      checkAnswer(
        df.select(df("`a.b`.c.`d..e`.`f`")),
        Row(1)
      )

      val df2 = spark.read.json(Seq("""{"a  b": {"c": {"d  e": {"f": 1}}}}""").toDS())
      checkAnswer(
        df2.select(df2("`a  b`.c.d  e.f")),
        Row(1)
      )

      def checkSyntaxError(name: String): Unit = {
        checkError(
          exception = intercept[org.apache.spark.sql.AnalysisException] {
            df(name)
          },
          errorClass = "_LEGACY_ERROR_TEMP_1049",
          parameters = Map("name" -> name))
      }

      checkSyntaxError("`abc.`c`")
      checkSyntaxError("`abc`..d")
      checkSyntaxError("`a`.b.")
      checkSyntaxError("`a.b`.c.`d")
    }
  }

  test("SPARK-8621: support empty string column name") {
    val df = Seq(Tuple1(1)).toDF("").as("t")
    // We should allow empty string as column name
    df.col("")
    df.col("t.``")
  }

  test("SPARK-8797: sort by float column containing NaN should not crash") {
    val inputData = Seq.fill(10)(Tuple1(Float.NaN)) ++ (1 to 1000).map(x => Tuple1(x.toFloat))
    val df = Random.shuffle(inputData).toDF("a")
    df.orderBy("a").collect()
  }

  test("SPARK-8797: sort by double column containing NaN should not crash") {
    val inputData = Seq.fill(10)(Tuple1(Double.NaN)) ++ (1 to 1000).map(x => Tuple1(x.toDouble))
    val df = Random.shuffle(inputData).toDF("a")
    df.orderBy("a").collect()
  }

  test("NaN is greater than all other non-NaN numeric values") {
    val maxDouble = Seq(Double.NaN, Double.PositiveInfinity, Double.MaxValue)
      .map(Tuple1.apply).toDF("a").selectExpr("max(a)").first()
    assert(java.lang.Double.isNaN(maxDouble.getDouble(0)))
    val maxFloat = Seq(Float.NaN, Float.PositiveInfinity, Float.MaxValue)
      .map(Tuple1.apply).toDF("a").selectExpr("max(a)").first()
    assert(java.lang.Float.isNaN(maxFloat.getFloat(0)))
  }

  test("SPARK-8072: Better Exception for Duplicate Columns") {
    // only one duplicate column present
    val e = intercept[AnalysisException] {
      Seq((1, 2, 3), (2, 3, 4), (3, 4, 5)).toDF("column1", "column2", "column1")
        .write.format("parquet").save("temp")
    }
    checkError(
      exception = e,
      errorClass = "COLUMN_ALREADY_EXISTS",
      parameters = Map("columnName" -> "`column1`"))

    // multiple duplicate columns present
    val f = intercept[AnalysisException] {
      Seq((1, 2, 3, 4, 5), (2, 3, 4, 5, 6), (3, 4, 5, 6, 7))
        .toDF("column1", "column2", "column3", "column1", "column3")
        .write.format("json").save("temp")
    }
    checkError(
      exception = f,
      errorClass = "COLUMN_ALREADY_EXISTS",
      parameters = Map("columnName" -> "`column1`"))
  }

  test("SPARK-6941: Better error message for inserting into RDD-based Table") {
    withTempDir { dir =>
      withTempView("parquet_base", "json_base", "rdd_base", "indirect_ds", "one_row") {
        val tempParquetFile = new File(dir, "tmp_parquet")
        val tempJsonFile = new File(dir, "tmp_json")

        val df = Seq(Tuple1(1)).toDF()
        val insertion = Seq(Tuple1(2)).toDF("col")

        // pass case: parquet table (HadoopFsRelation)
        df.write.mode(SaveMode.Overwrite).parquet(tempParquetFile.getCanonicalPath)
        val pdf = spark.read.parquet(tempParquetFile.getCanonicalPath)
        pdf.createOrReplaceTempView("parquet_base")

        insertion.write.insertInto("parquet_base")

        // pass case: json table (InsertableRelation)
        df.write.mode(SaveMode.Overwrite).json(tempJsonFile.getCanonicalPath)
        val jdf = spark.read.json(tempJsonFile.getCanonicalPath)
        jdf.createOrReplaceTempView("json_base")
        insertion.write.mode(SaveMode.Overwrite).insertInto("json_base")

        // error cases: insert into an RDD
        df.createOrReplaceTempView("rdd_base")
        checkError(
          exception = intercept[AnalysisException] {
            insertion.write.insertInto("rdd_base")
          },
          errorClass = "UNSUPPORTED_INSERT.RDD_BASED",
          parameters = Map.empty
        )

        // error case: insert into a logical plan that is not a LeafNode
        val indirectDS = pdf.select("_1").filter($"_1" > 5)
        indirectDS.createOrReplaceTempView("indirect_ds")
        checkError(
          exception = intercept[AnalysisException] {
            insertion.write.insertInto("indirect_ds")
          },
          errorClass = "UNSUPPORTED_INSERT.RDD_BASED",
          parameters = Map.empty
        )
      }
    }
  }

  test("SPARK-8609: local DataFrame with random columns should return same value after sort") {
    checkAnswer(testData.sort(rand(33)), testData.sort(rand(33)))

    // We will reuse the same Expression object for LocalRelation.
    val df = (1 to 10).map(Tuple1.apply).toDF()
    checkAnswer(df.sort(rand(33)), df.sort(rand(33)))
  }

  test("SPARK-9083: sort with non-deterministic expressions") {
    val seed = 33
    val df = (1 to 100).map(Tuple1.apply).toDF("i").repartition(1)
    val expected = df.select($"i", rand(seed)).as[(Long, Double)].collect().sortBy(_._2).map(_._1)
    val actual = df.sort(rand(seed)).collect().map(_.getInt(0))
    assert(expected === actual)
  }

  test("Sorting columns are not in Filter and Project") {
    checkAnswer(
      upperCaseData.filter($"N" > 1).select("N").filter($"N" < 6).orderBy($"L".asc),
      Row(2) :: Row(3) :: Row(4) :: Row(5) :: Nil)
  }

  test("SPARK-9323: DataFrame.orderBy should support nested column name") {
    val df = spark.read.json(Seq("""{"a": {"b": 1}}""").toDS())
    checkAnswer(df.orderBy("a.b"), Row(Row(1)))
  }

  test("SPARK-9950: correctly analyze grouping/aggregating on struct fields") {
    val df = Seq(("x", (1, 1)), ("y", (2, 2))).toDF("a", "b")
    checkAnswer(df.groupBy("b._1").agg(sum("b._2")), Row(1, 1) :: Row(2, 2) :: Nil)
  }

  test("SPARK-10093: Avoid transformations on executors") {
    val df = Seq((1, 1)).toDF("a", "b")
    df.where($"a" === 1)
      .select($"a", $"b", struct($"b"))
      .orderBy("a")
      .select(struct($"b"))
      .collect()
  }

  test("SPARK-10185: Read multiple Hadoop Filesystem paths and paths with a comma in it") {
    withTempDir { dir =>
      val df1 = Seq((1, 22)).toDF("a", "b")
      val dir1 = new File(dir, "dir,1").getCanonicalPath
      df1.write.format("json").save(dir1)

      val df2 = Seq((2, 23)).toDF("a", "b")
      val dir2 = new File(dir, "dir2").getCanonicalPath
      df2.write.format("json").save(dir2)

      checkAnswer(spark.read.format("json").load(dir1, dir2),
        Row(1, 22) :: Row(2, 23) :: Nil)

      checkAnswer(spark.read.format("json").load(dir1),
        Row(1, 22) :: Nil)
    }
  }

  test("Alias uses internally generated names 'aggOrder' and 'havingCondition'") {
    val df = Seq(1 -> 2).toDF("i", "j")
    val query1 = df.groupBy("i")
      .agg(max("j").as("aggOrder"))
      .orderBy(sum("j"))
    checkAnswer(query1, Row(1, 2))

    // In the plan, there are two attributes having the same name 'havingCondition'
    // One is a user-provided alias name; another is an internally generated one.
    val query2 = df.groupBy("i")
      .agg(max("j").as("havingCondition"))
      .where(sum("j") > 0)
      .orderBy($"havingCondition".asc)
    checkAnswer(query2, Row(1, 2))
  }

  test("SPARK-10316: respect non-deterministic expressions in PhysicalOperation") {
    withTempDir { dir =>
      (1 to 10).toDF("id").write.mode(SaveMode.Overwrite).json(dir.getCanonicalPath)
      val input = spark.read.json(dir.getCanonicalPath)

      val df = input.select($"id", rand(0).as("r"))
      df.as("a").join(df.filter($"r" < 0.5).as("b"), $"a.id" === $"b.id").collect().foreach { row =>
        assert(row.getDouble(1) - row.getDouble(3) === 0.0 +- 0.001)
      }
    }
  }

  test("SPARK-10743: keep the name of expression if possible when do cast") {
    val df = (1 to 10).map(Tuple1.apply).toDF("i").as("src")
    assert(df.select($"src.i".cast(StringType)).columns.head === "i")
    assert(df.select($"src.i".cast(StringType).cast(IntegerType)).columns.head === "i")
  }

  test("SPARK-11301: fix case sensitivity for filter on partitioned columns") {
    withSQLConf(SqlApiConf.CASE_SENSITIVE_KEY -> "false") {
      withTempPath { path =>
        Seq(2012 -> "a").toDF("year", "val").write.partitionBy("year").parquet(path.getAbsolutePath)
        val df = spark.read.parquet(path.getAbsolutePath)
        checkAnswer(df.filter($"yEAr" > 2000).select($"val"), Row("a"))
      }
    }
  }

  test("fix case sensitivity of partition by") {
    withSQLConf(SqlApiConf.CASE_SENSITIVE_KEY -> "false") {
      withTempPath { path =>
        val p = path.getAbsolutePath
        Seq(2012 -> "a").toDF("year", "val").write.partitionBy("yEAr").parquet(p)
        checkAnswer(spark.read.parquet(p).select("YeaR"), Row(2012))
      }
    }
  }

  test("SPARK-10656: completely support special chars") {
    val df = Seq(1 -> "a").toDF("i_$.a", "d^'a.")
    checkAnswer(df.select(df("*")), Row(1, "a"))
    checkAnswer(df.withColumnRenamed("d^'a.", "a"), Row(1, "a"))
  }

  test("SPARK-12512: support `.` in column name for withColumn()") {
    val df = Seq("a" -> "b").toDF("col.a", "col.b")
    checkAnswer(df.select(df("*")), Row("a", "b"))
    checkAnswer(df.withColumn("col.a", lit("c")), Row("c", "b"))
    checkAnswer(df.withColumn("col.c", lit("c")), Row("a", "b", "c"))
  }

  test("SPARK-12841: cast in filter") {
    checkAnswer(
      Seq(1 -> "a").toDF("i", "j").filter($"i".cast(StringType) === "1"),
      Row(1, "a"))
  }

  test("SPARK-12982: Add table name validation in temp table registration") {
    val df = Seq("foo", "bar").map(Tuple1.apply).toDF("col")
    // invalid table names
    Seq("11111", "t~", "#$@sum", "table!#").foreach { name =>
      withTempView(name) {
        val m = intercept[AnalysisException](df.createOrReplaceTempView(name)).getMessage
        assert(m.contains(s"Invalid view name: $name"))
      }
    }

    // valid table names
    Seq("table1", "`11111`", "`t~`", "`#$@sum`", "`table!#`").foreach { name =>
      withTempView(name) {
        df.createOrReplaceTempView(name)
      }
    }
  }

  test("SPARK-15230: distinct() does not handle column name with dot properly") {
    val df = Seq(1, 1, 2).toDF("column.with.dot")
    checkAnswer(df.distinct(), Row(1) :: Row(2) :: Nil)
  }

  test("copy results for sampling with replacement") {
    val df = Seq((1, 0), (2, 0), (3, 0)).toDF("a", "b")
    val sampleDf = df.sample(true, 2.00)
    val d = sampleDf.withColumn("c", monotonically_increasing_id()).select($"c").collect()
    assert(d.length == d.distinct.length)
  }

  test("SPARK-17897: Fixed IsNotNull Constraint Inference Rule") {
    val data = Seq[java.lang.Integer](1, null).toDF("key")
    checkAnswer(data.filter(!$"key".isNotNull), Row(null))
    checkAnswer(data.filter(!(- $"key").isNotNull), Row(null))
  }

  test("SPARK-17957: outer join + na.fill") {
    withSQLConf(SqlApiConf.SUPPORT_QUOTED_REGEX_COLUMN_NAME_KEY -> "false") {
      val df1 = Seq((1, 2), (2, 3)).toDF("a", "b")
      val df2 = Seq((2, 5), (3, 4)).toDF("a", "c")
      val joinedDf = df1.join(df2, Seq("a"), "outer").na.fill(0)
      val df3 = Seq((3, 1)).toDF("a", "d")
      checkAnswer(joinedDf.join(df3, "a"), Row(3, 0, 4, 1))
    }
  }

  test("SPARK-17913: compare long and string type column may return confusing result") {
    val df = Seq(123L -> "123", 19157170390056973L -> "19157170390056971").toDF("i", "j")
    checkAnswer(df.select($"i" === $"j"), Row(true) :: Row(false) :: Nil)
  }

  // The fix of SPARK-21720 avoid an exception regarding JVM code size limit
  // TODO: When we make a threshold of splitting statements (1024) configurable,
  // we will re-enable this with max threshold to cause an exception
  // See https://github.com/apache/spark/pull/18972/files#r150223463
  ignore("SPARK-19372: Filter can be executed w/o generated code due to JVM code size limit") {
    val N = 400
    val rows = Seq(Row.fromSeq(Seq.fill(N)("string")))
    val schema = StructType(Seq.tabulate(N)(i => StructField(s"_c$i", StringType)))
    val df = spark.createDataFrame(spark.sparkContext.makeRDD(rows), schema)

    val filter = (0 until N)
      .foldLeft(lit(false))((e, index) => e.or(df.col(df.columns(index)) =!= "string"))

    withSQLConf(SqlApiConf.CODEGEN_FALLBACK_KEY -> "true") {
      df.filter(filter).count()
    }

    withSQLConf(SqlApiConf.CODEGEN_FALLBACK_KEY -> "false") {
      val e = intercept[SparkException] {
        df.filter(filter).count()
      }.getMessage
      assert(e.contains("grows beyond 64 KiB"))
    }
  }

  test("order-by ordinal.") {
    checkAnswer(
      testData2.select(lit(7), $"a", $"b").orderBy(lit(1), lit(2), lit(3)),
      Seq(Row(7, 1, 1), Row(7, 1, 2), Row(7, 2, 1), Row(7, 2, 2), Row(7, 3, 1), Row(7, 3, 2)))
  }

  test("SPARK-22271: mean overflows and returns null for some decimal variables") {
    val d = 0.034567890
    val df = Seq(d, d, d, d, d, d, d, d, d, d).toDF("DecimalCol")
    val result = df.select($"DecimalCol" cast DecimalType(38, 33))
      .select(col("DecimalCol")).describe()
    val mean = result.select("DecimalCol").where($"summary" === "mean")
    assert(mean.collect().toSet === Set(Row("0.0345678900000000000000000000000000000")))
  }

  test("SPARK-24781: Using a reference from Dataset in Filter/Sort") {
    val df = Seq(("test1", 0), ("test2", 1)).toDF("name", "id")
    val filter1 = df.select(df("name")).filter(df("id") === 0)
    val filter2 = df.select(col("name")).filter(col("id") === 0)
    checkAnswer(filter1, filter2.collect())

    val sort1 = df.select(df("name")).orderBy(df("id"))
    val sort2 = df.select(col("name")).orderBy(col("id"))
    checkAnswer(sort1, sort2.collect())
  }

  test("SPARK-24781: Using a reference not in aggregation in Filter/Sort") {
     withSQLConf(SqlApiConf.DATAFRAME_RETAIN_GROUP_COLUMNS_KEY -> "false") {
      val df = Seq(("test1", 0), ("test2", 1)).toDF("name", "id")

      val aggPlusSort1 = df.groupBy(df("name")).agg(count(df("name"))).orderBy(df("name"))
      val aggPlusSort2 = df.groupBy(col("name")).agg(count(col("name"))).orderBy(col("name"))
      checkAnswer(aggPlusSort1, aggPlusSort2.collect())

      val aggPlusFilter1 =
        df.groupBy(df("name")).agg(count(df("name"))).filter(df("name") === "test1")
      val aggPlusFilter2 =
        df.groupBy(col("name")).agg(count(col("name"))).filter(col("name") === "test1")
      checkAnswer(aggPlusFilter1, aggPlusFilter2.collect())
    }
  }

  test("SPARK-25816 ResolveReferences works with nested extractors") {
    val df = Seq((1, Map(1 -> "a")), (2, Map(2 -> "b"))).toDF("key", "map")
    val swappedDf = df.select($"key".as("map"), $"map".as("key"))

    checkAnswer(swappedDf.filter($"key"($"map") > "a"), Row(2, Map(2 -> "b")))
  }

  test("SPARK-26057: attribute deduplication on already analyzed plans") {
    withTempView("a", "b", "v") {
      val df1 = Seq(("1-1", 6)).toDF("id", "n")
      df1.createOrReplaceTempView("a")
      val df3 = Seq("1-1").toDF("id")
      df3.createOrReplaceTempView("b")
      spark.sql(
        """
          |SELECT a.id, n as m
          |FROM a
          |WHERE EXISTS(
          |  SELECT 1
          |  FROM b
          |  WHERE b.id = a.id)
        """.stripMargin).createOrReplaceTempView("v")
      val res = spark.sql(
        """
          |SELECT a.id, n, m
          |  FROM a
          |  LEFT OUTER JOIN v ON v.id = a.id
        """.stripMargin)
      checkAnswer(res, Row("1-1", 6, 6))
    }
  }

  test("SPARK-27671: Fix analysis exception when casting null in nested field in struct") {
    val df = sql("SELECT * FROM VALUES (('a', (10, null))), (('b', (10, 50))), " +
      "(('c', null)) AS tab(x, y)")
    checkAnswer(df, Row("a", Row(10, null)) :: Row("b", Row(10, 50)) :: Row("c", null) :: Nil)

    val cast = sql("SELECT cast(struct(1, null) AS struct<a:int,b:int>)")
    checkAnswer(cast, Row(Row(1, null)) :: Nil)
  }

  test("SPARK-27439: Explain result should match collected result after view change") {
    withTempView("test", "test2", "tmp") {
      spark.range(10).createOrReplaceTempView("test")
      spark.range(5).createOrReplaceTempView("test2")
      spark.sql("select * from test").createOrReplaceTempView("tmp")
      val df = spark.sql("select * from tmp")
      spark.sql("select * from test2").createOrReplaceTempView("tmp")

      val captured = new ByteArrayOutputStream()
      Console.withOut(captured) {
        df.explain(extended = true)
      }
      checkAnswer(df, spark.range(10).toDF())
      val output = captured.toString
      assert(output.contains(
        """== Parsed Logical Plan ==
          |'Project [*]
          |+- 'UnresolvedRelation [tmp]""".stripMargin))
      assert(output.contains(
        """== Physical Plan ==
          |*(1) Range (0, 10, step=1, splits=2)""".stripMargin))
    }
  }

  test("SPARK-29442 Set `default` mode should override the existing mode") {
    val df = Seq(Tuple1(1)).toDF()
    val writer = df.write.mode("overwrite").mode("default")
    val modeField = classOf[DataFrameWriter[Tuple1[Int]]].getDeclaredField("mode")
    modeField.setAccessible(true)
    assert(SaveMode.ErrorIfExists === modeField.get(writer).asInstanceOf[SaveMode])
  }

  test("sample should not duplicated the input data") {
    val df1 = spark.range(10).select($"id" as "id1", $"id" % 5 as "key1")
    val df2 = spark.range(10).select($"id" as "id2", $"id" % 5 as "key2")
    val sampled = df1.join(df2, $"key1" === $"key2")
      .sample(0.5, 42)
      .select("id1", "id2")
    val idTuples = sampled.collect().map(row => row.getLong(0) -> row.getLong(1))
    assert(idTuples.length == idTuples.toSet.size)
  }

  test("SPARK-30811: CTE should not cause stack overflow when " +
    "it refers to non-existent table with same name") {
    val e = intercept[AnalysisException] {
      sql("WITH t AS (SELECT 1 FROM nonexist.t) SELECT * FROM t")
    }
    checkErrorTableNotFound(e, "`nonexist`.`t`",
      ExpectedContext("nonexist.t", 25, 34))
  }

  test("SPARK-31750: eliminate UpCast if child's dataType is DecimalType") {
    withTempPath { f =>
      sql("select cast(1 as decimal(38, 0)) as d")
        .write.mode("overwrite")
        .parquet(f.getAbsolutePath)

      val df = spark.read.parquet(f.getAbsolutePath).as[BigDecimal]
      assert(df.schema === new StructType().add(StructField("d", DecimalType(38, 0))))
    }
  }

  test("SPARK-32640: ln(NaN) should return NaN") {
    val df = Seq(Double.NaN).toDF("d")
    checkAnswer(df.selectExpr("ln(d)"), Row(Double.NaN))
  }

  test("SPARK-32764: -0.0 and 0.0 should be equal") {
    val df = Seq(0.0 -> -0.0).toDF("pos", "neg")
    checkAnswer(df.select($"pos" > $"neg"), Row(false))
  }

  test("SPARK-32635: Replace references with foldables coming only from the node's children") {
    val a = Seq("1").toDF("col1").withColumn("col2", lit("1"))
    val b = Seq("2").toDF("col1").withColumn("col2", lit("2"))
    val aub = a.union(b)
    val c = aub.filter($"col1" === "2").cache()
    val d = Seq("2").toDF("col4")
    val r = d.join(aub, $"col2" === $"col4").select("col4")
    val l = c.select("col2")
    val df = l.join(r, $"col2" === $"col4", "LeftOuter")
    checkAnswer(df, Row("2", "2"))
  }

  test("SPARK-33939: Make Column.named use UnresolvedAlias to assign name") {
    val df = spark.range(1).selectExpr("id as id1", "id as id2")
    val df1 = df.selectExpr("cast(struct(id1, id2).id1 as int)")
    assert(df1.schema.head.name == "CAST(struct(id1, id2).id1 AS INT)")

    val df2 = df.selectExpr("cast(array(struct(id1, id2))[0].id1 as int)")
    assert(df2.schema.head.name == "CAST(array(struct(id1, id2))[0].id1 AS INT)")

    val df3 = df.select(hex(expr("struct(id1, id2).id1")))
    assert(df3.schema.head.name == "hex(struct(id1, id2).id1)")

    // this test is to make sure we don't have a regression.
    val df4 = df.selectExpr("id1 == null")
    assert(df4.schema.head.name == "(id1 = NULL)")
  }

  test("SPARK-33989: Strip auto-generated cast when using Cast.sql") {
    Seq("SELECT id == null FROM VALUES(1) AS t(id)",
      "SELECT floor(1)",
      "SELECT split(struct(c1, c2).c1, ',') FROM VALUES(1, 2) AS t(c1, c2)").foreach { sqlStr =>
      assert(!sql(sqlStr).schema.fieldNames.head.toLowerCase(Locale.getDefault).contains("cast"))
    }

    Seq("SELECT id == CAST(null AS int) FROM VALUES(1) AS t(id)",
      "SELECT floor(CAST(1 AS double))",
      "SELECT split(CAST(struct(c1, c2).c1 AS string), ',') FROM VALUES(1, 2) AS t(c1, c2)"
    ).foreach { sqlStr =>
      assert(sql(sqlStr).schema.fieldNames.head.toLowerCase(Locale.getDefault).contains("cast"))
    }
  }

  test("SPARK-34318: colRegex should work with column names & qualifiers which contain newlines") {
    val df = Seq(1, 2, 3).toDF("test\n_column").as("test\n_table")
    val col1 = df.colRegex("`tes.*\n.*mn`")
    checkAnswer(df.select(col1), Row(1) :: Row(2) :: Row(3) :: Nil)

    val col2 = df.colRegex("test\n_table.`tes.*\n.*mn`")
    checkAnswer(df.select(col2), Row(1) :: Row(2) :: Row(3) :: Nil)
  }

  test("SPARK-34763: col(), $\"<name>\", df(\"name\") should handle quoted column name properly") {
    val df1 = spark.sql("SELECT 'col1' AS `a``b.c`")
    checkAnswer(df1.selectExpr("`a``b.c`"), Row("col1"))
    checkAnswer(df1.select(df1("`a``b.c`")), Row("col1"))
    checkAnswer(df1.select(col("`a``b.c`")), Row("col1"))
    checkAnswer(df1.select($"`a``b.c`"), Row("col1"))

    val df2 = df1.as("d.e`f")
    checkAnswer(df2.selectExpr("`a``b.c`"), Row("col1"))
    checkAnswer(df2.select(df2("`a``b.c`")), Row("col1"))
    checkAnswer(df2.select(col("`a``b.c`")), Row("col1"))
    checkAnswer(df2.select($"`a``b.c`"), Row("col1"))

    checkAnswer(df2.selectExpr("`d.e``f`.`a``b.c`"), Row("col1"))
    checkAnswer(df2.select(df2("`d.e``f`.`a``b.c`")), Row("col1"))
    checkAnswer(df2.select(col("`d.e``f`.`a``b.c`")), Row("col1"))
    checkAnswer(df2.select($"`d.e``f`.`a``b.c`"), Row("col1"))

    val df3 = df1.as("*-#&% ?")
    checkAnswer(df3.selectExpr("`*-#&% ?`.`a``b.c`"), Row("col1"))
    checkAnswer(df3.select(df3("*-#&% ?.`a``b.c`")), Row("col1"))
    checkAnswer(df3.select(col("*-#&% ?.`a``b.c`")), Row("col1"))
    checkAnswer(df3.select($"*-#&% ?.`a``b.c`"), Row("col1"))
  }

  test("SPARK-34776: Nested column pruning should not prune Window produced attributes") {
    val df = Seq(
      ("t1", "123", "bob"),
      ("t1", "456", "bob"),
      ("t2", "123", "sam")
    ).toDF("type", "value", "name")

    val test = df.select(
      $"*",
      struct(count($"*").over(Window.partitionBy($"type", $"value", $"name"))
        .as("count"), $"name").as("name_count")
    ).select(
      $"*",
      max($"name_count").over(Window.partitionBy($"type", $"value")).as("best_name")
    )
    checkAnswer(test.select($"best_name.name"), Row("bob") :: Row("bob") :: Row("sam") :: Nil)
  }

  test("isLocal should consider CommandResult and LocalRelation") {
    val df1 = sql("SHOW TABLES")
    assert(df1.isLocal)
    val df2 = (1 to 10).toDF()
    assert(df2.isLocal)
  }

  test("SPARK-35886: PromotePrecision should be subexpr replaced") {
    withTable("tbl") {
      sql(
        """
          |CREATE TABLE tbl (
          |  c1 DECIMAL(18,6),
          |  c2 DECIMAL(18,6),
          |  c3 DECIMAL(18,6))
          |USING parquet;
          |""".stripMargin)
      sql("INSERT INTO tbl SELECT 1, 1, 1")
      checkAnswer(sql("SELECT sum(c1 * c3) + sum(c2 * c3) FROM tbl"), Row(2.00000000000) :: Nil)
    }
  }

  test("SPARK-36338: DataFrame.withSequenceColumn should append unique sequence IDs") {
    val ids = spark.range(10).repartition(5).withSequenceColumn("default_index")
    assert(ids.collect().map(_.getLong(0)).toSet === Range(0, 10).toSet)
    assert(ids.take(5).map(_.getLong(0)).toSet === Range(0, 5).toSet)
  }

  test("SPARK-38285: Fix ClassCastException: GenericArrayData cannot be cast to InternalRow") {
    withTempView("v1") {
      val sqlText =
        """
          |CREATE OR REPLACE TEMP VIEW v1 AS
          |SELECT * FROM VALUES
          |(array(
          |  named_struct('s', 'string1', 'b', array(named_struct('e', 'string2'))),
          |  named_struct('s', 'string4', 'b', array(named_struct('e', 'string5')))
          |  )
          |)
          |v1(o);
          |""".stripMargin
      sql(sqlText)

      val df = sql("SELECT eo.b.e FROM (SELECT explode(o) AS eo FROM v1)")
      checkAnswer(df, Row(Seq("string2")) :: Row(Seq("string5")) :: Nil)
    }
  }

  test("SPARK-37865: Do not deduplicate union output columns") {
    val df1 = Seq((1, 1), (1, 2)).toDF("a", "b")
    val df2 = Seq((2, 2), (2, 3)).toDF("c", "d")

    def sqlQuery(cols1: Seq[String], cols2: Seq[String], distinct: Boolean): String = {
      val union = if (distinct) {
        "UNION"
      } else {
        "UNION ALL"
      }
      s"""
         |SELECT ${cols1.mkString(",")} FROM VALUES (1, 1), (1, 2) AS t1(a, b)
         |$union SELECT ${cols2.mkString(",")} FROM VALUES (2, 2), (2, 3) AS t2(c, d)
         |""".stripMargin
    }

    Seq(
      (Seq("a", "a"), Seq("c", "d"), Seq(Row(1, 1), Row(1, 1), Row(2, 2), Row(2, 3))),
      (Seq("a", "b"), Seq("c", "d"), Seq(Row(1, 1), Row(1, 2), Row(2, 2), Row(2, 3))),
      (Seq("a", "b"), Seq("c", "c"), Seq(Row(1, 1), Row(1, 2), Row(2, 2), Row(2, 2)))
    ).foreach { case (cols1, cols2, rows) =>
      // UNION ALL (non-distinct)
      val df3 = df1.selectExpr(cols1: _*).union(df2.selectExpr(cols2: _*))
      checkAnswer(df3, rows)

      val t3 = sqlQuery(cols1, cols2, false)
      checkAnswer(sql(t3), rows)

      // Avoid breaking change
      var correctAnswer = rows.map(r => Row(r(0)))
      checkAnswer(df3.select(df1.col("a")), correctAnswer)
      checkAnswer(sql(s"select a from ($t3) t3"), correctAnswer)

      // This has always been broken
      intercept[AnalysisException] {
        df3.select(df2.col("d")).collect()
      }
      intercept[AnalysisException] {
        sql(s"select d from ($t3) t3")
      }

      // UNION (distinct)
      val df4 = df3.distinct()
      checkAnswer(df4, rows.distinct)

      val t4 = sqlQuery(cols1, cols2, true)
      checkAnswer(sql(t4), rows.distinct)

      // Avoid breaking change
      correctAnswer = rows.distinct.map(r => Row(r(0)))
      checkAnswer(df4.select(df1.col("a")), correctAnswer)
      checkAnswer(sql(s"select a from ($t4) t4"), correctAnswer)

      // This has always been broken
      intercept[AnalysisException] {
        df4.select(df2.col("d")).collect()
      }
      intercept[AnalysisException] {
        sql(s"select d from ($t4) t4")
      }
    }
  }

  test("SPARK-39612: exceptAll with following count should work") {
    val d1 = Seq("a").toDF()
    assert(d1.exceptAll(d1).count() === 0)
  }

  test("SPARK-39887: RemoveRedundantAliases should keep attributes of a Union's first child 2") {
    val df = sql(
      """
        |SELECT
        |  to_date(a) a,
        |  to_date(b) b
        |FROM
        |  (
        |    SELECT
        |      a,
        |      a AS b
        |    FROM
        |      (
        |        SELECT
        |          to_date(a) a
        |        FROM
        |        VALUES
        |          ('2020-02-01') AS t1(a)
        |        GROUP BY
        |          to_date(a)
        |      ) t3
        |    UNION ALL
        |    SELECT
        |      a,
        |      b
        |    FROM
        |      (
        |        SELECT
        |          to_date(a) a,
        |          to_date(b) b
        |        FROM
        |        VALUES
        |          ('2020-01-01', '2020-01-02') AS t1(a, b)
        |        GROUP BY
        |          to_date(a),
        |          to_date(b)
        |      ) t4
        |  ) t5
        |GROUP BY
        |  to_date(a),
        |  to_date(b);
        |""".stripMargin)
    checkAnswer(df,
      Row(java.sql.Date.valueOf("2020-02-01"), java.sql.Date.valueOf("2020-02-01")) ::
        Row(java.sql.Date.valueOf("2020-01-01"), java.sql.Date.valueOf("2020-01-02")) :: Nil)
  }

  test("SPARK-45216: Non-deterministic functions with seed") {
    val df = Seq(Array.range(0, 10)).toDF("a")

    val r = rand()
    val r2 = randn()
    val r3 = random()
    val r4 = uuid()
    val r5 = shuffle(col("a"))
    df.select(r, r, r2, r2, r3, r3, r4, r4, r5, r5).collect().foreach { row =>
      (0 until 5).foreach(i => assert(row.get(i * 2) === row.get(i * 2 + 1)))
    }
  }

  test("SPARK-41219: IntegralDivide use decimal(1, 0) to represent 0") {
    val df = Seq("0.5944910").toDF("a")
    checkAnswer(df.selectExpr("cast(a as decimal(7,7)) div 100"), Row(0))
  }

  test("SPARK-44206: Dataset.selectExpr scope Session.active") {
    val _spark = spark.newSession()
    _spark.conf.set("spark.sql.legacy.interval.enabled", "true")
    val df1 = _spark.sql("select '2023-01-01'+ INTERVAL 1 YEAR as b")
    val df2 = _spark.sql("select '2023-01-01' as a").selectExpr("a + INTERVAL 1 YEAR as b")
    checkAnswer(df1, df2)
  }

  test("SPARK-44373: filter respects active session and it's params respects parser") {
    withSQLConf(SqlApiConf.ANSI_ENABLED_KEY -> "true",
      SqlApiConf.ENFORCE_RESERVED_KEYWORDS_KEY -> "true") {
      checkError(
        exception = intercept[SparkThrowable] {
          spark.range(1).toDF("CASE").filter("CASE").collect()
        },
        errorClass = "PARSE_SYNTAX_ERROR",
        parameters = Map("error" -> "'CASE'", "hint" -> ""))
    }
  }

  test("SPARK-44373: createTempView respects active session and it's params respects parser") {
    withSQLConf(SqlApiConf.ANSI_ENABLED_KEY -> "true",
      SqlApiConf.ENFORCE_RESERVED_KEYWORDS_KEY -> "true") {
      checkError(
        exception = intercept[AnalysisException] {
          spark.range(1).createTempView("AUTHORIZATION")
        },
        errorClass = "_LEGACY_ERROR_TEMP_1321",
        parameters = Map("viewName" -> "AUTHORIZATION"))
    }
  }
}
