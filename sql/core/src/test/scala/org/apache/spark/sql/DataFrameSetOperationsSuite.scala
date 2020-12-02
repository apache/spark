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

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.catalyst.plans.logical.Union
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{ExamplePoint, ExamplePointUDT, SharedSparkSession}
import org.apache.spark.sql.test.SQLTestData.NullStrings
import org.apache.spark.sql.types._

class DataFrameSetOperationsSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("except") {
    checkAnswer(
      lowerCaseData.except(upperCaseData),
      Row(1, "a") ::
        Row(2, "b") ::
        Row(3, "c") ::
        Row(4, "d") :: Nil)
    checkAnswer(lowerCaseData.except(lowerCaseData), Nil)
    checkAnswer(upperCaseData.except(upperCaseData), Nil)

    // check null equality
    checkAnswer(
      nullInts.except(nullInts.filter("0 = 1")),
      nullInts)
    checkAnswer(
      nullInts.except(nullInts),
      Nil)

    // check if values are de-duplicated
    checkAnswer(
      allNulls.except(allNulls.filter("0 = 1")),
      Row(null) :: Nil)
    checkAnswer(
      allNulls.except(allNulls),
      Nil)

    // check if values are de-duplicated
    val df = Seq(("id1", 1), ("id1", 1), ("id", 1), ("id1", 2)).toDF("id", "value")
    checkAnswer(
      df.except(df.filter("0 = 1")),
      Row("id1", 1) ::
        Row("id", 1) ::
        Row("id1", 2) :: Nil)

    // check if the empty set on the left side works
    checkAnswer(
      allNulls.filter("0 = 1").except(allNulls),
      Nil)
  }

  test("SPARK-23274: except between two projects without references used in filter") {
    val df = Seq((1, 2, 4), (1, 3, 5), (2, 2, 3), (2, 4, 5)).toDF("a", "b", "c")
    val df1 = df.filter($"a" === 1)
    val df2 = df.filter($"a" === 2)
    checkAnswer(df1.select("b").except(df2.select("b")), Row(3) :: Nil)
    checkAnswer(df1.select("b").except(df2.select("c")), Row(2) :: Nil)
  }

  test("except distinct - SQL compliance") {
    val df_left = Seq(1, 2, 2, 3, 3, 4).toDF("id")
    val df_right = Seq(1, 3).toDF("id")

    checkAnswer(
      df_left.except(df_right),
      Row(2) :: Row(4) :: Nil
    )
  }

  test("except - nullability") {
    val nonNullableInts = Seq(Tuple1(11), Tuple1(3)).toDF()
    assert(nonNullableInts.schema.forall(!_.nullable))

    val df1 = nonNullableInts.except(nullInts)
    checkAnswer(df1, Row(11) :: Nil)
    assert(df1.schema.forall(!_.nullable))

    val df2 = nullInts.except(nonNullableInts)
    checkAnswer(df2, Row(1) :: Row(2) :: Row(null) :: Nil)
    assert(df2.schema.forall(_.nullable))

    val df3 = nullInts.except(nullInts)
    checkAnswer(df3, Nil)
    assert(df3.schema.forall(_.nullable))

    val df4 = nonNullableInts.except(nonNullableInts)
    checkAnswer(df4, Nil)
    assert(df4.schema.forall(!_.nullable))
  }

  test("except all") {
    checkAnswer(
      lowerCaseData.exceptAll(upperCaseData),
      Row(1, "a") ::
        Row(2, "b") ::
        Row(3, "c") ::
        Row(4, "d") :: Nil)
    checkAnswer(lowerCaseData.exceptAll(lowerCaseData), Nil)
    checkAnswer(upperCaseData.exceptAll(upperCaseData), Nil)

    // check null equality
    checkAnswer(
      nullInts.exceptAll(nullInts.filter("0 = 1")),
      nullInts)
    checkAnswer(
      nullInts.exceptAll(nullInts),
      Nil)

    // check that duplicate values are preserved
    checkAnswer(
      allNulls.exceptAll(allNulls.filter("0 = 1")),
      Row(null) :: Row(null) :: Row(null) :: Row(null) :: Nil)
    checkAnswer(
      allNulls.exceptAll(allNulls.limit(2)),
      Row(null) :: Row(null) :: Nil)

    // check that duplicates are retained.
    val df = spark.sparkContext.parallelize(
      NullStrings(1, "id1") ::
        NullStrings(1, "id1") ::
        NullStrings(2, "id1") ::
        NullStrings(3, null) :: Nil).toDF("id", "value")

    checkAnswer(
      df.exceptAll(df.filter("0 = 1")),
      Row(1, "id1") ::
        Row(1, "id1") ::
        Row(2, "id1") ::
        Row(3, null) :: Nil)

    // check if the empty set on the left side works
    checkAnswer(
      allNulls.filter("0 = 1").exceptAll(allNulls),
      Nil)

  }

  test("exceptAll - nullability") {
    val nonNullableInts = Seq(Tuple1(11), Tuple1(3)).toDF()
    assert(nonNullableInts.schema.forall(!_.nullable))

    val df1 = nonNullableInts.exceptAll(nullInts)
    checkAnswer(df1, Row(11) :: Nil)
    assert(df1.schema.forall(!_.nullable))

    val df2 = nullInts.exceptAll(nonNullableInts)
    checkAnswer(df2, Row(1) :: Row(2) :: Row(null) :: Nil)
    assert(df2.schema.forall(_.nullable))

    val df3 = nullInts.exceptAll(nullInts)
    checkAnswer(df3, Nil)
    assert(df3.schema.forall(_.nullable))

    val df4 = nonNullableInts.exceptAll(nonNullableInts)
    checkAnswer(df4, Nil)
    assert(df4.schema.forall(!_.nullable))
  }

  test("intersect") {
    checkAnswer(
      lowerCaseData.intersect(lowerCaseData),
      Row(1, "a") ::
        Row(2, "b") ::
        Row(3, "c") ::
        Row(4, "d") :: Nil)
    checkAnswer(lowerCaseData.intersect(upperCaseData), Nil)

    // check null equality
    checkAnswer(
      nullInts.intersect(nullInts),
      Row(1) ::
        Row(2) ::
        Row(3) ::
        Row(null) :: Nil)

    // check if values are de-duplicated
    checkAnswer(
      allNulls.intersect(allNulls),
      Row(null) :: Nil)

    // check if values are de-duplicated
    val df = Seq(("id1", 1), ("id1", 1), ("id", 1), ("id1", 2)).toDF("id", "value")
    checkAnswer(
      df.intersect(df),
      Row("id1", 1) ::
        Row("id", 1) ::
        Row("id1", 2) :: Nil)
  }

  test("intersect - nullability") {
    val nonNullableInts = Seq(Tuple1(1), Tuple1(3)).toDF()
    assert(nonNullableInts.schema.forall(!_.nullable))

    val df1 = nonNullableInts.intersect(nullInts)
    checkAnswer(df1, Row(1) :: Row(3) :: Nil)
    assert(df1.schema.forall(!_.nullable))

    val df2 = nullInts.intersect(nonNullableInts)
    checkAnswer(df2, Row(1) :: Row(3) :: Nil)
    assert(df2.schema.forall(!_.nullable))

    val df3 = nullInts.intersect(nullInts)
    checkAnswer(df3, Row(1) :: Row(2) :: Row(3) :: Row(null) :: Nil)
    assert(df3.schema.forall(_.nullable))

    val df4 = nonNullableInts.intersect(nonNullableInts)
    checkAnswer(df4, Row(1) :: Row(3) :: Nil)
    assert(df4.schema.forall(!_.nullable))
  }

  test("intersectAll") {
    checkAnswer(
      lowerCaseDataWithDuplicates.intersectAll(lowerCaseDataWithDuplicates),
      Row(1, "a") ::
        Row(2, "b") ::
        Row(2, "b") ::
        Row(3, "c") ::
        Row(3, "c") ::
        Row(3, "c") ::
        Row(4, "d") :: Nil)
    checkAnswer(lowerCaseData.intersectAll(upperCaseData), Nil)

    // check null equality
    checkAnswer(
      nullInts.intersectAll(nullInts),
      Row(1) ::
        Row(2) ::
        Row(3) ::
        Row(null) :: Nil)

    // Duplicate nulls are preserved.
    checkAnswer(
      allNulls.intersectAll(allNulls),
      Row(null) :: Row(null) :: Row(null) :: Row(null) :: Nil)

    val df_left = Seq(1, 2, 2, 3, 3, 4).toDF("id")
    val df_right = Seq(1, 2, 2, 3).toDF("id")

    checkAnswer(
      df_left.intersectAll(df_right),
      Row(1) :: Row(2) :: Row(2) :: Row(3) :: Nil)
  }

  test("intersectAll - nullability") {
    val nonNullableInts = Seq(Tuple1(1), Tuple1(3)).toDF()
    assert(nonNullableInts.schema.forall(!_.nullable))

    val df1 = nonNullableInts.intersectAll(nullInts)
    checkAnswer(df1, Row(1) :: Row(3) :: Nil)
    assert(df1.schema.forall(!_.nullable))

    val df2 = nullInts.intersectAll(nonNullableInts)
    checkAnswer(df2, Row(1) :: Row(3) :: Nil)
    assert(df2.schema.forall(!_.nullable))

    val df3 = nullInts.intersectAll(nullInts)
    checkAnswer(df3, Row(1) :: Row(2) :: Row(3) :: Row(null) :: Nil)
    assert(df3.schema.forall(_.nullable))

    val df4 = nonNullableInts.intersectAll(nonNullableInts)
    checkAnswer(df4, Row(1) :: Row(3) :: Nil)
    assert(df4.schema.forall(!_.nullable))
  }

  test("SPARK-10539: Project should not be pushed down through Intersect or Except") {
    val df1 = (1 to 100).map(Tuple1.apply).toDF("i")
    val df2 = (1 to 30).map(Tuple1.apply).toDF("i")
    val intersect = df1.intersect(df2)
    val except = df1.except(df2)
    assert(intersect.count() === 30)
    assert(except.count() === 70)
  }

  test("SPARK-10740: handle nondeterministic expressions correctly for set operations") {
    val df1 = (1 to 20).map(Tuple1.apply).toDF("i")
    val df2 = (1 to 10).map(Tuple1.apply).toDF("i")

    // When generating expected results at here, we need to follow the implementation of
    // Rand expression.
    def expected(df: DataFrame): Seq[Row] =
      df.rdd.collectPartitions().zipWithIndex.flatMap {
        case (data, index) =>
          val rng = new org.apache.spark.util.random.XORShiftRandom(7 + index)
          data.filter(_.getInt(0) < rng.nextDouble() * 10)
      }.toSeq

    val union = df1.union(df2)
    checkAnswer(
      union.filter($"i" < rand(7) * 10),
      expected(union)
    )
    checkAnswer(
      union.select(rand(7)),
      union.rdd.collectPartitions().zipWithIndex.flatMap {
        case (data, index) =>
          val rng = new org.apache.spark.util.random.XORShiftRandom(7 + index)
          data.map(_ => rng.nextDouble()).map(i => Row(i))
      }
    )

    val intersect = df1.intersect(df2)
    checkAnswer(
      intersect.filter($"i" < rand(7) * 10),
      expected(intersect)
    )

    val except = df1.except(df2)
    checkAnswer(
      except.filter($"i" < rand(7) * 10),
      expected(except)
    )
  }

  test("SPARK-17123: Performing set operations that combine non-scala native types") {
    val dates = Seq(
      (new Date(0), BigDecimal.valueOf(1), new Timestamp(2)),
      (new Date(3), BigDecimal.valueOf(4), new Timestamp(5))
    ).toDF("date", "timestamp", "decimal")

    val widenTypedRows = Seq(
      (new Timestamp(2), 10.5D, "string")
    ).toDF("date", "timestamp", "decimal")

    dates.union(widenTypedRows).collect()
    dates.except(widenTypedRows).collect()
    dates.intersect(widenTypedRows).collect()
  }

  test("SPARK-19893: cannot run set operations with map type") {
    val df = spark.range(1).select(map(lit("key"), $"id").as("m"))
    val e = intercept[AnalysisException](df.intersect(df))
    assert(e.message.contains(
      "Cannot have map type columns in DataFrame which calls set operations"))
    val e2 = intercept[AnalysisException](df.except(df))
    assert(e2.message.contains(
      "Cannot have map type columns in DataFrame which calls set operations"))
    val e3 = intercept[AnalysisException](df.distinct())
    assert(e3.message.contains(
      "Cannot have map type columns in DataFrame which calls set operations"))
    withTempView("v") {
      df.createOrReplaceTempView("v")
      val e4 = intercept[AnalysisException](sql("SELECT DISTINCT m FROM v"))
      assert(e4.message.contains(
        "Cannot have map type columns in DataFrame which calls set operations"))
    }
  }

  test("union all") {
    val unionDF = testData.union(testData).union(testData)
      .union(testData).union(testData)

    // Before optimizer, Union should be combined.
    assert(unionDF.queryExecution.analyzed.collect {
      case j: Union if j.children.size == 5 => j }.size === 1)

    checkAnswer(
      unionDF.agg(avg("key"), max("key"), min("key"), sum("key")),
      Row(50.5, 100, 1, 25250) :: Nil
    )

    // unionAll is an alias of union
    val unionAllDF = testData.unionAll(testData).unionAll(testData)
      .unionAll(testData).unionAll(testData)

    checkAnswer(unionDF, unionAllDF)
  }

  test("union should union DataFrames with UDTs (SPARK-13410)") {
    val rowRDD1 = sparkContext.parallelize(Seq(Row(1, new ExamplePoint(1.0, 2.0))))
    val schema1 = StructType(Array(StructField("label", IntegerType, false),
      StructField("point", new ExamplePointUDT(), false)))
    val rowRDD2 = sparkContext.parallelize(Seq(Row(2, new ExamplePoint(3.0, 4.0))))
    val schema2 = StructType(Array(StructField("label", IntegerType, false),
      StructField("point", new ExamplePointUDT(), false)))
    val df1 = spark.createDataFrame(rowRDD1, schema1)
    val df2 = spark.createDataFrame(rowRDD2, schema2)

    checkAnswer(
      df1.union(df2).orderBy("label"),
      Seq(Row(1, new ExamplePoint(1.0, 2.0)), Row(2, new ExamplePoint(3.0, 4.0)))
    )
  }

  test("union by name") {
    var df1 = Seq((1, 2, 3)).toDF("a", "b", "c")
    var df2 = Seq((3, 1, 2)).toDF("c", "a", "b")
    val df3 = Seq((2, 3, 1)).toDF("b", "c", "a")
    val unionDf = df1.unionByName(df2.unionByName(df3))
    checkAnswer(unionDf,
      Row(1, 2, 3) :: Row(1, 2, 3) :: Row(1, 2, 3) :: Nil
    )

    // Check if adjacent unions are combined into a single one
    assert(unionDf.queryExecution.optimizedPlan.collect { case u: Union => true }.size == 1)

    // Check failure cases
    df1 = Seq((1, 2)).toDF("a", "c")
    df2 = Seq((3, 4, 5)).toDF("a", "b", "c")
    var errMsg = intercept[AnalysisException] {
      df1.unionByName(df2)
    }.getMessage
    assert(errMsg.contains(
      "Union can only be performed on tables with the same number of columns, " +
        "but the first table has 2 columns and the second table has 3 columns"))

    df1 = Seq((1, 2, 3)).toDF("a", "b", "c")
    df2 = Seq((4, 5, 6)).toDF("a", "c", "d")
    errMsg = intercept[AnalysisException] {
      df1.unionByName(df2)
    }.getMessage
    assert(errMsg.contains("""Cannot resolve column name "b" among (a, c, d)"""))
  }

  test("union by name - type coercion") {
    var df1 = Seq((1, "a")).toDF("c0", "c1")
    var df2 = Seq((3, 1L)).toDF("c1", "c0")
    checkAnswer(df1.unionByName(df2), Row(1L, "a") :: Row(1L, "3") :: Nil)

    df1 = Seq((1, 1.0)).toDF("c0", "c1")
    df2 = Seq((8L, 3.0)).toDF("c1", "c0")
    checkAnswer(df1.unionByName(df2), Row(1.0, 1.0) :: Row(3.0, 8.0) :: Nil)

    df1 = Seq((2.0f, 7.4)).toDF("c0", "c1")
    df2 = Seq(("a", 4.0)).toDF("c1", "c0")
    checkAnswer(df1.unionByName(df2), Row(2.0, "7.4") :: Row(4.0, "a") :: Nil)

    df1 = Seq((1, "a", 3.0)).toDF("c0", "c1", "c2")
    df2 = Seq((1.2, 2, "bc")).toDF("c2", "c0", "c1")
    val df3 = Seq(("def", 1.2, 3)).toDF("c1", "c2", "c0")
    checkAnswer(df1.unionByName(df2.unionByName(df3)),
      Row(1, "a", 3.0) :: Row(2, "bc", 1.2) :: Row(3, "def", 1.2) :: Nil
    )
  }

  test("union by name - check case sensitivity") {
    def checkCaseSensitiveTest(): Unit = {
      val df1 = Seq((1, 2, 3)).toDF("ab", "cd", "ef")
      val df2 = Seq((4, 5, 6)).toDF("cd", "ef", "AB")
      checkAnswer(df1.unionByName(df2), Row(1, 2, 3) :: Row(6, 4, 5) :: Nil)
    }
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val errMsg2 = intercept[AnalysisException] {
        checkCaseSensitiveTest()
      }.getMessage
      assert(errMsg2.contains("""Cannot resolve column name "ab" among (cd, ef, AB)"""))
    }
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      checkCaseSensitiveTest()
    }
  }

  test("union by name - check name duplication") {
    Seq((true, ("a", "a")), (false, ("aA", "Aa"))).foreach { case (caseSensitive, (c0, c1)) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        var df1 = Seq((1, 1)).toDF(c0, c1)
        var df2 = Seq((1, 1)).toDF("c0", "c1")
        var errMsg = intercept[AnalysisException] {
          df1.unionByName(df2)
        }.getMessage
        assert(errMsg.contains("Found duplicate column(s) in the left attributes:"))
        df1 = Seq((1, 1)).toDF("c0", "c1")
        df2 = Seq((1, 1)).toDF(c0, c1)
        errMsg = intercept[AnalysisException] {
          df1.unionByName(df2)
        }.getMessage
        assert(errMsg.contains("Found duplicate column(s) in the right attributes:"))
      }
    }
  }

  test("SPARK-25368 Incorrect predicate pushdown returns wrong result") {
    def check(newCol: Column, filter: Column, result: Seq[Row]): Unit = {
      val df1 = spark.createDataFrame(Seq(
        (1, 1)
      )).toDF("a", "b").withColumn("c", newCol)

      val df2 = df1.union(df1).withColumn("d", spark_partition_id).filter(filter)
      checkAnswer(df2, result)
    }

    check(lit(null).cast("int"), $"c".isNull, Seq(Row(1, 1, null, 0), Row(1, 1, null, 1)))
    check(lit(null).cast("int"), $"c".isNotNull, Seq())
    check(lit(2).cast("int"), $"c".isNull, Seq())
    check(lit(2).cast("int"), $"c".isNotNull, Seq(Row(1, 1, 2, 0), Row(1, 1, 2, 1)))
    check(lit(2).cast("int"), $"c" === 2, Seq(Row(1, 1, 2, 0), Row(1, 1, 2, 1)))
    check(lit(2).cast("int"), $"c" =!= 2, Seq())
  }

  test("SPARK-29358: Make unionByName optionally fill missing columns with nulls") {
    var df1 = Seq(1, 2, 3).toDF("a")
    var df2 = Seq(3, 1, 2).toDF("b")
    val df3 = Seq(2, 3, 1).toDF("c")
    val unionDf = df1.unionByName(df2.unionByName(df3, true), true)
    checkAnswer(unionDf,
      Row(1, null, null) :: Row(2, null, null) :: Row(3, null, null) :: // df1
        Row(null, 3, null) :: Row(null, 1, null) :: Row(null, 2, null) :: // df2
        Row(null, null, 2) :: Row(null, null, 3) :: Row(null, null, 1) :: Nil // df3
    )

    df1 = Seq((1, 2)).toDF("a", "c")
    df2 = Seq((3, 4, 5)).toDF("a", "b", "c")
    checkAnswer(df1.unionByName(df2, true),
      Row(1, 2, null) :: Row(3, 5, 4) :: Nil)
    checkAnswer(df2.unionByName(df1, true),
      Row(3, 4, 5) :: Row(1, null, 2) :: Nil)

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      df2 = Seq((3, 4, 5)).toDF("a", "B", "C")
      val union1 = df1.unionByName(df2, true)
      val union2 = df2.unionByName(df1, true)

      checkAnswer(union1, Row(1, 2, null, null) :: Row(3, null, 4, 5) :: Nil)
      checkAnswer(union2, Row(3, 4, 5, null) :: Row(1, null, null, 2) :: Nil)

      assert(union1.schema.fieldNames === Array("a", "c", "B", "C"))
      assert(union2.schema.fieldNames === Array("a", "B", "C", "c"))
    }
  }

  test("SPARK-32376: Make unionByName null-filling behavior work with struct columns - simple") {
    val df1 = Seq(((1, 2, 3), 0), ((2, 3, 4), 1), ((3, 4, 5), 2)).toDF("a", "idx")
    val df2 = Seq(((3, 4), 0), ((1, 2), 1), ((2, 3), 2)).toDF("a", "idx")
    val df3 = Seq(((100, 101, 102, 103), 0), ((110, 111, 112, 113), 1), ((120, 121, 122, 123), 2))
      .toDF("a", "idx")

    var unionDf = df1.unionByName(df2, true)

    checkAnswer(unionDf,
      Row(Row(1, 2, 3), 0) :: Row(Row(2, 3, 4), 1) :: Row(Row(3, 4, 5), 2) ::
        Row(Row(3, 4, null), 0) :: Row(Row(1, 2, null), 1) :: Row(Row(2, 3, null), 2) :: Nil
    )

    assert(unionDf.schema.toDDL == "`a` STRUCT<`_1`: INT, `_2`: INT, `_3`: INT>,`idx` INT")

    unionDf = df1.unionByName(df2, true).unionByName(df3, true)

    checkAnswer(unionDf,
      Row(Row(1, 2, 3, null), 0) ::
        Row(Row(2, 3, 4, null), 1) ::
        Row(Row(3, 4, 5, null), 2) :: // df1
        Row(Row(3, 4, null, null), 0) ::
        Row(Row(1, 2, null, null), 1) ::
        Row(Row(2, 3, null, null), 2) :: // df2
        Row(Row(100, 101, 102, 103), 0) ::
        Row(Row(110, 111, 112, 113), 1) ::
        Row(Row(120, 121, 122, 123), 2) :: Nil // df3
    )
    assert(unionDf.schema.toDDL ==
      "`a` STRUCT<`_1`: INT, `_2`: INT, `_3`: INT, `_4`: INT>,`idx` INT")
  }

  test("SPARK-32376: Make unionByName null-filling behavior work with struct columns - nested") {
    val df1 = Seq((0, UnionClass1a(0, 1L, UnionClass2(1, "2")))).toDF("id", "a")
    val df2 = Seq((1, UnionClass1b(1, 2L, UnionClass3(2, 3L)))).toDF("id", "a")

    val expectedSchema = "`id` INT,`a` STRUCT<`a`: INT, `b`: BIGINT, " +
      "`nested`: STRUCT<`a`: INT, `b`: BIGINT, `c`: STRING>>"

    var unionDf = df1.unionByName(df2, true)
    checkAnswer(unionDf,
      Row(0, Row(0, 1, Row(1, null, "2"))) ::
        Row(1, Row(1, 2, Row(2, 3L, null))) :: Nil)
    assert(unionDf.schema.toDDL == expectedSchema)

    unionDf = df2.unionByName(df1, true)
    checkAnswer(unionDf,
      Row(1, Row(1, 2, Row(2, 3L, null))) ::
        Row(0, Row(0, 1, Row(1, null, "2"))) :: Nil)
    assert(unionDf.schema.toDDL == expectedSchema)

    val df3 = Seq((2, UnionClass1b(2, 3L, null))).toDF("id", "a")
    unionDf = df1.unionByName(df3, true)
    checkAnswer(unionDf,
      Row(0, Row(0, 1, Row(1, null, "2"))) ::
        Row(2, Row(2, 3, null)) :: Nil)
    assert(unionDf.schema.toDDL == expectedSchema)
  }

  test("SPARK-32376: Make unionByName null-filling behavior work with struct columns" +
      " - case-sensitive cases") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val df1 = Seq((0, UnionClass1a(0, 1L, UnionClass2(1, "2")))).toDF("id", "a")
      val df2 = Seq((1, UnionClass1c(1, 2L, UnionClass4(2, 3L)))).toDF("id", "a")

      var unionDf = df1.unionByName(df2, true)
      checkAnswer(unionDf,
        Row(0, Row(0, 1, Row(null, 1, null, "2"))) ::
          Row(1, Row(1, 2, Row(2, null, 3L, null))) :: Nil)
      assert(unionDf.schema.toDDL ==
        "`id` INT,`a` STRUCT<`a`: INT, `b`: BIGINT, " +
          "`nested`: STRUCT<`A`: INT, `a`: INT, `b`: BIGINT, `c`: STRING>>")

      unionDf = df2.unionByName(df1, true)
      checkAnswer(unionDf,
        Row(1, Row(1, 2, Row(2, null, 3L, null))) ::
          Row(0, Row(0, 1, Row(null, 1, null, "2"))) :: Nil)
      assert(unionDf.schema.toDDL ==
        "`id` INT,`a` STRUCT<`a`: INT, `b`: BIGINT, " +
          "`nested`: STRUCT<`A`: INT, `a`: INT, `b`: BIGINT, `c`: STRING>>")

      val df3 = Seq((2, UnionClass1b(2, 3L, UnionClass3(4, 5L)))).toDF("id", "a")
      unionDf = df2.unionByName(df3, true)
      checkAnswer(unionDf,
        Row(1, Row(1, 2, Row(2, null, 3L))) ::
          Row(2, Row(2, 3, Row(null, 4, 5L))) :: Nil)
      assert(unionDf.schema.toDDL ==
        "`id` INT,`a` STRUCT<`a`: INT, `b`: BIGINT, " +
          "`nested`: STRUCT<`A`: INT, `a`: INT, `b`: BIGINT>>")
    }
  }

  test("SPARK-32376: Make unionByName null-filling behavior work with struct columns - edge case") {
    val nestedStructType1 = StructType(Seq(
      StructField("b", StringType)))
    val nestedStructValues1 = Row("b")

    val nestedStructType2 = StructType(Seq(
      StructField("b", StringType),
      StructField("a", StringType)))
    val nestedStructValues2 = Row("b", "a")

    val df1: DataFrame = spark.createDataFrame(
      sparkContext.parallelize(Row(nestedStructValues1) :: Nil),
      StructType(Seq(StructField("topLevelCol", nestedStructType1))))

    val df2: DataFrame = spark.createDataFrame(
      sparkContext.parallelize(Row(nestedStructValues2) :: Nil),
      StructType(Seq(StructField("topLevelCol", nestedStructType2))))

    val union = df1.unionByName(df2, allowMissingColumns = true)
    checkAnswer(union, Row(Row(null, "b")) :: Row(Row("a", "b")) :: Nil)
    assert(union.schema.toDDL == "`topLevelCol` STRUCT<`a`: STRING, `b`: STRING>")
  }

  test("SPARK-32376: Make unionByName null-filling behavior work with struct columns - deep expr") {
    def nestedDf(depth: Int, numColsAtEachDepth: Int): DataFrame = {
      val initialNestedStructType = StructType(
        (0 to numColsAtEachDepth).map(i =>
          StructField(s"nested${depth}Col$i", IntegerType, nullable = false))
      )
      val initialNestedValues = Row(0 to numColsAtEachDepth: _*)

      var depthCounter = depth - 1
      var structType = initialNestedStructType
      var struct = initialNestedValues
      while (depthCounter != 0) {
        struct = Row((struct +: (1 to numColsAtEachDepth)): _*)
        structType = StructType(
          StructField(s"nested${depthCounter}Col0", structType, nullable = false) +:
            (1 to numColsAtEachDepth).map(i =>
              StructField(s"nested${depthCounter}Col$i", IntegerType, nullable = false))
        )
        depthCounter -= 1
      }

      val df: DataFrame = spark.createDataFrame(
        sparkContext.parallelize(Row(struct) :: Nil),
        StructType(Seq(StructField("nested0Col0", structType))))

      df
    }

    val df1 = nestedDf(depth = 10, numColsAtEachDepth = 1)
    val df2 = nestedDf(depth = 10, numColsAtEachDepth = 20)
    val union = df1.unionByName(df2, allowMissingColumns = true)
    // scalastyle:off
    val row1 = Row(Row(Row(Row(Row(Row(Row(Row(Row(Row(
      Row(0, 1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
      1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
      1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
      1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
      1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
      1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
      1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
      1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
      1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null),
      1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null))
    val row2 = Row(Row(Row(Row(Row(Row(Row(Row(Row(Row(
      Row(0, 1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 2, 20, 3, 4, 5, 6, 7, 8, 9),
      1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 2, 20, 3, 4, 5, 6, 7, 8, 9),
      1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 2, 20, 3, 4, 5, 6, 7, 8, 9),
      1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 2, 20, 3, 4, 5, 6, 7, 8, 9),
      1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 2, 20, 3, 4, 5, 6, 7, 8, 9),
      1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 2, 20, 3, 4, 5, 6, 7, 8, 9),
      1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 2, 20, 3, 4, 5, 6, 7, 8, 9),
      1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 2, 20, 3, 4, 5, 6, 7, 8, 9),
      1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 2, 20, 3, 4, 5, 6, 7, 8, 9),
      1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 2, 20, 3, 4, 5, 6, 7, 8, 9))
    // scalastyle:on
    checkAnswer(union, row1 :: row2 :: Nil)
  }
}

case class UnionClass1a(a: Int, b: Long, nested: UnionClass2)
case class UnionClass1b(a: Int, b: Long, nested: UnionClass3)
case class UnionClass1c(a: Int, b: Long, nested: UnionClass4)

case class UnionClass2(a: Int, c: String)
case class UnionClass3(a: Int, b: Long)
case class UnionClass4(A: Int, b: Long)
