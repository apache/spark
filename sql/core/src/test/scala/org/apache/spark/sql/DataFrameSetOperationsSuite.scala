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

import org.apache.spark.sql.catalyst.optimizer.RemoveNoopUnion
import org.apache.spark.sql.catalyst.plans.logical.Union
import org.apache.spark.sql.execution.{SparkPlan, UnionExec}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{ExamplePoint, ExamplePointUDT, SharedSparkSession, SQLTestData}
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
      (new Timestamp(2), 10.5D, "2021-01-01 00:00:00")
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
      case u: Union if u.children.size == 5 => u }.size === 1)

    checkAnswer(
      unionDF.agg(avg("key"), max("key"), min("key"), sum("key")),
      Row(50.5, 100, 1, 25250) :: Nil
    )

    // unionAll is an alias of union
    val unionAllDF = testData.unionAll(testData).unionAll(testData)
      .unionAll(testData).unionAll(testData)

    checkAnswer(unionDF, unionAllDF)
  }

  test("SPARK-34283: SQL-style union using Dataset, " +
    "remove unnecessary deduplicate in multiple unions") {
    withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> RemoveNoopUnion.ruleName) {
      val unionDF = testData.union(testData).distinct().union(testData).distinct()
        .union(testData).distinct().union(testData).distinct()

      // Before optimizer, there are three 'union.deduplicate' operations should be combined.
      assert(unionDF.queryExecution.analyzed.collect {
        case u: Union if u.children.size == 4 => u
      }.size === 1)

      // After optimizer, four 'union.deduplicate' operations should be combined.
      assert(unionDF.queryExecution.optimizedPlan.collect {
        case u: Union if u.children.size == 5 => u
      }.size === 1)

      checkAnswer(
        unionDF.agg(avg("key"), max("key"), min("key"),
          sum("key")), Row(50.5, 100, 1, 5050) :: Nil
      )

      // The result of SQL-style union
      val unionSQLResult = sql(
        """
          | select key, value from testData
          | union
          | select key, value from testData
          | union
          | select key, value from testData
          | union
          | select key, value from testData
          | union
          | select key, value from testData
          |""".stripMargin)
      checkAnswer(unionDF, unionSQLResult)
    }
  }

  test("SPARK-34283: SQL-style union using Dataset, " +
    "keep necessary deduplicate in multiple unions") {
    withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> RemoveNoopUnion.ruleName) {
      val df1 = Seq((1, 2, 3)).toDF("a", "b", "c")
      var df2 = Seq((6, 2, 5)).toDF("a", "b", "c")
      var df3 = Seq((2, 4, 3)).toDF("c", "a", "b")
      var df4 = Seq((1, 4, 5)).toDF("b", "a", "c")

      val unionDF = df1.unionByName(df2).dropDuplicates(Seq("a"))
        .unionByName(df3).dropDuplicates("c").unionByName(df4)
        .dropDuplicates("b")

      // In this case, there is no 'union.deduplicate' operation will be combined.
      assert(unionDF.queryExecution.analyzed.collect {
        case u: Union if u.children.size == 2 => u
      }.size === 3)

      assert(unionDF.queryExecution.optimizedPlan.collect {
        case u: Union if u.children.size == 2 => u
      }.size === 3)

      checkAnswer(
        unionDF,
        Row(4, 3, 2) :: Row(4, 1, 5) :: Row(1, 2, 3) :: Nil
      )

      val unionDF1 = df1.unionByName(df2).dropDuplicates(Seq("B", "A", "c"))
        .unionByName(df3).dropDuplicates().unionByName(df4)
        .dropDuplicates("A")

      // In this case, there are two 'union.deduplicate' operations will be combined.
      assert(unionDF1.queryExecution.analyzed.collect {
        case u: Union if u.children.size == 2 => u
      }.size === 1)
      assert(unionDF1.queryExecution.analyzed.collect {
        case u: Union if u.children.size == 3 => u
      }.size === 1)

      assert(unionDF1.queryExecution.optimizedPlan.collect {
        case u: Union if u.children.size == 2 => u
      }.size === 1)
      assert(unionDF1.queryExecution.optimizedPlan.collect {
        case u: Union if u.children.size == 3 => u
      }.size === 1)

      checkAnswer(
        unionDF1,
        Row(4, 3, 2) :: Row(6, 2, 5) :: Row(1, 2, 3) :: Nil
      )

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        df2 = Seq((6, 2, 5)).toDF("a", "B", "C")
        df3 = Seq((2, 1, 3)).toDF("b", "a", "c")
        df4 = Seq((1, 4, 5)).toDF("b", "a", "c")

        val unionDF2 = df1.unionByName(df2, true).distinct()
          .unionByName(df3, true).dropDuplicates(Seq("a")).unionByName(df4, true).distinct()

        checkAnswer(unionDF2,
          Row(4, 1, 5, null, null) :: Row(1, 2, 3, null, null) :: Row(6, null, null, 2, 5) :: Nil)
        assert(unionDF2.schema.fieldNames === Array("a", "b", "c", "B", "C"))
      }
    }
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
    checkError(
      exception = intercept[AnalysisException] {
        df1.unionByName(df2)
      },
      errorClass = "NUM_COLUMNS_MISMATCH",
      parameters = Map(
        "operator" -> "UNION",
        "refNumColumns" -> "2",
        "invalidOrdinalNum" -> "second",
        "invalidNumColumns" -> "3"))

    df1 = Seq((1, 2, 3)).toDF("a", "b", "c")
    df2 = Seq((4, 5, 6)).toDF("a", "c", "d")
    val errMsg = intercept[AnalysisException] {
      df1.unionByName(df2)
    }.getMessage
    assert(errMsg.contains("""Cannot resolve column name "b" among (a, c, d)"""))
  }

  test("union by name - type coercion") {
    var df1 = Seq((1, 1.0)).toDF("c0", "c1")
    var df2 = Seq((8L, 3.0)).toDF("c1", "c0")
    checkAnswer(df1.unionByName(df2), Row(1.0, 1.0) :: Row(3.0, 8.0) :: Nil)
    if (!conf.ansiEnabled) {
      df1 = Seq((1, "a")).toDF("c0", "c1")
      df2 = Seq((3, 1L)).toDF("c1", "c0")
      checkAnswer(df1.unionByName(df2), Row(1L, "a") :: Row(1L, "3") :: Nil)

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

    var schema = new StructType()
      .add("a", new StructType()
        .add("_1", IntegerType, true)
        .add("_2", IntegerType, true)
        .add("_3", IntegerType, true), true)
      .add("idx", IntegerType, false)
    assert(unionDf.schema == schema)

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
    schema = new StructType()
      .add("a", new StructType()
        .add("_1", IntegerType, true)
        .add("_2", IntegerType, true)
        .add("_3", IntegerType, true)
        .add("_4", IntegerType, true), true)
      .add("idx", IntegerType, false)
    assert(unionDf.schema == schema)
  }

  test("SPARK-32376: Make unionByName null-filling behavior work with struct columns - nested") {
    val df1 = Seq((0, UnionClass1a(0, 1L, UnionClass2(1, "2")))).toDF("id", "a")
    val df2 = Seq((1, UnionClass1b(1, 2L, UnionClass3(2, 3L)))).toDF("id", "a")

    var unionDf = df1.unionByName(df2, true)
    val schema1 = new StructType()
      .add("id", IntegerType, false)
      .add("a", new StructType()
        .add("a", IntegerType, true)
        .add("b", LongType, true)
        .add("nested", new StructType()
          .add("a", IntegerType, true)
          .add("c", StringType, true)
          .add("b", LongType, true), true), true)
    assert(unionDf.schema == schema1)
    checkAnswer(unionDf,
      Row(0, Row(0, 1, Row(1, "2", null))) ::
        Row(1, Row(1, 2, Row(2, null, 3L))) :: Nil)

    unionDf = df2.unionByName(df1, true)
    val schema2 = new StructType()
      .add("id", IntegerType, false)
      .add("a", new StructType()
        .add("a", IntegerType, true)
        .add("b", LongType, true)
        .add("nested", new StructType()
          .add("a", IntegerType, true)
          .add("b", LongType, true)
          .add("c", StringType, true), true), true)
    assert(unionDf.schema== schema2)
    checkAnswer(unionDf,
      Row(1, Row(1, 2, Row(2, 3L, null))) ::
        Row(0, Row(0, 1, Row(1, null, "2"))) :: Nil)

    val df3 = Seq((2, UnionClass1b(2, 3L, null))).toDF("id", "a")
    unionDf = df1.unionByName(df3, true)
    assert(unionDf.schema == schema1)
    checkAnswer(unionDf,
      Row(0, Row(0, 1, Row(1, "2", null))) ::
        Row(2, Row(2, 3, null)) :: Nil)
  }

  test("SPARK-32376: Make unionByName null-filling behavior work with struct columns" +
      " - case-sensitive cases") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val df1 = Seq((0, UnionClass1a(0, 1L, UnionClass2(1, "2")))).toDF("id", "a")
      val df2 = Seq((1, UnionClass1c(1, 2L, UnionClass4(2, 3L)))).toDF("id", "a")

      var unionDf = df1.unionByName(df2, true)
      var schema = new StructType()
        .add("id", IntegerType, false)
        .add("a", new StructType()
          .add("a", IntegerType, true)
          .add("b", LongType, true)
          .add("nested", new StructType()
            .add("a", IntegerType, true)
            .add("c", StringType, true)
            .add("A", IntegerType, true)
            .add("b", LongType, true), true), true)
      assert(unionDf.schema == schema)
      checkAnswer(unionDf,
        Row(0, Row(0, 1, Row(1, "2", null, null))) ::
          Row(1, Row(1, 2, Row(null, null, 2, 3L))) :: Nil)

      unionDf = df2.unionByName(df1, true)
      schema = new StructType()
        .add("id", IntegerType, false)
        .add("a", new StructType()
          .add("a", IntegerType, true)
          .add("b", LongType, true)
          .add("nested", new StructType()
            .add("A", IntegerType, true)
            .add("b", LongType, true)
            .add("a", IntegerType, true)
            .add("c", StringType, true), true), true)
      assert(unionDf.schema == schema)
      checkAnswer(unionDf,
        Row(1, Row(1, 2, Row(2, 3L, null, null))) ::
          Row(0, Row(0, 1, Row(null, null, 1, "2"))) :: Nil)

      val df3 = Seq((2, UnionClass1b(2, 3L, UnionClass3(4, 5L)))).toDF("id", "a")
      unionDf = df2.unionByName(df3, true)
      schema = new StructType()
        .add("id", IntegerType, false)
        .add("a", new StructType()
          .add("a", IntegerType, true)
          .add("b", LongType, true)
          .add("nested", new StructType()
            .add("A", IntegerType, true)
            .add("b", LongType, true)
            .add("a", IntegerType, true), true), true)
      assert(unionDf.schema == schema)
      checkAnswer(unionDf,
        Row(1, Row(1, 2, Row(2, 3L, null))) ::
          Row(2, Row(2, 3, Row(null, 5L, 4))) :: Nil)
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

    val df1 = spark.createDataFrame(
      sparkContext.parallelize(Row(nestedStructValues1) :: Nil),
      StructType(Seq(StructField("topLevelCol", nestedStructType1))))

    val df2 = spark.createDataFrame(
      sparkContext.parallelize(Row(nestedStructValues2) :: Nil),
      StructType(Seq(StructField("topLevelCol", nestedStructType2))))

    val union = df1.unionByName(df2, allowMissingColumns = true)
    assert(union.schema.toDDL == "topLevelCol STRUCT<b: STRING, a: STRING>")
    checkAnswer(union, Row(Row("b", null)) :: Row(Row("b", "a")) :: Nil)
  }

  test("SPARK-35290: Make unionByName null-filling behavior work with struct columns"
      + " - sorting edge case") {
    val nestedStructType1 = StructType(Seq(
      StructField("b", StructType(Seq(
        StructField("ba", StringType)
      )))
    ))
    val nestedStructValues1 = Row(Row("ba"))

    val nestedStructType2 = StructType(Seq(
      StructField("a", StructType(Seq(
        StructField("aa", StringType)
      ))),
      StructField("b", StructType(Seq(
        StructField("bb", StringType)
      )))
    ))
    val nestedStructValues2 = Row(Row("aa"), Row("bb"))

    val df1 = spark.createDataFrame(
      sparkContext.parallelize(Row(nestedStructValues1) :: Nil),
      StructType(Seq(StructField("topLevelCol", nestedStructType1))))

    val df2 = spark.createDataFrame(
      sparkContext.parallelize(Row(nestedStructValues2) :: Nil),
      StructType(Seq(StructField("topLevelCol", nestedStructType2))))

    var unionDf = df1.unionByName(df2, true)
    assert(unionDf.schema.toDDL == "topLevelCol " +
      "STRUCT<b: STRUCT<ba: STRING, bb: STRING>, a: STRUCT<aa: STRING>>")
    checkAnswer(unionDf,
      Row(Row(Row("ba", null), null)) ::
      Row(Row(Row(null, "bb"), Row("aa"))) :: Nil)

    unionDf = df2.unionByName(df1, true)
    assert(unionDf.schema.toDDL == "topLevelCol STRUCT<a: STRUCT<aa: STRING>, " +
      "b: STRUCT<bb: STRING, ba: STRING>>")
    checkAnswer(unionDf,
      Row(Row(null, Row(null, "ba"))) ::
      Row(Row(Row("aa"), Row("bb", null))) :: Nil)
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

      val df = spark.createDataFrame(
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
      Row(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20),
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20),
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20),
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20),
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20),
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20),
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20),
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20),
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20),
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20))
    // scalastyle:on
    checkAnswer(union, row1 :: row2 :: Nil)
  }

  test("SPARK-34474: Remove unnecessary Union under Distinct") {
    Seq(RemoveNoopUnion.ruleName, "").map { ruleName =>
      withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ruleName) {
        val distinctUnionDF1 = testData.union(testData).distinct()
        checkAnswer(distinctUnionDF1, testData.distinct())


        val distinctUnionDF2 = testData.union(testData).dropDuplicates(Seq("key"))
        checkAnswer(distinctUnionDF2, testData.dropDuplicates(Seq("key")))

        val distinctUnionDF3 = sql(
          """
            |select key, value from testData
            |union
            |select key, value from testData
            |""".stripMargin)
        checkAnswer(distinctUnionDF3, testData.distinct())

        val distinctUnionDF4 = sql(
          """
            |select distinct key, expr
            |from
            |(
            |  select key, key + 1 as expr
            |  from testData
            |  union all
            |  select key, key + 2 as expr
            |  from testData
            |)
            |""".stripMargin)
        val expected = sql(
          """
            |select key, expr
            |from
            |(
            |  select key, key + 1 as expr
            |  from testData
            |  union all
            |  select key, key + 2 as expr
            |  from testData
            |) group by key, expr
            |""".stripMargin)
        checkAnswer(distinctUnionDF4, expected)
      }
    }
  }

  test("SPARK-34548: Remove unnecessary children from Union") {
    Seq(RemoveNoopUnion.ruleName, "").map { ruleName =>
      withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ruleName) {
        val testDataCopy = spark.sparkContext.parallelize(
          (1 to 100).map(i => SQLTestData.TestData(i, i.toString))).toDF()

        val distinctUnionDF1 = testData.union(testData).union(testDataCopy).distinct()
        val expected = testData.union(testDataCopy).distinct()
        checkAnswer(distinctUnionDF1, expected)

        val distinctUnionDF2 = testData.union(testData).union(testDataCopy)
          .dropDuplicates(Seq("key"))
        checkAnswer(distinctUnionDF2, expected)
      }
    }
  }

  test("SPARK-35756: unionByName support struct having same col names but different sequence") {
    // struct having same col names but different sequence
    var df1 = Seq(("d1", Struct1(1, 2))).toDF("a", "b")
    var df2 = Seq(("d2", Struct2(1, 2))).toDF("a", "b")
    var unionDF = df1.unionByName(df2)
    var expected = Row("d1", Row(1, 2)) :: Row("d2", Row(2, 1)) :: Nil
    val schema = StructType(Seq(StructField("a", StringType),
      StructField("b", StructType(Seq(StructField("c1", IntegerType),
        StructField("c2", IntegerType))))))

    assert(unionDF.schema === schema)
    checkAnswer(unionDF, expected)

    // nested struct, inner struct having different col name
    df1 = Seq((0, UnionClass1a(0, 1L, UnionClass2(1, "2")))).toDF("id", "a")
    df2 = Seq((1, UnionClass1b(1, 2L, UnionClass3(2, 3L)))).toDF("id", "a")
    checkError(
      exception = intercept[AnalysisException] {
        df1.unionByName(df2)
      },
      errorClass = "FIELD_NOT_FOUND",
      parameters = Map("fieldName" -> "`c`", "fields" -> "`a`, `b`"))

    // If right side of the nested struct has extra col.
    df1 = Seq((1, 2, UnionClass1d(1, 2, Struct3(1)))).toDF("a", "b", "c")
    df2 = Seq((1, 2, UnionClass1e(1, 2, Struct4(1, 5)))).toDF("a", "b", "c")
    val errMsg = intercept[AnalysisException] {
      df1.unionByName(df2)
    }.getMessage
    assert(errMsg.contains("Union can only be performed on tables with" +
      " compatible column types." +
      " The third column of the second table is struct<c1:int,c2:int,c3:struct<c3:int,c5:int>>" +
      " type which is not compatible with struct<c1:int,c2:int,c3:struct<c3:int>> at the same" +
      " column of the first table"))

    // diff Case sensitive attributes names and diff sequence scenario for unionByName
    df1 = Seq((1, 2, UnionClass1d(1, 2, Struct3(1)))).toDF("a", "b", "c")
    df2 = Seq((1, 2, UnionClass1f(1, 2, Struct3a(1)))).toDF("a", "b", "c")
    expected =
      Row(1, 2, Row(1, 2, Row(1))) :: Row(1, 2, Row(2, 1, Row(1))) :: Nil

    unionDF = df1.unionByName(df2)
    checkAnswer(unionDF, expected)

    df1 = Seq((1, Struct1(1, 2))).toDF("a", "b")
    df2 = Seq((1, Struct2a(1, 2))).toDF("a", "b")
    expected = Row(1, Row(1, 2)) :: Row(1, Row(2, 1)) :: Nil

    unionDF = df1.unionByName(df2)
    checkAnswer(unionDF, expected)
  }

  test("SPARK-36673: Only merge nullability for Unions of struct") {
    val df1 = spark.range(2).withColumn("nested", struct(expr("id * 5 AS INNER")))
    val df2 = spark.range(2).withColumn("nested", struct(expr("id * 5 AS inner")))

    val union1 = df1.union(df2)
    val union2 = df1.unionByName(df2)

    val schema = StructType(Seq(StructField("id", LongType, false),
      StructField("nested", StructType(Seq(StructField("INNER", LongType, false))), false)))

    Seq(union1, union2).foreach { df =>
      assert(df.schema == schema)
      assert(df.queryExecution.optimizedPlan.schema == schema)
      assert(df.queryExecution.executedPlan.schema == schema)

      checkAnswer(df, Row(0, Row(0)) :: Row(1, Row(5)) :: Row(0, Row(0)) :: Row(1, Row(5)) :: Nil)
      checkAnswer(df.select("nested.*"), Row(0) :: Row(5) :: Row(0) :: Row(5) :: Nil)
    }
  }

  test("SPARK-36797: Union should resolve nested columns as top-level columns") {
    // Different nested field names, but same nested field types. Union resolves column by position.
    val df1 = spark.range(2).withColumn("nested",
      struct(expr("id * 5 AS inner1"), struct(expr("id * 10 AS inner2"))))
    val df2 = spark.range(2).withColumn("nested",
      struct(expr("id * 5 AS inner2"), struct(expr("id * 10 AS inner1"))))

    checkAnswer(df1.union(df2),
      Row(0, Row(0, Row(0))) :: Row(0, Row(0, Row(0))) :: Row(1, Row(5, Row(10))) ::
        Row(1, Row(5, Row(10))) :: Nil)

    val df3 = spark.range(2).withColumn("nested array",
      array(struct(expr("id * 5 AS inner1"), struct(expr("id * 10 AS inner2")))))
    val df4 = spark.range(2).withColumn("nested",
      array(struct(expr("id * 5 AS inner2"), struct(expr("id * 10 AS inner1")))))

    checkAnswer(df3.union(df4),
      Row(0, Seq(Row(0, Row(0)))) :: Row(0, Seq(Row(0, Row(0)))) :: Row(1, Seq(Row(5, Row(10)))) ::
        Row(1, Seq(Row(5, Row(10)))) :: Nil)

    val df5 = spark.range(2).withColumn("nested array",
      map(struct(expr("id * 5 AS key1")),
        struct(expr("id * 5 AS inner1"), struct(expr("id * 10 AS inner2")))))
    val df6 = spark.range(2).withColumn("nested",
      map(struct(expr("id * 5 AS key2")),
        struct(expr("id * 5 AS inner2"), struct(expr("id * 10 AS inner1")))))

    checkAnswer(df5.union(df6),
      Row(0, Map(Row(0) -> Row(0, Row(0)))) ::
        Row(0, Map(Row(0) -> Row(0, Row(0)))) ::
        Row(1, Map(Row(5) ->Row(5, Row(10)))) ::
        Row(1, Map(Row(5) ->Row(5, Row(10)))) :: Nil)

    // Different nested field names, and different nested field types.
    val df7 = spark.range(2).withColumn("nested",
      struct(expr("id * 5 AS inner1"), struct(expr("id * 10 AS inner2").cast("string"))))
    val df8 = spark.range(2).withColumn("nested",
      struct(expr("id * 5 AS inner2").cast("string"), struct(expr("id * 10 AS inner1"))))

    val err = intercept[AnalysisException](df7.union(df8).collect())
    assert(err.message
      .contains("Union can only be performed on tables with compatible column types"))
  }

  test("SPARK-36546: Add unionByName support to arrays of structs") {
    val arrayType1 = ArrayType(
      StructType(Seq(
        StructField("ba", StringType),
        StructField("bb", StringType)
      ))
    )
    val arrayValues1 = Seq(Row("ba", "bb"))

    val arrayType2 = ArrayType(
      StructType(Seq(
        StructField("bb", StringType),
        StructField("ba", StringType)
      ))
    )
    val arrayValues2 = Seq(Row("bb", "ba"))

    val df1 = spark.createDataFrame(
      sparkContext.parallelize(Row(arrayValues1) :: Nil),
      StructType(Seq(StructField("arr", arrayType1))))

    val df2 = spark.createDataFrame(
      sparkContext.parallelize(Row(arrayValues2) :: Nil),
      StructType(Seq(StructField("arr", arrayType2))))

    var unionDf = df1.unionByName(df2)
    assert(unionDf.schema.toDDL == "arr ARRAY<STRUCT<ba: STRING, bb: STRING>>")
    checkAnswer(unionDf,
      Row(Seq(Row("ba", "bb"))) ::
      Row(Seq(Row("ba", "bb"))) :: Nil)

    unionDf = df2.unionByName(df1)
    assert(unionDf.schema.toDDL == "arr ARRAY<STRUCT<bb: STRING, ba: STRING>>")
    checkAnswer(unionDf,
      Row(Seq(Row("bb", "ba"))) ::
      Row(Seq(Row("bb", "ba"))) :: Nil)

    val arrayType3 = ArrayType(
      StructType(Seq(
        StructField("ba", StringType)
      ))
    )
    val arrayValues3 = Seq(Row("ba"))

    val arrayType4 = ArrayType(
      StructType(Seq(
        StructField("bb", StringType)
      ))
    )
    val arrayValues4 = Seq(Row("bb"))

    val df3 = spark.createDataFrame(
      sparkContext.parallelize(Row(arrayValues3) :: Nil),
      StructType(Seq(StructField("arr", arrayType3))))

    val df4 = spark.createDataFrame(
      sparkContext.parallelize(Row(arrayValues4) :: Nil),
      StructType(Seq(StructField("arr", arrayType4))))

    assertThrows[AnalysisException] {
      df3.unionByName(df4)
    }

    unionDf = df3.unionByName(df4, true)
    assert(unionDf.schema.toDDL == "arr ARRAY<STRUCT<ba: STRING, bb: STRING>>")
    checkAnswer(unionDf,
      Row(Seq(Row("ba", null))) ::
      Row(Seq(Row(null, "bb"))) :: Nil)

    assertThrows[AnalysisException] {
      df4.unionByName(df3)
    }

    unionDf = df4.unionByName(df3, true)
    assert(unionDf.schema.toDDL == "arr ARRAY<STRUCT<bb: STRING, ba: STRING>>")
    checkAnswer(unionDf,
      Row(Seq(Row("bb", null))) ::
      Row(Seq(Row(null, "ba"))) :: Nil)
  }

  test("SPARK-36546: Add unionByName support to nested arrays of structs") {
    val nestedStructType1 = StructType(Seq(
      StructField("b", ArrayType(
        StructType(Seq(
          StructField("ba", StringType),
          StructField("bb", StringType)
        ))
      ))
    ))
    val nestedStructValues1 = Row(Seq(Row("ba", "bb")))

    val nestedStructType2 = StructType(Seq(
      StructField("b", ArrayType(
        StructType(Seq(
          StructField("bb", StringType),
          StructField("ba", StringType)
        ))
      ))
    ))
    val nestedStructValues2 = Row(Seq(Row("bb", "ba")))

    val df1 = spark.createDataFrame(
      sparkContext.parallelize(Row(nestedStructValues1) :: Nil),
      StructType(Seq(StructField("topLevelCol", nestedStructType1))))

    val df2 = spark.createDataFrame(
      sparkContext.parallelize(Row(nestedStructValues2) :: Nil),
      StructType(Seq(StructField("topLevelCol", nestedStructType2))))

    var unionDf = df1.unionByName(df2)
    assert(unionDf.schema.toDDL == "topLevelCol " +
      "STRUCT<b: ARRAY<STRUCT<ba: STRING, bb: STRING>>>")
    checkAnswer(unionDf,
      Row(Row(Seq(Row("ba", "bb")))) ::
      Row(Row(Seq(Row("ba", "bb")))) :: Nil)

    unionDf = df2.unionByName(df1)
    assert(unionDf.schema.toDDL == "topLevelCol STRUCT<" +
      "b: ARRAY<STRUCT<bb: STRING, ba: STRING>>>")
    checkAnswer(unionDf,
      Row(Row(Seq(Row("bb", "ba")))) ::
      Row(Row(Seq(Row("bb", "ba")))) :: Nil)

    val nestedStructType3 = StructType(Seq(
      StructField("b", ArrayType(
        StructType(Seq(
          StructField("ba", StringType)
        ))
      ))
    ))
    val nestedStructValues3 = Row(Seq(Row("ba")))

    val nestedStructType4 = StructType(Seq(
      StructField("b", ArrayType(
        StructType(Seq(
          StructField("bb", StringType)
        ))
      ))
    ))
    val nestedStructValues4 = Row(Seq(Row("bb")))

    val df3 = spark.createDataFrame(
      sparkContext.parallelize(Row(nestedStructValues3) :: Nil),
      StructType(Seq(StructField("topLevelCol", nestedStructType3))))

    val df4 = spark.createDataFrame(
      sparkContext.parallelize(Row(nestedStructValues4) :: Nil),
      StructType(Seq(StructField("topLevelCol", nestedStructType4))))

    assertThrows[AnalysisException] {
      df3.unionByName(df4)
    }

    unionDf = df3.unionByName(df4, true)
    assert(unionDf.schema.toDDL == "topLevelCol " +
      "STRUCT<b: ARRAY<STRUCT<ba: STRING, bb: STRING>>>")
    checkAnswer(unionDf,
      Row(Row(Seq(Row("ba", null)))) ::
      Row(Row(Seq(Row(null, "bb")))) :: Nil)

    assertThrows[AnalysisException] {
      df4.unionByName(df3)
    }

    unionDf = df4.unionByName(df3, true)
    assert(unionDf.schema.toDDL == "topLevelCol STRUCT<" +
      "b: ARRAY<STRUCT<bb: STRING, ba: STRING>>>")
    checkAnswer(unionDf,
      Row(Row(Seq(Row("bb", null)))) ::
      Row(Row(Seq(Row(null, "ba")))) :: Nil)
  }

  test("SPARK-36546: Add unionByName support to multiple levels of nested arrays of structs") {
    val nestedStructType1 = StructType(Seq(
      StructField("b", ArrayType(
        ArrayType(
          StructType(Seq(
            StructField("ba", StringType),
            StructField("bb", StringType)
          ))
        )
      ))
    ))
    val nestedStructValues1 = Row(Seq(Seq(Row("ba", "bb"))))

    val nestedStructType2 = StructType(Seq(
      StructField("b", ArrayType(
        ArrayType(
          StructType(Seq(
            StructField("bb", StringType),
            StructField("ba", StringType)
          ))
        )
      ))
    ))
    val nestedStructValues2 = Row(Seq(Seq(Row("bb", "ba"))))

    val df1 = spark.createDataFrame(
      sparkContext.parallelize(Row(nestedStructValues1) :: Nil),
      StructType(Seq(StructField("topLevelCol", nestedStructType1))))

    val df2 = spark.createDataFrame(
      sparkContext.parallelize(Row(nestedStructValues2) :: Nil),
      StructType(Seq(StructField("topLevelCol", nestedStructType2))))

    var unionDf = df1.unionByName(df2)
    assert(unionDf.schema.toDDL == "topLevelCol " +
      "STRUCT<b: ARRAY<ARRAY<STRUCT<ba: STRING, bb: STRING>>>>")
    checkAnswer(unionDf,
      Row(Row(Seq(Seq(Row("ba", "bb"))))) ::
      Row(Row(Seq(Seq(Row("ba", "bb"))))) :: Nil)

    unionDf = df2.unionByName(df1)
    assert(unionDf.schema.toDDL == "topLevelCol STRUCT<" +
      "b: ARRAY<ARRAY<STRUCT<bb: STRING, ba: STRING>>>>")
    checkAnswer(unionDf,
      Row(Row(Seq(Seq(Row("bb", "ba"))))) ::
      Row(Row(Seq(Seq(Row("bb", "ba"))))) :: Nil)

    val nestedStructType3 = StructType(Seq(
      StructField("b", ArrayType(
        ArrayType(
          StructType(Seq(
            StructField("ba", StringType)
          ))
        )
      ))
    ))
    val nestedStructValues3 = Row(Seq(Seq(Row("ba"))))

    val nestedStructType4 = StructType(Seq(
      StructField("b", ArrayType(
        ArrayType(
          StructType(Seq(
            StructField("bb", StringType)
          ))
        )
      ))
    ))
    val nestedStructValues4 = Row(Seq(Seq(Row("bb"))))

    val df3 = spark.createDataFrame(
      sparkContext.parallelize(Row(nestedStructValues3) :: Nil),
      StructType(Seq(StructField("topLevelCol", nestedStructType3))))

    val df4 = spark.createDataFrame(
      sparkContext.parallelize(Row(nestedStructValues4) :: Nil),
      StructType(Seq(StructField("topLevelCol", nestedStructType4))))

    assertThrows[AnalysisException] {
      df3.unionByName(df4)
    }

    unionDf = df3.unionByName(df4, true)
    assert(unionDf.schema.toDDL == "topLevelCol " +
      "STRUCT<b: ARRAY<ARRAY<STRUCT<ba: STRING, bb: STRING>>>>")
    checkAnswer(unionDf,
      Row(Row(Seq(Seq(Row("ba", null))))) ::
      Row(Row(Seq(Seq(Row(null, "bb"))))) :: Nil)

    assertThrows[AnalysisException] {
      df4.unionByName(df3)
    }

    unionDf = df4.unionByName(df3, true)
    assert(unionDf.schema.toDDL == "topLevelCol STRUCT<" +
      "b: ARRAY<ARRAY<STRUCT<bb: STRING, ba: STRING>>>>")
    checkAnswer(unionDf,
      Row(Row(Seq(Seq(Row("bb", null))))) ::
      Row(Row(Seq(Seq(Row(null, "ba"))))) :: Nil)
  }

  test("SPARK-37371: UnionExec should support columnar if all children support columnar") {
    def checkIfColumnar(
        plan: SparkPlan,
        targetPlan: (SparkPlan) => Boolean,
        isColumnar: Boolean): Unit = {
      val target = plan.collect {
        case p if targetPlan(p) => p
      }
      assert(target.nonEmpty)
      assert(target.forall(_.supportsColumnar == isColumnar))
    }

    Seq(true, false).foreach { supported =>
      withSQLConf(SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> supported.toString) {
        val df1 = Seq(1, 2, 3).toDF("i").cache()
        val df2 = Seq(4, 5, 6).toDF("j").cache()

        val union = df1.union(df2)
        checkIfColumnar(union.queryExecution.executedPlan,
          _.isInstanceOf[InMemoryTableScanExec], supported)
        checkIfColumnar(union.queryExecution.executedPlan,
          _.isInstanceOf[InMemoryTableScanExec], supported)
        checkIfColumnar(union.queryExecution.executedPlan, _.isInstanceOf[UnionExec], supported)
        checkAnswer(union, Row(1) :: Row(2) :: Row(3) :: Row(4) :: Row(5) :: Row(6) :: Nil)

        val nonColumnarUnion = df1.union(Seq(7, 8, 9).toDF("k"))
        checkIfColumnar(nonColumnarUnion.queryExecution.executedPlan,
          _.isInstanceOf[UnionExec], false)
        checkAnswer(nonColumnarUnion,
          Row(1) :: Row(2) :: Row(3) :: Row(7) :: Row(8) :: Row(9) :: Nil)
      }
    }
  }
}

case class UnionClass1a(a: Int, b: Long, nested: UnionClass2)
case class UnionClass1b(a: Int, b: Long, nested: UnionClass3)
case class UnionClass1c(a: Int, b: Long, nested: UnionClass4)
case class UnionClass1d(c1: Int, c2: Int, c3: Struct3)
case class UnionClass1e(c2: Int, c1: Int, c3: Struct4)
case class UnionClass1f(c2: Int, c1: Int, c3: Struct3a)

case class UnionClass2(a: Int, c: String)
case class UnionClass3(a: Int, b: Long)
case class UnionClass4(A: Int, b: Long)
case class Struct1(c1: Int, c2: Int)
case class Struct2(c2: Int, c1: Int)
case class Struct2a(C2: Int, c1: Int)
case class Struct3(c3: Int)
case class Struct3a(C3: Int)
case class Struct4(c3: Int, c5: Int)
