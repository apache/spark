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

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, AttributeReference, BinaryExpression, PythonUDF, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{Expand, Generate, Join, Project, ScriptInputOutputSchema, ScriptTransformation, Window => WindowPlan}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, explode, sum, year}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.test.SQLTestData.TestData
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}

class DataFrameSelfJoinSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("join - join using self join") {
    val df = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str")

    // self join
    checkAnswer(
      df.join(df, "int"),
      Row(1, "1", "1") :: Row(2, "2", "2") :: Row(3, "3", "3") :: Nil)
  }

  test("join - self join") {
    val df1 = testData.select(testData("key")).as("df1")
    val df2 = testData.select(testData("key")).as("df2")

    checkAnswer(
      df1.join(df2, $"df1.key" === $"df2.key"),
      sql("SELECT a.key, b.key FROM testData a JOIN testData b ON a.key = b.key")
        .collect().toSeq)
  }

  test("join - self join auto resolve ambiguity with case insensitivity") {
    val df = Seq((1, "1"), (2, "2")).toDF("key", "value")
    checkAnswer(
      df.join(df, df("key") === df("Key")),
      Row(1, "1", 1, "1") :: Row(2, "2", 2, "2") :: Nil)

    checkAnswer(
      df.join(df.filter($"value" === "2"), df("key") === df("Key")),
      Row(2, "2", 2, "2") :: Nil)
  }

  test("join - using aliases after self join") {
    val df = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str")
    checkAnswer(
      df.as("x").join(df.as("y"), $"x.str" === $"y.str").groupBy("x.str").count(),
      Row("1", 1) :: Row("2", 1) :: Row("3", 1) :: Nil)

    checkAnswer(
      df.as("x").join(df.as("y"), $"x.str" === $"y.str").groupBy("y.str").count(),
      Row("1", 1) :: Row("2", 1) :: Row("3", 1) :: Nil)
  }

  test("[SPARK-6231] join - self join auto resolve ambiguity") {
    val df = Seq((1, "1"), (2, "2")).toDF("key", "value")
    checkAnswer(
      df.join(df, df("key") === df("key")),
      Row(1, "1", 1, "1") :: Row(2, "2", 2, "2") :: Nil)

    checkAnswer(
      df.join(df.filter($"value" === "2"), df("key") === df("key")),
      Row(2, "2", 2, "2") :: Nil)

    checkAnswer(
      df.join(df, df("key") === df("key") && df("value") === 1),
      Row(1, "1", 1, "1") :: Nil)

    val left = df.groupBy("key").agg(count("*"))
    val right = df.groupBy("key").agg(sum("key"))
    checkAnswer(
      left.join(right, left("key") === right("key")),
      Row(1, 1, 1, 1) :: Row(2, 1, 2, 2) :: Nil)
  }

  private def assertAmbiguousSelfJoin(df: => DataFrame): Unit = {
    val e = intercept[AnalysisException](df)
    assert(e.message.contains("ambiguous"))
  }

  private def assertCorrectResolution(
      df: => DataFrame,
      leftResolution: Resolution.Resolution,
      rightResolution: Resolution.Resolution): Unit = {
    val join = df.queryExecution.analyzed.asInstanceOf[Join]
    val binaryCondition = join.condition.get.asInstanceOf[BinaryExpression]
    leftResolution match {
      case Resolution.LeftConditionToLeftLeg =>
        assert(join.left.outputSet.contains(binaryCondition.left.references.head))
      case Resolution.LeftConditionToRightLeg =>
        assert(join.right.outputSet.contains(binaryCondition.left.references.head))
    }

    rightResolution match {
      case Resolution.RightConditionToLeftLeg =>
        assert(join.left.outputSet.contains(binaryCondition.right.references.head))
      case Resolution.RightConditionToRightLeg =>
        assert(join.right.outputSet.contains(binaryCondition.right.references.head))
    }
  }

  test("SPARK-28344: NOT AN ambiguous self join - column ref in join condition") {
    Seq(true, false).foreach(fail => {
      withSQLConf(
        SQLConf.FAIL_AMBIGUOUS_SELF_JOIN_ENABLED.key -> fail.toString,
        SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
        val df1 = spark.range(3)
        val df2 = df1.filter($"id" > 0)
        // `df1("id") > df2("id")` is always false.
        checkAnswer(df1.join(df2, df1("id") > df2("id")), Seq(Row(2, 1)))
        assertCorrectResolution(df1.join(df2, df1("id") > df2("id")),
          Resolution.LeftConditionToLeftLeg, Resolution.RightConditionToRightLeg)

        // Alias the dataframe and use qualified column names to eliminate all possibilities
        // of ambiguity in self-join.
        val aliasedDf1 = df1.alias("left")
        val aliasedDf2 = df2.as("right")
        checkAnswer(
          aliasedDf1.join(aliasedDf2, $"left.id" > $"right.id"),
          Seq(Row(2, 1)))
        assertCorrectResolution(aliasedDf1.join(aliasedDf2, $"left.id" > $"right.id"),
          Resolution.LeftConditionToLeftLeg, Resolution.RightConditionToRightLeg)
      }
    })
  }

  test("SPARK-28344: Not AN ambiguous self join - Dataset.colRegex as column ref") {
    Seq(true, false).foreach(fail => {
      withSQLConf(
        SQLConf.FAIL_AMBIGUOUS_SELF_JOIN_ENABLED.key -> fail.toString,
        SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
        val df1 = spark.range(3)
        val df2 = df1.filter($"id" > 0)
        assertCorrectResolution(df1.join(df2, df1.colRegex("id") > df2.colRegex("id")),
          Resolution.LeftConditionToLeftLeg, Resolution.RightConditionToRightLeg)
      }
    })
  }

  test("SPARK-28344: Not An ambiguous self join - Dataset.col with nested field") {
    Seq(true, false).foreach(fail => {
      withSQLConf(
        SQLConf.FAIL_AMBIGUOUS_SELF_JOIN_ENABLED.key -> fail.toString,
        SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
        val df1 = spark.read.json(Seq("""{"a": {"b": 1, "c": 1}}""").toDS())
        val df2 = df1.filter($"a.b" > 0)
        assertCorrectResolution(df1.join(df2, df1("a.b") > df2("a.c")),
          Resolution.LeftConditionToLeftLeg, Resolution.RightConditionToRightLeg)
      }
    })
  }

  test("SPARK-28344: Not an ambiguous  - column ref in Project") {
    Seq(true, false).foreach(fail => {
      withSQLConf(
        SQLConf.FAIL_AMBIGUOUS_SELF_JOIN_ENABLED.key -> fail.toString,
        SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
        val df1 = spark.range(3)
        val df2 = df1.filter($"id" > 0)
        // `df2("id")` actually points to the column of `df1`.
        checkAnswer(df1.join(df2).select(df2("id")), Seq(1, 1, 1, 2, 2, 2).map(Row(_)))

        // Alias the dataframe and use qualified column names can fix ambiguous self-join.
        val aliasedDf1 = df1.alias("left")
        val aliasedDf2 = df2.as("right")
        checkAnswer(
          aliasedDf1.join(aliasedDf2).select($"right.id"),
          Seq(1, 1, 1, 2, 2, 2).map(Row(_)))

        val proj1 = df1.join(df2).select(df2("id")).queryExecution.analyzed.asInstanceOf[Project]
        val join1 = proj1.child.asInstanceOf[Join]
        assert(proj1.projectList(0).references.subsetOf(join1.right.outputSet))
      }
    })
  }

  test("SPARK-28344: fail ambiguous self join - join three tables") {
    val df1 = spark.range(3)
    val df2 = df1.filter($"id" > 0)
    val df3 = df1.filter($"id" <= 2)
    val df4 = spark.range(1)

    withSQLConf(
      SQLConf.FAIL_AMBIGUOUS_SELF_JOIN_ENABLED.key -> "false",
      SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      // Here df3("id") is unambiguous, df2("id") is ambiguous. default resolves to df1
      checkAnswer(df1.join(df2).join(df3, df2("id") < df3("id")),
        Seq(Row(0, 1, 1), Row(0, 2, 1), Row(0, 1, 2), Row(0, 2, 2), Row(1, 1, 2), Row(1, 2, 2)))
      // `df2("id")` actually points to the column of `df1`.
    checkAnswer(
        df1.join(df4).join(df2).select(df2("id")),
        Seq(1, 2, 1, 2, 1, 2).map(Row(_)))
      // `df4("id")` is not ambiguous.
      checkAnswer(
        df1.join(df4).join(df2).select(df4("id")),
        Seq(0, 0, 0, 0, 0, 0).map(Row(_)))

      // Alias the dataframe and use qualified column names can fix ambiguous self-join.
      val aliasedDf1 = df1.alias("x")
      val aliasedDf2 = df2.as("y")
      val aliasedDf3 = df3.as("z")
      checkAnswer(
        aliasedDf1.join(aliasedDf2).join(aliasedDf3, $"y.id" < $"z.id"),
        Seq(Row(0, 1, 2), Row(1, 1, 2), Row(2, 1, 2)))
      checkAnswer(
        aliasedDf1.join(df4).join(aliasedDf2).select($"y.id"),
        Seq(1, 1, 1, 2, 2, 2).map(Row(_)))
    }

    withSQLConf(
      SQLConf.FAIL_AMBIGUOUS_SELF_JOIN_ENABLED.key -> "true",
      SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      assertAmbiguousSelfJoin(df1.join(df2).join(df3, df2("id") < df3("id")))

      val proj1 = df1.join(df4).join(df2).select(df2("id")).queryExecution.analyzed.
        asInstanceOf[Project]
      val join1 = proj1.child.asInstanceOf[Join]
      assert(proj1.projectList(0).references.subsetOf(join1.right.outputSet))
    }
  }

  test("SPARK-28344: don't fail if there is no ambiguous self join") {
    Seq(true, false).foreach(fail => {
      withSQLConf(
        SQLConf.FAIL_AMBIGUOUS_SELF_JOIN_ENABLED.key -> fail.toString) {
        val df = Seq(1, 1, 2, 2).toDF("a")
        val w = Window.partitionBy(df("a"))
        checkAnswer(
          df.select(df("a").alias("x"), sum(df("a")).over(w)),
          Seq((1, 2), (1, 2), (2, 4), (2, 4)).map(Row.fromTuple))

        val joined = df.join(spark.range(1)).select($"a")
        checkAnswer(
          joined.select(joined("a").alias("x"), sum(joined("a")).over(w)),
          Seq((1, 2), (1, 2), (2, 4), (2, 4)).map(Row.fromTuple))
      }
    })
  }

  test("SPARK-33071/SPARK-33536: Avoid changing dataset_id of LogicalPlan in join() " +
    "to not break DetectAmbiguousSelfJoin") {
    val emp1 = Seq[TestData](
      TestData(1, "sales"),
      TestData(2, "personnel"),
      TestData(3, "develop"),
      TestData(4, "IT")).toDS()
    val emp2 = Seq[TestData](
      TestData(1, "sales"),
      TestData(2, "personnel"),
      TestData(3, "develop")).toDS()
    val emp3 = emp1.join(emp2, emp1("key") === emp2("key")).select(emp1("*"))

    assertCorrectResolution(emp1.join(emp3, emp1.col("key") === emp3.col("key")),
      Resolution.LeftConditionToLeftLeg, Resolution.RightConditionToRightLeg)

    val proj1 = emp1.join(emp3, emp1.col("key") === emp3.col("key"),
      "left_outer").select(emp1.col("*"), emp3.col("key").as("e2")).
      queryExecution.analyzed.asInstanceOf[Project]
    val join1 = proj1.child.asInstanceOf[Join]
    assert(proj1.projectList(0).references.subsetOf(join1.left.outputSet))
    assert(proj1.projectList(1).references.subsetOf(join1.left.outputSet))
    assert(proj1.projectList(2).references.subsetOf(join1.right.outputSet))
  }

  test("df.show() should also not change dataset_id of LogicalPlan") {
    val df = Seq[TestData](
      TestData(1, "sales"),
      TestData(2, "personnel"),
      TestData(3, "develop"),
      TestData(4, "IT")).toDF()
    val ds_id1 = df.logicalPlan.getTagValue(Dataset.DATASET_ID_TAG)
    df.show(0)
    val ds_id2 = df.logicalPlan.getTagValue(Dataset.DATASET_ID_TAG)
    assert(ds_id1 === ds_id2)
  }

  test("SPARK-34200: ambiguous column reference should consider attribute availability") {
    withTable("t") {
      sql("CREATE TABLE t USING json AS SELECT 1 a, 2 b")
      val df1 = spark.table("t")
      val df2 = df1.select("a")
      checkAnswer(df1.join(df2, df1("b") === 2), Row(1, 2, 1))
    }
  }

  test("SPARK-35454: __dataset_id and __col_position should be correctly set") {
    val ds = Seq[TestData](
      TestData(1, "sales"),
      TestData(2, "personnel"),
      TestData(3, "develop"),
      TestData(4, "IT")).toDS()
    var dsIdSetOpt = ds.logicalPlan.getTagValue(Dataset.DATASET_ID_TAG)
    assert(dsIdSetOpt.get.size === 1)
    var col1DsId = -1L
    val col1 = ds.col("key")
    col1.expr.foreach {
      case a: AttributeReference =>
        col1DsId = a.metadata.getLong(Dataset.DATASET_ID_KEY)
        assert(dsIdSetOpt.get.contains(col1DsId))
        assert(a.metadata.getLong(Dataset.COL_POS_KEY) === 0)
    }

    val df = ds.toDF()
    dsIdSetOpt = df.logicalPlan.getTagValue(Dataset.DATASET_ID_TAG)
    assert(dsIdSetOpt.get.size === 2)
    var col2DsId = -1L
    val col2 = df.col("key")
    col2.expr.foreach {
      case a: AttributeReference =>
        col2DsId = a.metadata.getLong(Dataset.DATASET_ID_KEY)
        assert(dsIdSetOpt.get.contains(a.metadata.getLong(Dataset.DATASET_ID_KEY)))
        assert(a.metadata.getLong(Dataset.COL_POS_KEY) === 0)
    }
    assert(col1DsId !== col2DsId)
  }

  test("SPARK-35454: Not an ambiguous self join - toDF") {
    Seq(true, false).foreach(fail => {
      withSQLConf(
        SQLConf.FAIL_AMBIGUOUS_SELF_JOIN_ENABLED.key -> fail.toString,
        SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
        val df1 = spark.range(3).toDF()
        val df2 = df1.filter($"id" > 0).toDF()
        assertCorrectResolution(df1.join(df2, df1.col("id") > df2.col("id")),
          Resolution.LeftConditionToLeftLeg, Resolution.RightConditionToRightLeg)
      }
    })
  }

  test("SPARK-35454: fail ambiguous self join - join four tables") {
    val df1 = spark.range(3).select($"id".as("a"), $"id".as("b"))
    val df2 = df1.filter($"a" > 0).select("b")
    val df3 = df1.filter($"a" <= 2).select("b")
    val df4 = df1.filter($"b" <= 2).as("temp")
    val df5 = spark.range(1)

    withSQLConf(
      SQLConf.FAIL_AMBIGUOUS_SELF_JOIN_ENABLED.key -> "false",
      SQLConf.CROSS_JOINS_ENABLED.key -> "true") {

      //  df4("b") is unambiguous
      checkAnswer(df1.join(df2).join(df3).join(df4, df2("b") < df4("b")),
        Seq(
          Row(0, 0, 1, 0, 1, 1),
          Row(0, 0, 1, 1, 1, 1),
          Row(0, 0, 1, 2, 1, 1),
          Row(0, 0, 2, 0, 1, 1),
          Row(0, 0, 2, 1, 1, 1),
          Row(0, 0, 2, 2, 1, 1),
          Row(0, 0, 1, 0, 2, 2),
          Row(0, 0, 1, 1, 2, 2),
          Row(0, 0, 1, 2, 2, 2),
          Row(0, 0, 2, 0, 2, 2),
          Row(0, 0, 2, 1, 2, 2),
          Row(0, 0, 2, 2, 2, 2),
          Row(1, 1, 1, 0, 2, 2),
          Row(1, 1, 1, 1, 2, 2),
          Row(1, 1, 1, 2, 2, 2),
          Row(1, 1, 2, 0, 2, 2),
          Row(1, 1, 2, 1, 2, 2),
          Row(1, 1, 2, 2, 2, 2)
        ))
      // `df2("b")` actually points to the column of `df1`.
      checkAnswer(
        df1.join(df2).join(df5).join(df4).select(df2("b")),
        Seq(0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2).map(Row(_)))
      // `df5("id")` is not ambiguous.
      checkAnswer(
        df1.join(df5).join(df3).select(df5("id")),
        Seq(0, 0, 0, 0, 0, 0, 0, 0, 0).map(Row(_)))

      // Alias the dataframe and use qualified column names can fix ambiguous self-join.
      val aliasedDf1 = df1.alias("w")
      val aliasedDf2 = df2.as("x")
      val aliasedDf3 = df3.as("y")
      val aliasedDf4 = df3.as("z")
      checkAnswer(
        aliasedDf1.join(aliasedDf2).join(aliasedDf3).join(aliasedDf4, $"x.b" < $"y.b"),
        Seq(Row(0, 0, 1, 2, 0), Row(0, 0, 1, 2, 1), Row(0, 0, 1, 2, 2),
          Row(1, 1, 1, 2, 0), Row(1, 1, 1, 2, 1), Row(1, 1, 1, 2, 2),
          Row(2, 2, 1, 2, 0), Row(2, 2, 1, 2, 1), Row(2, 2, 1, 2, 2)))
      checkAnswer(
        aliasedDf1.join(df5).join(aliasedDf3).select($"y.b"),
        Seq(0, 0, 0, 1, 1, 1, 2, 2, 2).map(Row(_)))
    }

    withSQLConf(
      SQLConf.FAIL_AMBIGUOUS_SELF_JOIN_ENABLED.key -> "true",
      SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      assertAmbiguousSelfJoin(df1.join(df2).join(df3).join(df4, df2("b") < df4("b")))
      assertAmbiguousSelfJoin(df1.join(df2).join(df5).join(df4).select(df2("b")))
    }
  }

  test("SPARK-36874: DeduplicateRelations should copy dataset_id tag " +
    "to avoid ambiguous self join") {
    // Test for Project
    Seq(true, false).foreach(fail => {
      withSQLConf(
        SQLConf.FAIL_AMBIGUOUS_SELF_JOIN_ENABLED.key -> fail.toString) {
        val df1 = Seq((1, 2, "A1"), (2, 1, "A2")).toDF("key1", "key2", "value")
        val df2 = df1.filter($"value" === "A2")
        assertCorrectResolution(df1.join(df2, df1("key1") === df2("key2")),
          Resolution.LeftConditionToLeftLeg, Resolution.RightConditionToRightLeg)
        assertCorrectResolution(df2.join(df1, df1("key1") === df2("key2")),
          Resolution.LeftConditionToRightLeg, Resolution.RightConditionToLeftLeg)


        // Test for SerializeFromObject
        val df3 = spark.sparkContext.parallelize(1 to 10).map(x => (x, x)).toDF()
        val df4 = df3.filter($"_1" <=> 0)
        assertCorrectResolution(df3.join(df4, df3("_1") === df4("_2")),
          Resolution.LeftConditionToLeftLeg, Resolution.RightConditionToRightLeg)
        assertCorrectResolution(df4.join(df3, df3("_1") === df4("_2")),
          Resolution.LeftConditionToRightLeg, Resolution.RightConditionToLeftLeg)

        // Test For Aggregate
        val df5 = df1.groupBy($"key1").agg(count($"value") as "count")
        val df6 = df5.filter($"key1" > 0)
        assertCorrectResolution(df5.join(df6, df5("key1") === df6("count")),
          Resolution.LeftConditionToLeftLeg, Resolution.RightConditionToRightLeg)
        assertCorrectResolution(df6.join(df5, df5("key1") === df6("count")),
          Resolution.LeftConditionToRightLeg, Resolution.RightConditionToLeftLeg)

        // Test for MapInPandas
        val mapInPandasUDF = PythonUDF("mapInPandasUDF", null,
          StructType(Seq(StructField("x", LongType), StructField("y", LongType))),
          Seq.empty,
          PythonEvalType.SQL_MAP_PANDAS_ITER_UDF,
          true)
        val df7 = df1.mapInPandas(mapInPandasUDF)
        val df8 = df7.filter($"x" > 0)
        assertCorrectResolution(df7.join(df8, df7("x") === df8("y")),
          Resolution.LeftConditionToLeftLeg, Resolution.RightConditionToRightLeg)
        assertCorrectResolution(df8.join(df7, df7("x") === df8("y")),
          Resolution.LeftConditionToRightLeg, Resolution.RightConditionToLeftLeg)

        // Test for FlatMapGroupsInPandas
        val flatMapGroupsInPandasUDF = PythonUDF("flagMapGroupsInPandasUDF", null,
          StructType(Seq(StructField("x", LongType), StructField("y", LongType))),
          Seq.empty,
          PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
          true)
        val df9 = df1.groupBy($"key1").flatMapGroupsInPandas(flatMapGroupsInPandasUDF)
        val df10 = df9.filter($"x" > 0)
        assertCorrectResolution(df9.join(df10, df9("x") === df10("y")),
          Resolution.LeftConditionToLeftLeg, Resolution.RightConditionToRightLeg)
        assertCorrectResolution(df10.join(df9, df9("x") === df10("y")),
          Resolution.LeftConditionToRightLeg, Resolution.RightConditionToLeftLeg)

        // Test for FlatMapCoGroupsInPandas
        val flatMapCoGroupsInPandasUDF = PythonUDF("flagMapCoGroupsInPandasUDF", null,
          StructType(Seq(StructField("x", LongType), StructField("y", LongType))),
          Seq.empty,
          PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF,
          true)
        val df11 = df1.groupBy($"key1").flatMapCoGroupsInPandas(
          df1.groupBy($"key2"), flatMapCoGroupsInPandasUDF)
        val df12 = df11.filter($"x" > 0)
        assertCorrectResolution(df11.join(df12, df11("x") === df12("y")),
          Resolution.LeftConditionToLeftLeg, Resolution.RightConditionToRightLeg)
        assertCorrectResolution(df12.join(df11, df11("x") === df12("y")),
          Resolution.LeftConditionToRightLeg, Resolution.RightConditionToLeftLeg)

        // Test for AttachDistributedSequence
        val df13 = df1.withSequenceColumn("seq")
        val df14 = df13.filter($"value" === "A2")
        assertCorrectResolution(df13.join(df14, df13("key1") === df14("key2")),
          Resolution.LeftConditionToLeftLeg, Resolution.RightConditionToRightLeg)
        assertCorrectResolution(df14.join(df13, df13("key1") === df14("key2")),
          Resolution.LeftConditionToRightLeg, Resolution.RightConditionToLeftLeg)
        // Test for Generate
        // Ensure that the root of the plan is Generate
        val df15 = Seq((1, Seq(1, 2, 3))).toDF("a", "intList").select($"a", explode($"intList"))
          .queryExecution.optimizedPlan.find(_.isInstanceOf[Generate]).get.toDF()
        val df16 = df15.filter($"a" > 0)
        assertCorrectResolution(df15.join(df16, df15("a") === df16("col")),
          Resolution.LeftConditionToLeftLeg, Resolution.RightConditionToRightLeg)
        assertCorrectResolution(df16.join(df15, df15("a") === df16("col")),
          Resolution.LeftConditionToRightLeg, Resolution.RightConditionToLeftLeg)

        // Test for Expand
        // Ensure that the root of the plan is Expand
        val df17 =
        Expand(
          Seq(Seq($"key1".expr, $"key2".expr)),
          Seq(
            AttributeReference("x", IntegerType)(),
            AttributeReference("y", IntegerType)()),
          df1.queryExecution.logical).toDF()
        val df18 = df17.filter($"x" > 0)
        assertCorrectResolution(df17.join(df18, df17("x") === df18("y")),
          Resolution.LeftConditionToLeftLeg, Resolution.RightConditionToRightLeg)
        assertCorrectResolution(df18.join(df17, df17("x") === df18("y")),
          Resolution.LeftConditionToRightLeg, Resolution.RightConditionToLeftLeg)
        // Test for Window
        val dfWithTS = spark.sql("SELECT timestamp'2021-10-15 01:52:00' time, 1 a, 2 b")
        // Ensure that the root of the plan is Window
        val df19 = WindowPlan(
          Seq(Alias(dfWithTS("time").expr, "ts")()),
          Seq(dfWithTS("a").expr),
          Seq(SortOrder(dfWithTS("a").expr, Ascending)),
          dfWithTS.queryExecution.logical).toDF()
        val df20 = df19.filter($"a" > 0)
        assertCorrectResolution(df19.join(df20, df19("a") === df20("b")),
          Resolution.LeftConditionToLeftLeg, Resolution.RightConditionToRightLeg)
        assertCorrectResolution(df20.join(df19, df19("a") === df20("b")),
          Resolution.LeftConditionToRightLeg, Resolution.RightConditionToLeftLeg)
        // Test for ScriptTransformation
        val ioSchema =
          ScriptInputOutputSchema(
            Seq(("TOK_TABLEROWFORMATFIELD", ","),
              ("TOK_TABLEROWFORMATCOLLITEMS", "#"),
              ("TOK_TABLEROWFORMATMAPKEYS", "@"),
              ("TOK_TABLEROWFORMATNULL", "null"),
              ("TOK_TABLEROWFORMATLINES", "\n")),
            Seq(("TOK_TABLEROWFORMATFIELD", ","),
              ("TOK_TABLEROWFORMATCOLLITEMS", "#"),
              ("TOK_TABLEROWFORMATMAPKEYS", "@"),
              ("TOK_TABLEROWFORMATNULL", "null"),
              ("TOK_TABLEROWFORMATLINES", "\n")), None, None,
            List.empty, List.empty, None, None, false)
        // Ensure that the root of the plan is ScriptTransformation
        val df21 = ScriptTransformation(
          "cat",
          Seq(
            AttributeReference("x", IntegerType)(),
            AttributeReference("y", IntegerType)()),
          df1.queryExecution.logical,
          ioSchema).toDF()
        val df22 = df21.filter($"x" > 0)
        assertCorrectResolution(df21.join(df22, df21("x") === df22("y")),
          Resolution.LeftConditionToLeftLeg, Resolution.RightConditionToRightLeg)
        assertCorrectResolution(df22.join(df21, df21("x") === df22("y")),
          Resolution.LeftConditionToRightLeg, Resolution.RightConditionToLeftLeg)
      }
    })
  }

  test("SPARK-35937: GetDateFieldOperations should skip unresolved nodes") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val df = Seq("1644821603").map(i => (i.toInt, i)).toDF("tsInt", "tsStr")
      val df1 = df.select(df("tsStr").cast("timestamp")).as("df1")
      val df2 = df.select(df("tsStr").cast("timestamp")).as("df2")
      df1.join(df2, $"df1.tsStr" === $"df2.tsStr", "left_outer")
      val df3 = df1.join(df2, $"df1.tsStr" === $"df2.tsStr", "left_outer")
        .select($"df1.tsStr".as("timeStr")).as("df3")
      // Before the fix, it throws "UnresolvedException: Invalid call to
      // dataType on unresolved object".
      val ex = intercept[AnalysisException](
        df3.join(df1, year($"df1.timeStr") === year($"df3.tsStr"))
      )
      checkError(ex,
        errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        parameters = Map("objectName" -> "`df1`.`timeStr`",
          "proposal" -> "`df3`.`timeStr`, `df1`.`tsStr`"),
        context = ExpectedContext(fragment = "$", getCurrentClassCallSitePattern))
    }
  }

  test("SPARK-20897: cached self-join should not fail") {
    // force to plan sort merge join
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0") {
      val df = Seq(1 -> "a").toDF("i", "j")
      val df1 = df.as("t1")
      val df2 = df.as("t2")
      assert(df1.join(df2, $"t1.i" === $"t2.i").cache().count() == 1)
    }
  }

  test("SPARK-47217: deduplication of project causes ambiguity in resolution") {
    Seq(true, false).foreach(fail => {
      withSQLConf(
        SQLConf.FAIL_AMBIGUOUS_SELF_JOIN_ENABLED.key -> fail.toString) {
        val df = Seq((1, 2)).toDF("a", "b")
        val df2 = df.select(df("a").as("aa"), df("b").as("bb"))
        val df3 = df2.join(df, df2("bb") === df("b")).select(df2("aa"), df("a"))
        checkAnswer(
          df3,
          Row(1, 1) :: Nil)
      }
    })
  }

  test("SPARK-47217: deduplication in nested joins with join attribute aliased") {
    Seq(true, false).foreach(fail => {
      withSQLConf(
        SQLConf.FAIL_AMBIGUOUS_SELF_JOIN_ENABLED.key -> fail.toString) {
        val df1 = Seq((1, 2)).toDF("a", "b")
        val df2 = Seq((1, 2)).toDF("aa", "bb")
        val df1Joindf2 = df1.join(df2, df1("a") === df2("aa")).select(df1("a").as("aaa"),
          df2("aa"), df1("b"))

        assertCorrectResolution(df1Joindf2.join(df1, df1Joindf2("aaa") === df1("a")),
          Resolution.LeftConditionToLeftLeg, Resolution.RightConditionToRightLeg)

        assertCorrectResolution(df1.join(df1Joindf2, df1Joindf2("aaa") === df1("a")),
          Resolution.LeftConditionToRightLeg, Resolution.RightConditionToLeftLeg)

        val proj1 = df1Joindf2.join(df1, df1Joindf2("aaa") === df1("a")).select(df1Joindf2("aa"),
          df1("a")).queryExecution.analyzed.asInstanceOf[Project]
        val join1 = proj1.child.asInstanceOf[Join]
        assert(proj1.projectList(0).references.subsetOf(join1.left.outputSet))
        assert(proj1.projectList(1).references.subsetOf(join1.right.outputSet))

        val proj2 = df1.join(df1Joindf2, df1Joindf2("aaa") === df1("a")).select(df1Joindf2("aa"),
          df1("a")).queryExecution.analyzed.asInstanceOf[Project]
        val join2 = proj2.child.asInstanceOf[Join]
        assert(proj2.projectList(0).references.subsetOf(join2.right.outputSet))
        assert(proj2.projectList(1).references.subsetOf(join2.left.outputSet))
      }
    })
  }

  test("SPARK-47217: deduplication in nested joins without join attribute aliased") {
    Seq(true, false).foreach(fail => {
      withSQLConf(
        SQLConf.FAIL_AMBIGUOUS_SELF_JOIN_ENABLED.key -> fail.toString) {
        val df1 = Seq((1, 2)).toDF("a", "b")
        val df2 = Seq((1, 2)).toDF("aa", "bb")
        val df1Joindf2 = df1.join(df2, df1("a") === df2("aa")).select(df1("a"), df2("aa"), df1("b"))

        assertCorrectResolution(df1Joindf2.join(df1, df1Joindf2("a") === df1("a")),
          Resolution.LeftConditionToLeftLeg, Resolution.RightConditionToRightLeg)

        assertCorrectResolution(df1.join(df1Joindf2, df1Joindf2("a") === df1("a")),
          Resolution.LeftConditionToRightLeg, Resolution.RightConditionToLeftLeg)

        val proj1 = df1Joindf2.join(df1, df1Joindf2("a") === df1("a")).select(df1Joindf2("a"),
          df1("a")).queryExecution.analyzed.asInstanceOf[Project]
        val join1 = proj1.child.asInstanceOf[Join]
        assert(proj1.projectList(0).references.subsetOf(join1.left.outputSet))
        assert(proj1.projectList(1).references.subsetOf(join1.right.outputSet))

        val proj2 = df1.join(df1Joindf2, df1Joindf2("a") === df1("a")).select(df1Joindf2("a"),
          df1("a")).queryExecution.analyzed.asInstanceOf[Project]
        val join2 = proj2.child.asInstanceOf[Join]
        assert(proj2.projectList(0).references.subsetOf(join2.right.outputSet))
        assert(proj2.projectList(1).references.subsetOf(join2.left.outputSet))
      }
    })
  }
}

object Resolution extends Enumeration {
  type Resolution = Value

  val LeftConditionToLeftLeg, LeftConditionToRightLeg, RightConditionToRightLeg,
    RightConditionToLeftLeg = Value
}

