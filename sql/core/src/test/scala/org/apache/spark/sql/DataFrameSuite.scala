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

import org.apache.spark.SparkException
import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Cast, CreateMap, EqualTo, ExpressionSet, GreaterThan, Literal, PythonUDF, ScalarSubquery, Uuid}
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LeafNode, LocalRelation, LogicalPlan, OneRowRelation}
import org.apache.spark.sql.connector.FakeV2Provider
import org.apache.spark.sql.execution.{FilterExec, LogicalRDD, QueryExecution, SortExec, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec, ShuffleExchangeExec, ShuffleExchangeLike}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{ExamplePoint, ExamplePointUDT, SharedSparkSession}
import org.apache.spark.sql.test.SQLTestData.TestData2
import org.apache.spark.sql.types._
import org.apache.spark.tags.SlowSQLTest
import org.apache.spark.unsafe.types.CalendarInterval

@SlowSQLTest
class DataFrameSuite extends QueryTest
  with SharedDataFrameSuite
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {
  import testImplicits._

  test("global sorting - RDD") {
    checkAnswer(
      arrayData.toDF().orderBy($"data".getItem(0).asc),
      arrayData.toDF().collect().sortBy(_.getAs[Seq[Int]](0)(0)).toSeq)

    checkAnswer(
      arrayData.toDF().orderBy($"data".getItem(0).desc),
      arrayData.toDF().collect().sortBy(_.getAs[Seq[Int]](0)(0)).reverse.toSeq)

    checkAnswer(
      arrayData.toDF().orderBy($"data".getItem(1).asc),
      arrayData.toDF().collect().sortBy(_.getAs[Seq[Int]](0)(1)).toSeq)

    checkAnswer(
      arrayData.toDF().orderBy($"data".getItem(1).desc),
      arrayData.toDF().collect().sortBy(_.getAs[Seq[Int]](0)(1)).reverse.toSeq)
  }

  test("limit - RDD") {
    checkAnswer(
      arrayData.toDF().limit(1),
      arrayData.take(1).map(r => Row.fromSeq(r.productIterator.toSeq)))

    checkAnswer(
      mapData.toDF().limit(1),
      mapData.take(1).map(r => Row.fromSeq(r.productIterator.toSeq)))

    // SPARK-12340: overstep the bounds of Int in SparkPlan.executeTake
    checkAnswer(
      spark.range(2).toDF().limit(2147483638),
      Row(0) :: Row(1) :: Nil
    )
  }

  test("offset - RDD") {
    checkAnswer(
      arrayData.toDF().offset(99),
      arrayData.collect().drop(99).map(r => Row.fromSeq(r.productIterator.toSeq)))

    checkAnswer(
      mapData.toDF().offset(99),
      mapData.collect().drop(99).map(r => Row.fromSeq(r.productIterator.toSeq)))
  }

  test("limit with offset") {
    checkAnswer(
      testData.limit(10).offset(5),
      testData.take(10).drop(5).toSeq)

    checkAnswer(
      testData.offset(5).limit(10),
      testData.take(15).drop(5).toSeq)
  }

  test("createDataFrame(RDD[Row], StructType) should convert UDTs (SPARK-6672)") {
    val rowRDD = sparkContext.parallelize(Seq(Row(new ExamplePoint(1.0, 2.0))))
    val schema = StructType(Array(StructField("point", new ExamplePointUDT(), false)))
    val df = spark.createDataFrame(rowRDD, schema)
    df.rdd.collect()
  }

  test("SPARK-7324 dropDuplicates") {
    val testData = sparkContext.parallelize(
      (2, 1, 2) :: (1, 1, 1) ::
      (1, 2, 1) :: (2, 1, 2) ::
      (2, 2, 2) :: (2, 2, 1) ::
      (2, 1, 1) :: (1, 1, 2) ::
      (1, 2, 2) :: (1, 2, 1) :: Nil).toDF("key", "value1", "value2")

    checkAnswer(
      testData.dropDuplicates(),
      Seq(Row(2, 1, 2), Row(1, 1, 1), Row(1, 2, 1),
        Row(2, 2, 2), Row(2, 1, 1), Row(2, 2, 1),
        Row(1, 1, 2), Row(1, 2, 2)))

    checkAnswer(
      testData.dropDuplicates(Seq("key", "value1")),
      Seq(Row(2, 1, 2), Row(1, 2, 1), Row(1, 1, 1), Row(2, 2, 2)))

    checkAnswer(
      testData.dropDuplicates(Seq("value1", "value2")),
      Seq(Row(2, 1, 2), Row(1, 2, 1), Row(1, 1, 1), Row(2, 2, 2)))

    checkAnswer(
      testData.dropDuplicates(Seq("key")),
      Seq(Row(2, 1, 2), Row(1, 1, 1)))

    checkAnswer(
      testData.dropDuplicates(Seq("value1")),
      Seq(Row(2, 1, 2), Row(1, 2, 1)))

    checkAnswer(
      testData.dropDuplicates(Seq("value2")),
      Seq(Row(2, 1, 2), Row(1, 1, 1)))

    checkAnswer(
      testData.dropDuplicates("key", "value1"),
      Seq(Row(2, 1, 2), Row(1, 2, 1), Row(1, 1, 1), Row(2, 2, 2)))
  }

  test("SPARK-6941: Better error message for inserting into RDD-based Table") {
    val insertion = Seq(Tuple1(2)).toDF("col")

    // error case: insert into an OneRowRelation
    Dataset.ofRows(spark, OneRowRelation()).createOrReplaceTempView("one_row")
    checkError(
      exception = intercept[AnalysisException] {
        insertion.write.insertInto("one_row")
      },
      errorClass = "UNSUPPORTED_INSERT.RDD_BASED",
      parameters = Map.empty
    )
  }

  // This test case is to verify a bug when making a new instance of LogicalRDD.
  test("SPARK-11633: LogicalRDD throws TreeNode Exception: Failed to Copy Node") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val rdd = sparkContext.makeRDD(Seq(Row(1, 3), Row(2, 1)))
      val df = spark.createDataFrame(
        rdd,
        new StructType().add("f1", IntegerType).add("f2", IntegerType))
        .select($"F1", $"f2".as("f2"))
      val df1 = df.as("a")
      val df2 = df.as("b")
      checkAnswer(df1.join(df2, $"a.f2" === $"b.f2"), Row(1, 3, 1, 3) :: Row(2, 1, 2, 1) :: Nil)
    }
  }


  /**
   * Verifies that there is no Exchange between the Aggregations for `df`
   */
  private def verifyNonExchangingAgg(df: DataFrame) = {
    var atFirstAgg: Boolean = false
    df.queryExecution.executedPlan.foreach {
      case agg: HashAggregateExec =>
        atFirstAgg = !atFirstAgg
      case _ =>
        if (atFirstAgg) {
          fail("Should not have operators between the two aggregations")
        }
    }
  }

  /**
   * Verifies that there is an Exchange between the Aggregations for `df`
   */
  private def verifyExchangingAgg(df: DataFrame) = {
    var atFirstAgg: Boolean = false
    df.queryExecution.executedPlan.foreach {
      case agg: HashAggregateExec =>
        if (atFirstAgg) {
          fail("Should not have back to back Aggregates")
        }
        atFirstAgg = true
      case e: ShuffleExchangeExec => atFirstAgg = false
      case _ =>
    }
  }

  test("distributeBy and localSort") {
    val original = testData.repartition(1)
    assert(original.rdd.partitions.length == 1)
    val df = original.repartition(5, $"key")
    assert(df.rdd.partitions.length == 5)
    checkAnswer(original.select(), df.select())

    val df2 = original.repartition(10, $"key")
    assert(df2.rdd.partitions.length == 10)
    checkAnswer(original.select(), df2.select())

    // Group by the column we are distributed by. This should generate a plan with no exchange
    // between the aggregates
    val df3 = testData.repartition($"key").groupBy("key").count()
    verifyNonExchangingAgg(df3)
    verifyNonExchangingAgg(testData.repartition($"key", $"value")
      .groupBy("key", "value").count())

    // Grouping by just the first distributeBy expr, need to exchange.
    verifyExchangingAgg(testData.repartition($"key", $"value")
      .groupBy("key").count())

    val data = spark.sparkContext.parallelize(
      (1 to 100).map(i => TestData2(i % 10, i))).toDF()

    // Distribute and order by.
    val df4 = data.repartition(5, $"a").sortWithinPartitions($"b".desc)
    // Walk each partition and verify that it is sorted descending and does not contain all
    // the values.
    df4.rdd.foreachPartition { p =>
      // Skip empty partition
      if (p.hasNext) {
        var previousValue: Int = -1
        var allSequential: Boolean = true
        p.foreach { r =>
          val v: Int = r.getInt(1)
          if (previousValue != -1) {
            if (previousValue < v) throw new SparkException("Partition is not ordered.")
            if (v + 1 != previousValue) allSequential = false
          }
          previousValue = v
        }
        if (allSequential) throw new SparkException("Partition should not be globally ordered")
      }
    }

    // Distribute and order by with multiple order bys
    val df5 = data.repartition(2, $"a").sortWithinPartitions($"b".asc, $"a".asc)
    // Walk each partition and verify that it is sorted ascending
    df5.rdd.foreachPartition { p =>
      var previousValue: Int = -1
      var allSequential: Boolean = true
      p.foreach { r =>
        val v: Int = r.getInt(1)
        if (previousValue != -1) {
          if (previousValue > v) throw new SparkException("Partition is not ordered.")
          if (v - 1 != previousValue) allSequential = false
        }
        previousValue = v
      }
      if (allSequential) throw new SparkException("Partition should not be all sequential")
    }

    // Distribute into one partition and order by. This partition should contain all the values.
    val df6 = data.repartition(1, $"a").sortWithinPartitions("b")
    // Walk each partition and verify that it is sorted ascending and not globally sorted.
    df6.rdd.foreachPartition { p =>
      var previousValue: Int = -1
      var allSequential: Boolean = true
      p.foreach { r =>
        val v: Int = r.getInt(1)
        if (previousValue != -1) {
          if (previousValue > v) throw new SparkException("Partition is not ordered.")
          if (v - 1 != previousValue) allSequential = false
        }
        previousValue = v
      }
      if (!allSequential) throw new SparkException("Partition should contain all sequential values")
    }
  }

  test("SPARK-39834: build the constraints for LogicalRDD based on origin constraints") {
    def buildExpectedConstraints(attrs: Seq[Attribute]): ExpressionSet = {
      val exprs = attrs.flatMap { attr =>
        attr.dataType match {
          case BooleanType => Some(EqualTo(attr, Literal(true, BooleanType)))
          case IntegerType => Some(GreaterThan(attr, Literal(5, IntegerType)))
          case _ => None
        }
      }
      ExpressionSet(exprs)
    }

    val outputList = Seq(
      AttributeReference("cbool", BooleanType)(),
      AttributeReference("cbyte", ByteType)(),
      AttributeReference("cint", IntegerType)()
    )

    val statsPlan = OutputListAwareConstraintsTestPlan(outputList = outputList)

    val df = Dataset.ofRows(spark, statsPlan)
      // add some map-like operations which optimizer will optimize away, and make a divergence
      // for output between logical plan and optimized plan
      // logical plan
      // Project [cb#6 AS cbool#12, cby#7 AS cbyte#13, ci#8 AS cint#14]
      // +- Project [cbool#0 AS cb#6, cbyte#1 AS cby#7, cint#2 AS ci#8]
      //    +- OutputListAwareConstraintsTestPlan [cbool#0, cbyte#1, cint#2]
      // optimized plan
      // OutputListAwareConstraintsTestPlan [cbool#0, cbyte#1, cint#2]
      .selectExpr("cbool AS cb", "cbyte AS cby", "cint AS ci")
      .selectExpr("cb AS cbool", "cby AS cbyte", "ci AS cint")

    // We can't leverage LogicalRDD.fromDataset here, since it triggers physical planning and
    // there is no matching physical node for OutputListAwareConstraintsTestPlan.
    val optimizedPlan = df.queryExecution.optimizedPlan
    val rewrite = LogicalRDD.buildOutputAssocForRewrite(optimizedPlan.output, df.logicalPlan.output)
    val logicalRDD = LogicalRDD(
      df.logicalPlan.output, spark.sparkContext.emptyRDD[InternalRow], isStreaming = true)(
      spark, None, Some(LogicalRDD.rewriteConstraints(optimizedPlan.constraints, rewrite.get)))

    val constraints = logicalRDD.constraints
    val expectedConstraints = buildExpectedConstraints(logicalRDD.output)
    assert(constraints === expectedConstraints)

    // This method re-issues expression IDs for all outputs. We expect constraints to be
    // reflected as well.
    val newLogicalRDD = logicalRDD.newInstance()
    val newConstraints = newLogicalRDD.constraints
    val newExpectedConstraints = buildExpectedConstraints(newLogicalRDD.output)
    assert(newConstraints === newExpectedConstraints)
  }

  test("SPARK-46794: exclude subqueries from LogicalRDD constraints") {
    withTempDir { checkpointDir =>
      val subquery =
        new Column(ScalarSubquery(spark.range(10).selectExpr("max(id)").logicalPlan))
      val df = spark.range(1000).filter($"id" === subquery)
      assert(df.logicalPlan.constraints.exists(_.exists(_.isInstanceOf[ScalarSubquery])))

      spark.sparkContext.setCheckpointDir(checkpointDir.getAbsolutePath)
      val checkpointedDf = df.checkpoint()
      assert(!checkpointedDf.logicalPlan.constraints
        .exists(_.exists(_.isInstanceOf[ScalarSubquery])))
    }
  }

  test("reuse exchange") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2") {
      val df = spark.range(100).toDF()
      val join = df.join(df, "id")
      val plan = join.queryExecution.executedPlan
      checkAnswer(join, df)
      assert(
        collect(join.queryExecution.executedPlan) {
          case e: ShuffleExchangeExec => true }.size === 1)
      assert(
        collect(join.queryExecution.executedPlan) { case e: ReusedExchangeExec => true }.size === 1)
      val broadcasted = broadcast(join)
      val join2 = join.join(broadcasted, "id").join(broadcasted, "id")
      checkAnswer(join2, df)
      assert(
        collect(join2.queryExecution.executedPlan) {
          case e: ShuffleExchangeExec => true }.size == 1)
      assert(
        collect(join2.queryExecution.executedPlan) {
          case e: BroadcastExchangeExec => true }.size === 1)
      assert(
        collect(join2.queryExecution.executedPlan) { case e: ReusedExchangeExec => true }.size == 4)
    }
  }

  test("sameResult() on aggregate") {
    val df = spark.range(100)
    val agg1 = df.groupBy().count()
    val agg2 = df.groupBy().count()
    // two aggregates with different ExprId within them should have same result
    assert(agg1.queryExecution.executedPlan.sameResult(agg2.queryExecution.executedPlan))
    val agg3 = df.groupBy().sum()
    assert(!agg1.queryExecution.executedPlan.sameResult(agg3.queryExecution.executedPlan))
    val df2 = spark.range(101)
    val agg4 = df2.groupBy().count()
    assert(!agg1.queryExecution.executedPlan.sameResult(agg4.queryExecution.executedPlan))
  }

  test("SPARK-16664: persist with more than 200 columns") {
    val size = 201L
    val rdd = sparkContext.makeRDD(Seq(Row.fromSeq(Seq.range(0, size))))
    val schemas = List.range(0, size).map(a => StructField("name" + a, LongType, true))
    val df = spark.createDataFrame(rdd, StructType(schemas))
    assert(df.persist().take(1).apply(0).toSeq(100).asInstanceOf[Long] == 100)
  }

  test("SPARK-17409: Do Not Optimize Query in CTAS (Data source tables) More Than Once") {
    withTable("bar") {
      withTempView("foo") {
        withSQLConf(SQLConf.DEFAULT_DATA_SOURCE_NAME.key -> "json") {
          sql("select 0 as id").createOrReplaceTempView("foo")
          val df = sql("select * from foo group by id")
          // If we optimize the query in CTAS more than once, the following saveAsTable will fail
          // with the error: `GROUP BY position 0 is not in select list (valid range is [1, 1])`
          df.write.mode("overwrite").saveAsTable("bar")
          checkAnswer(spark.table("bar"), Row(0) :: Nil)
          val tableMetadata = spark.sessionState.catalog.getTableMetadata(TableIdentifier("bar"))
          assert(tableMetadata.provider == Some("json"),
            "the expected table is a data source table using json")
        }
      }
    }
  }

  private def verifyNullabilityInFilterExec(
      df: DataFrame,
      expr: String,
      expectedNonNullableColumns: Seq[String]): Unit = {
    val dfWithFilter = df.where(s"isnotnull($expr)").selectExpr(expr)
    dfWithFilter.queryExecution.executedPlan.collect {
      // When the child expression in isnotnull is null-intolerant (i.e. any null input will
      // result in null output), the involved columns are converted to not nullable;
      // otherwise, no change should be made.
      case e: FilterExec =>
        assert(e.output.forall { o =>
          if (expectedNonNullableColumns.contains(o.name)) !o.nullable else o.nullable
        })
    }
  }

  test("SPARK-17957: no change on nullability in FilterExec output") {
    val df = sparkContext.parallelize(Seq(
      null.asInstanceOf[java.lang.Integer] -> java.lang.Integer.valueOf(3),
      java.lang.Integer.valueOf(1) -> null.asInstanceOf[java.lang.Integer],
      java.lang.Integer.valueOf(2) -> java.lang.Integer.valueOf(4))).toDF()

    verifyNullabilityInFilterExec(df,
      expr = "Rand()", expectedNonNullableColumns = Seq.empty[String])
    verifyNullabilityInFilterExec(df,
      expr = "coalesce(_1, _2)", expectedNonNullableColumns = Seq.empty[String])
    verifyNullabilityInFilterExec(df,
      expr = "coalesce(_1, 0) + Rand()", expectedNonNullableColumns = Seq.empty[String])
    verifyNullabilityInFilterExec(df,
      expr = "cast(coalesce(cast(coalesce(_1, _2) as double), 0.0) as int)",
      expectedNonNullableColumns = Seq.empty[String])
  }

  test("SPARK-17957: set nullability to false in FilterExec output") {
    val df = sparkContext.parallelize(Seq(
      null.asInstanceOf[java.lang.Integer] -> java.lang.Integer.valueOf(3),
      java.lang.Integer.valueOf(1) -> null.asInstanceOf[java.lang.Integer],
      java.lang.Integer.valueOf(2) -> java.lang.Integer.valueOf(4))).toDF()

    verifyNullabilityInFilterExec(df,
      expr = "_1 + _2 * 3", expectedNonNullableColumns = Seq("_1", "_2"))
    verifyNullabilityInFilterExec(df,
      expr = "_1 + _2", expectedNonNullableColumns = Seq("_1", "_2"))
    verifyNullabilityInFilterExec(df,
      expr = "_1", expectedNonNullableColumns = Seq("_1"))
    // `constructIsNotNullConstraints` infers the IsNotNull(_2) from IsNotNull(_2 + Rand())
    // Thus, we are able to set nullability of _2 to false.
    // If IsNotNull(_2) is not given from `constructIsNotNullConstraints`, the impl of
    // isNullIntolerant in `FilterExec` needs an update for more advanced inference.
    verifyNullabilityInFilterExec(df,
      expr = "_2 + Rand()", expectedNonNullableColumns = Seq("_2"))
    verifyNullabilityInFilterExec(df,
      expr = "_2 * 3 + coalesce(_1, 0)", expectedNonNullableColumns = Seq("_2"))
    verifyNullabilityInFilterExec(df,
      expr = "cast((_1 + _2) as boolean)", expectedNonNullableColumns = Seq("_1", "_2"))
  }

  test("SPARK-18070 binary operator should not consider nullability when comparing input types") {
    val rows = Seq(Row(Seq(1), Seq(1)))
    val schema = new StructType()
      .add("array1", ArrayType(IntegerType))
      .add("array2", ArrayType(IntegerType, containsNull = false))
    val df = spark.createDataFrame(spark.sparkContext.makeRDD(rows), schema)
    assert(df.filter($"array1" === $"array2").count() == 1)
  }

  test("SPARK-22520: support code generation for large CaseWhen") {
    val N = 30
    var expr1 = when($"id" === lit(0), 0)
    var expr2 = when($"id" === lit(0), 10)
    (1 to N).foreach { i =>
      expr1 = expr1.when($"id" === lit(i), -i)
      expr2 = expr2.when($"id" === lit(i + 10), i)
    }
    val df = spark.range(1).select(expr1, expr2.otherwise(0))
    checkAnswer(df, Row(0, 10) :: Nil)
    assert(df.queryExecution.executedPlan.isInstanceOf[WholeStageCodegenExec])
  }

  test("Uuid expressions should produce same results at retries in the same DataFrame") {
    val df = spark.range(1).select($"id", new Column(Uuid()))
    checkAnswer(df, df.collect())
  }

  test("SPARK-25402 Null handling in BooleanSimplification") {
    val schema = StructType.fromDDL("a boolean, b int")
    val rows = Seq(Row(null, 1))

    val rdd = sparkContext.parallelize(rows)
    val df = spark.createDataFrame(rdd, schema)

    checkAnswer(df.where("(NOT a) OR a"), Seq.empty)
  }

  test("SPARK-25714 Null handling in BooleanSimplification") {
    withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ConvertToLocalRelation.ruleName) {
      val df = Seq(("abc", 1), (null, 3)).toDF("col1", "col2")
      checkAnswer(
        df.filter("col1 = 'abc' OR (col1 != 'abc' AND col2 == 3)"),
        Row ("abc", 1))
    }
  }

  test("groupBy.as: custom grouping expressions") {
    val df1 = Seq((1, 2, 3), (2, 3, 4)).toDF("a1", "b", "c")
      .repartition($"a1", $"b").sortWithinPartitions("a1", "b")
    val df2 = Seq((1, 2, 4), (2, 3, 5)).toDF("a1", "b", "c")
      .repartition($"a1", $"b").sortWithinPartitions("a1", "b")

    implicit val valueEncoder = ExpressionEncoder(df1.schema)

    val groupedDataset1 = df1.groupBy(($"a1" + 1).as("a"), $"b").as[GroupByKey, Row]
    val groupedDataset2 = df2.groupBy(($"a1" + 1).as("a"), $"b").as[GroupByKey, Row]

    val df3 = groupedDataset1
      .cogroup(groupedDataset2) { case (_, data1, data2) =>
        data1.zip(data2).map { p =>
          p._1.getInt(2) + p._2.getInt(2)
        }
      }.toDF()

    checkAnswer(df3.sort("value"), Row(7) :: Row(9) :: Nil)
  }

  test("groupBy.as: throw AnalysisException for unresolved grouping expr") {
    val df = Seq((1, 2, 3), (2, 3, 4)).toDF("a", "b", "c")

    implicit val valueEncoder = ExpressionEncoder(df.schema)

    checkError(
      exception = intercept[AnalysisException] {
        df.groupBy($"d", $"b").as[GroupByKey, Row]
      },
      errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
      parameters = Map("objectName" -> "`d`", "proposal" -> "`a`, `b`, `c`"),
      context = ExpectedContext(fragment = "$", callSitePattern = getCurrentClassCallSitePattern))
  }

  test("SPARK-42655 Fix ambiguous column reference error") {
    val df1 = sparkContext.parallelize(List((1, 2, 3, 4, 5))).toDF("id", "col2", "col3",
      "col4", "col5")
    val op_cols_mixed_case = List("id", "col2", "col3", "col4", "col5", "ID")
    val df2 = df1.select(op_cols_mixed_case.head, op_cols_mixed_case.tail: _*)
    // should not throw any error.
    checkAnswer(df2.select("id"), Row(1))
  }

  test("groupBy.as") {
    val df1 = Seq((1, 2, 3), (2, 3, 4)).toDF("a", "b", "c")
      .repartition($"a", $"b").sortWithinPartitions("a", "b")
    val df2 = Seq((1, 2, 4), (2, 3, 5)).toDF("a", "b", "c")
      .repartition($"a", $"b").sortWithinPartitions("a", "b")

    implicit val valueEncoder = ExpressionEncoder(df1.schema)

    val df3 = df1.groupBy("a", "b").as[GroupByKey, Row]
      .cogroup(df2.groupBy("a", "b").as[GroupByKey, Row]) { case (_, data1, data2) =>
        data1.zip(data2).map { p =>
          p._1.getInt(2) + p._2.getInt(2)
        }
      }.toDF()

    checkAnswer(df3.sort("value"), Row(7) :: Row(9) :: Nil)

    // Assert that no extra shuffle introduced by cogroup.
    val exchanges = collect(df3.queryExecution.executedPlan) {
      case h: ShuffleExchangeExec => h
    }
    assert(exchanges.size == 2)
  }

  test("emptyDataFrame should be foldable") {
    val emptyDf = spark.emptyDataFrame.withColumn("id", lit(1L))
    val joined = spark.range(10).join(emptyDf, "id")
    joined.queryExecution.optimizedPlan match {
      case LocalRelation(Seq(id), Nil, _) =>
        assert(id.name == "id")
      case _ =>
        fail("emptyDataFrame should be foldable")
    }
  }

  test("assertAnalyzed shouldn't replace original stack trace") {
    val e = intercept[AnalysisException] {
      spark.range(1).select($"id" as "a", $"id" as "b").groupBy("a").agg($"b")
    }

    assert(e.getStackTrace.head.getClassName != classOf[QueryExecution].getName)
  }

  test("CalendarInterval reflection support") {
    val df = Seq((1, new CalendarInterval(1, 2, 3))).toDF("a", "b")
    checkAnswer(df.selectExpr("b"), Row(new CalendarInterval(1, 2, 3)))
  }

  test("SPARK-32680: Don't analyze CTAS with unresolved query") {
    val v2Source = classOf[FakeV2Provider].getName
    val e = intercept[AnalysisException] {
      sql(s"CREATE TABLE t USING $v2Source AS SELECT * from nonexist")
    }
    checkErrorTableNotFound(e, "`nonexist`",
      ExpectedContext("nonexist", s"CREATE TABLE t USING $v2Source AS SELECT * from ".length,
        s"CREATE TABLE t USING $v2Source AS SELECT * from nonexist".length - 1))
  }

  test("SPARK-34882: Aggregate with multiple distinct null sensitive aggregators") {
    withUserDefinedFunction(("countNulls", true)) {
      spark.udf.register("countNulls", udaf(new Aggregator[JLong, JLong, JLong] {
        def zero: JLong = 0L
        def reduce(b: JLong, a: JLong): JLong = if (a == null) {
          b + 1
        } else {
          b
        }
        def merge(b1: JLong, b2: JLong): JLong = b1 + b2
        def finish(r: JLong): JLong = r
        def bufferEncoder: Encoder[JLong] = Encoders.LONG
        def outputEncoder: Encoder[JLong] = Encoders.LONG
      }))

      val result = testData.selectExpr(
        "countNulls(key)",
        "countNulls(DISTINCT key)",
        "countNulls(key) FILTER (WHERE key > 50)",
        "countNulls(DISTINCT key) FILTER (WHERE key > 50)",
        "count(DISTINCT key)")

      checkAnswer(result, Row(0, 0, 0, 0, 100))
    }
  }

  test("SPARK-35410: SubExpr elimination should not include redundant child exprs " +
    "for conditional expressions") {
    val accum = sparkContext.longAccumulator("call")
    val simpleUDF = udf((s: String) => {
      accum.add(1)
      s
    })
    val df1 = spark.range(5).select(when(functions.length(simpleUDF($"id")) > 0,
      functions.length(simpleUDF($"id"))).otherwise(
        functions.length(simpleUDF($"id")) + 1))
    df1.collect()
    assert(accum.value == 5)

    val nondeterministicUDF = simpleUDF.asNondeterministic()
    val df2 = spark.range(5).select(when(functions.length(nondeterministicUDF($"id")) > 0,
      functions.length(nondeterministicUDF($"id"))).otherwise(
        functions.length(nondeterministicUDF($"id")) + 1))
    df2.collect()
    assert(accum.value == 15)
  }

  test("SPARK-35560: Remove redundant subexpression evaluation in nested subexpressions") {
    Seq(1, Int.MaxValue).foreach { splitThreshold =>
      withSQLConf(SQLConf.CODEGEN_METHOD_SPLIT_THRESHOLD.key -> splitThreshold.toString) {
        val accum = sparkContext.longAccumulator("call")
        val simpleUDF = udf((s: String) => {
          accum.add(1)
          s
        })

        // Common exprs:
        //  1. simpleUDF($"id")
        //  2. functions.length(simpleUDF($"id"))
        // We should only evaluate `simpleUDF($"id")` once, i.e.
        // subExpr1 = simpleUDF($"id");
        // subExpr2 = functions.length(subExpr1);
        val df = spark.range(5).select(
          when(functions.length(simpleUDF($"id")) === 1, lower(simpleUDF($"id")))
            .when(functions.length(simpleUDF($"id")) === 0, upper(simpleUDF($"id")))
            .otherwise(simpleUDF($"id")).as("output"))
        df.collect()
        assert(accum.value == 5)
      }
    }
  }

  test("SPARK-39887: RemoveRedundantAliases should keep attributes of a Union's first child") {
    val df = sql(
      """
        |SELECT a, b AS a FROM (
        |  SELECT a, a AS b FROM (SELECT a FROM VALUES (1) AS t(a))
        |  UNION ALL
        |  SELECT a, b FROM (SELECT a, b FROM VALUES (1, 2) AS t(a, b))
        |)
        |""".stripMargin)
    val stringCols = df.logicalPlan.output.map(Column(_).cast(StringType))
    val castedDf = df.select(stringCols: _*)
    checkAnswer(castedDf, Row("1", "1") :: Row("1", "2") :: Nil)
  }

  test("SPARK-39915: Dataset.repartition(N) may not create N partitions") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val df = spark.sql("select * from values(1) where 1 < rand()").repartition(2)
      assert(df.queryExecution.executedPlan.execute().getNumPartitions == 2)
    }
  }

  test("SPARK-41048: Improve output partitioning and ordering with AQE cache") {
    withSQLConf(
        SQLConf.CAN_CHANGE_CACHED_PLAN_OUTPUT_PARTITIONING.key -> "true",
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      val df1 = spark.range(10).selectExpr("cast(id as string) c1")
      val df2 = spark.range(10).selectExpr("cast(id as string) c2")
      val cached = df1.join(df2, $"c1" === $"c2").cache()
      cached.count()
      val executedPlan = cached.groupBy("c1").agg(max($"c2")).queryExecution.executedPlan
      // before is 2 sort and 1 shuffle
      assert(collect(executedPlan) {
        case s: ShuffleExchangeLike => s
      }.isEmpty)
      assert(collect(executedPlan) {
        case s: SortExec => s
      }.isEmpty)
    }
  }

  test("SPARK-40601: flatMapCoGroupsInPandas should fail with different number of keys") {
    val df1 = Seq((1, 2, "A1"), (2, 1, "A2")).toDF("key1", "key2", "value")
    val df2 = df1.filter($"value" === "A2")

    val flatMapCoGroupsInPandasUDF = PythonUDF("flagMapCoGroupsInPandasUDF", null,
      StructType(Seq(StructField("x", LongType), StructField("y", LongType))),
      Seq.empty,
      PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF,
      true)

    // the number of keys must match
    val exception1 = intercept[IllegalArgumentException] {
      df1.groupBy($"key1", $"key2").flatMapCoGroupsInPandas(
        df2.groupBy($"key2"), flatMapCoGroupsInPandasUDF)
    }
    assert(exception1.getMessage.contains("Cogroup keys must have same size: 2 != 1"))
    val exception2 = intercept[IllegalArgumentException] {
      df1.groupBy($"key1").flatMapCoGroupsInPandas(
        df2.groupBy($"key1", $"key2"), flatMapCoGroupsInPandasUDF)
    }
    assert(exception2.getMessage.contains("Cogroup keys must have same size: 1 != 2"))

    // but different keys are allowed
    val actual = df1.groupBy($"key1").flatMapCoGroupsInPandas(
      df2.groupBy($"key2"), flatMapCoGroupsInPandasUDF)
    // can't evaluate the DataFrame as there is no PythonFunction given
    assert(actual != null)
  }

  test("SPARK-41049: stateful expression should be copied correctly") {
    val df = spark.sparkContext.parallelize(1 to 5).toDF("x")
    val v1 = (rand() * 10000).cast(IntegerType)
    val v2 = to_csv(struct(v1.as("a"))) // to_csv is CodegenFallback
    df.select(v1, v1, v2, v2).collect().foreach { row =>
      assert(row.getInt(0) == row.getInt(1))
      assert(row.getInt(0).toString == row.getString(2))
      assert(row.getInt(0).toString == row.getString(3))
    }

    val v3 = Column(CreateMap(Seq(Literal("key"), Literal("value"))))
    val v4 = to_csv(struct(v3.as("a"))) // to_csv is CodegenFallback
    df.select(v3, v3, v4, v4).collect().foreach { row =>
      assert(row.getMap(0).toString() == row.getMap(1).toString())
      assert(row.getString(2) == s"{key -> ${row.getMap(0).get("key").get}}")
      assert(row.getString(3) == s"{key -> ${row.getMap(0).get("key").get}}")
    }
  }

  test("SPARK-46502: Unwrap timestamp cast on timestamp_ntz column") {
    def getQueryResult(ruleEnabled: Boolean): Seq[Row] = {
      val ruleName = if (ruleEnabled) {
        ""
      } else {
        "org.apache.spark.sql.catalyst.optimizer.UnwrapCastInBinaryComparison"
      }

      var result: Seq[Row] = Seq.empty

      withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ruleName) {
        withTable("table_timestamp") {
          sql(
            """
              |CREATE TABLE table_timestamp (
              |  batch TIMESTAMP_NTZ)
              |USING parquet;
              |""".stripMargin)
          sql("INSERT INTO table_timestamp SELECT CAST('2023-12-21 09:00:00' AS TIMESTAMP)")
          sql("INSERT INTO table_timestamp SELECT CAST('2023-12-21 10:00:00' AS TIMESTAMP)")
          sql("INSERT INTO table_timestamp SELECT CAST('2023-12-21 12:00:00' AS TIMESTAMP)")

          sql("CREATE OR REPLACE VIEW timestamp_view AS " +
            "SELECT CAST(batch AS TIMESTAMP) FROM table_timestamp")
          val df = sql("SELECT * from timestamp_view where batch >= '2023-12-21 10:00:00'")

          val filter = df.queryExecution.optimizedPlan.collect {
            case f: Filter => f
          }
          assert(filter.size == 1)

          val filterCondition = filter.head.condition
          val castExpr = filterCondition.collect {
            case c: Cast => c
          }

          if (ruleEnabled) {
            assert(castExpr.isEmpty)
          } else {
            assert(castExpr.size == 1)
          }

          result = df.collect().toSeq

          sql("DROP VIEW timestamp_view")
        }
      }
      result
    }

    val actual = getQueryResult(true).map(_.getTimestamp(0).toString).sorted
    val expected = getQueryResult(false).map(_.getTimestamp(0).toString).sorted
    assert(actual == expected)
  }
}

case class GroupByKey(a: Int, b: Int)

case class Bar2(s: String)

/**
 * This class is used for unit-testing. It's a logical plan whose output is passed in.
 */
case class OutputListAwareConstraintsTestPlan(
    outputList: Seq[Attribute]) extends LeafNode with MultiInstanceRelation {
  override def output: Seq[Attribute] = outputList

  override lazy val constraints: ExpressionSet = {
    val exprs = outputList.flatMap { attr =>
      attr.dataType match {
        case BooleanType => Some(EqualTo(attr, Literal(true, BooleanType)))
        case IntegerType => Some(GreaterThan(attr, Literal(5, IntegerType)))
        case _ => None
      }
    }
    ExpressionSet(exprs)
  }

  override def newInstance(): LogicalPlan = copy(outputList = outputList.map(_.newInstance()))
}


