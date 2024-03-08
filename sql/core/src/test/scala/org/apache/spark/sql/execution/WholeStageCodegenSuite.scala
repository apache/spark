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

package org.apache.spark.sql.execution

import org.apache.spark.SparkException
import org.apache.spark.rdd.MapPartitionsWithEvaluatorRDD
import org.apache.spark.sql.{Dataset, QueryTest, Row, SaveMode}
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.catalyst.expressions.codegen.{ByteCodeStats, CodeAndComment, CodeGenerator}
import org.apache.spark.sql.execution.adaptive.DisableAdaptiveExecutionSuite
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BroadcastNestedLoopJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

// Disable AQE because the WholeStageCodegenExec is added when running QueryStageExec
class WholeStageCodegenSuite extends QueryTest with SharedSparkSession
  with DisableAdaptiveExecutionSuite {

  import testImplicits._

  test("range/filter should be combined") {
    val df = spark.range(10).filter("id = 1").selectExpr("id + 1")
    val plan = df.queryExecution.executedPlan
    assert(plan.exists(_.isInstanceOf[WholeStageCodegenExec]))
    assert(df.collect() === Array(Row(2)))
  }

  test("HashAggregate should be included in WholeStageCodegen") {
    val df = spark.range(10).agg(max(col("id")), avg(col("id")))
    val plan = df.queryExecution.executedPlan
    assert(plan.exists(p =>
      p.isInstanceOf[WholeStageCodegenExec] &&
        p.asInstanceOf[WholeStageCodegenExec].child.isInstanceOf[HashAggregateExec]))
    assert(df.collect() === Array(Row(9, 4.5)))
  }

  test("SortAggregate should be included in WholeStageCodegen") {
    val df = spark.range(10).agg(max(col("id")), avg(col("id")))
    withSQLConf("spark.sql.test.forceApplySortAggregate" -> "true") {
      val plan = df.queryExecution.executedPlan
      assert(plan.exists(p =>
        p.isInstanceOf[WholeStageCodegenExec] &&
          p.asInstanceOf[WholeStageCodegenExec].child.isInstanceOf[SortAggregateExec]))
      assert(df.collect() === Array(Row(9, 4.5)))
    }
  }

  testWithWholeStageCodegenOnAndOff("GenerateExec should be" +
    " included in WholeStageCodegen") { codegenEnabled =>
    import testImplicits._
    val arrayData = Seq(("James", Seq("Java", "Scala"), Map("hair" -> "black", "eye" -> "brown")))
    val df = arrayData.toDF("name", "knownLanguages", "properties")

    // Array - explode
    var expDF = df.select($"name", explode($"knownLanguages"), $"properties")
    var plan = expDF.queryExecution.executedPlan
    assert(plan.exists {
      case stage: WholeStageCodegenExec =>
        stage.exists(_.isInstanceOf[GenerateExec])
      case _ => !codegenEnabled.toBoolean
    })
    checkAnswer(expDF, Array(Row("James", "Java", Map("hair" -> "black", "eye" -> "brown")),
      Row("James", "Scala", Map("hair" -> "black", "eye" -> "brown"))))

    // Map - explode
    expDF = df.select($"name", $"knownLanguages", explode($"properties"))
    plan = expDF.queryExecution.executedPlan
    assert(plan.exists {
      case stage: WholeStageCodegenExec =>
        stage.exists(_.isInstanceOf[GenerateExec])
      case _ => !codegenEnabled.toBoolean
    })
    checkAnswer(expDF,
      Array(Row("James", List("Java", "Scala"), "hair", "black"),
        Row("James", List("Java", "Scala"), "eye", "brown")))

    // Array - posexplode
    expDF = df.select($"name", posexplode($"knownLanguages"))
    plan = expDF.queryExecution.executedPlan
    assert(plan.exists {
      case stage: WholeStageCodegenExec =>
        stage.exists(_.isInstanceOf[GenerateExec])
      case _ => !codegenEnabled.toBoolean
    })
    checkAnswer(expDF,
      Array(Row("James", 0, "Java"), Row("James", 1, "Scala")))

    // Map - posexplode
    expDF = df.select($"name", posexplode($"properties"))
    plan = expDF.queryExecution.executedPlan
    assert(plan.exists {
      case stage: WholeStageCodegenExec =>
        stage.exists(_.isInstanceOf[GenerateExec])
      case _ => !codegenEnabled.toBoolean
    })
    checkAnswer(expDF,
      Array(Row("James", 0, "hair", "black"), Row("James", 1, "eye", "brown")))

    // Array - explode , selecting all columns
    expDF = df.select($"*", explode($"knownLanguages"))
    plan = expDF.queryExecution.executedPlan
    assert(plan.exists {
      case stage: WholeStageCodegenExec =>
        stage.exists(_.isInstanceOf[GenerateExec])
      case _ => !codegenEnabled.toBoolean
    })
    checkAnswer(expDF,
      Array(Row("James", Seq("Java", "Scala"), Map("hair" -> "black", "eye" -> "brown"), "Java"),
        Row("James", Seq("Java", "Scala"), Map("hair" -> "black", "eye" -> "brown"), "Scala")))

    // Map - explode, selecting all columns
    expDF = df.select($"*", explode($"properties"))
    plan = expDF.queryExecution.executedPlan
    assert(plan.exists {
      case stage: WholeStageCodegenExec =>
        stage.exists(_.isInstanceOf[GenerateExec])
      case _ => !codegenEnabled.toBoolean
    })
    checkAnswer(expDF,
      Array(
        Row("James", List("Java", "Scala"),
          Map("hair" -> "black", "eye" -> "brown"), "hair", "black"),
        Row("James", List("Java", "Scala"),
          Map("hair" -> "black", "eye" -> "brown"), "eye", "brown")))
  }

  test("HashAggregate with grouping keys should be included in WholeStageCodegen") {
    val df = spark.range(3).groupBy(col("id") * 2).count().orderBy(col("id") * 2)
    val plan = df.queryExecution.executedPlan
    assert(plan.exists(p =>
      p.isInstanceOf[WholeStageCodegenExec] &&
        p.asInstanceOf[WholeStageCodegenExec].child.isInstanceOf[HashAggregateExec]))
    assert(df.collect() === Array(Row(0, 1), Row(2, 1), Row(4, 1)))
  }

  test("BroadcastHashJoin should be included in WholeStageCodegen") {
    val rdd = spark.sparkContext.makeRDD(Seq(Row(1, "1"), Row(1, "1"), Row(2, "2")))
    val schema = new StructType().add("k", IntegerType).add("v", StringType)
    val smallDF = spark.createDataFrame(rdd, schema)
    val df = spark.range(10).join(broadcast(smallDF), col("k") === col("id"))
    assert(df.queryExecution.executedPlan.exists(p =>
      p.isInstanceOf[WholeStageCodegenExec] &&
        p.asInstanceOf[WholeStageCodegenExec].child.isInstanceOf[BroadcastHashJoinExec]))
    assert(df.collect() === Array(Row(1, 1, "1"), Row(1, 1, "1"), Row(2, 2, "2")))
  }

  test("Inner ShuffledHashJoin should be included in WholeStageCodegen") {
    val df1 = spark.range(5).select($"id".as("k1"))
    val df2 = spark.range(15).select($"id".as("k2"))
    val df3 = spark.range(6).select($"id".as("k3"))

    // test one shuffled hash join
    val oneJoinDF = df1.join(df2.hint("SHUFFLE_HASH"), $"k1" === $"k2")
    assert(oneJoinDF.queryExecution.executedPlan.collect {
      case WholeStageCodegenExec(_ : ShuffledHashJoinExec) => true
    }.size === 1)
    checkAnswer(oneJoinDF, Seq(Row(0, 0), Row(1, 1), Row(2, 2), Row(3, 3), Row(4, 4)))

    // test two shuffled hash joins
    val twoJoinsDF = df1.join(df2.hint("SHUFFLE_HASH"), $"k1" === $"k2")
      .join(df3.hint("SHUFFLE_HASH"), $"k1" === $"k3")
    assert(twoJoinsDF.queryExecution.executedPlan.collect {
      case WholeStageCodegenExec(_ : ShuffledHashJoinExec) => true
    }.size === 2)
    checkAnswer(twoJoinsDF,
      Seq(Row(0, 0, 0), Row(1, 1, 1), Row(2, 2, 2), Row(3, 3, 3), Row(4, 4, 4)))
  }

  test("SPARK-44236: disable WholeStageCodegen when set spark.sql.codegen.factoryMode is " +
    "NO_CODEGEN") {
    withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString) {
      val df = spark.range(10).select($"id" + 1)
      val plan = df.queryExecution.executedPlan
      assert(!plan.exists(_.isInstanceOf[WholeStageCodegenExec]))
      checkAnswer(df, 1L to 10L map { i => Row(i) })
    }
  }

  test("Full Outer ShuffledHashJoin and SortMergeJoin should be included in WholeStageCodegen") {
    val df1 = spark.range(5).select($"id".as("k1"))
    val df2 = spark.range(10).select($"id".as("k2"))
    val df3 = spark.range(3).select($"id".as("k3"))

    Seq("SHUFFLE_HASH", "SHUFFLE_MERGE").foreach { hint =>
      // test one join with unique key from build side
      val joinUniqueDF = df1.join(df2.hint(hint), $"k1" === $"k2", "full_outer")
      assert(joinUniqueDF.queryExecution.executedPlan.collect {
        case WholeStageCodegenExec(_ : ShuffledHashJoinExec) if hint == "SHUFFLE_HASH" => true
        case WholeStageCodegenExec(_ : SortMergeJoinExec) if hint == "SHUFFLE_MERGE" => true
      }.size === 1)
      checkAnswer(joinUniqueDF, Seq(Row(0, 0), Row(1, 1), Row(2, 2), Row(3, 3), Row(4, 4),
        Row(null, 5), Row(null, 6), Row(null, 7), Row(null, 8), Row(null, 9)))
      assert(joinUniqueDF.count() === 10)

      // test one join with non-unique key from build side
      val joinNonUniqueDF = df1.join(df2.hint(hint), $"k1" === $"k2" % 3, "full_outer")
      assert(joinNonUniqueDF.queryExecution.executedPlan.collect {
        case WholeStageCodegenExec(_ : ShuffledHashJoinExec) if hint == "SHUFFLE_HASH" => true
        case WholeStageCodegenExec(_ : SortMergeJoinExec) if hint == "SHUFFLE_MERGE" => true
      }.size === 1)
      checkAnswer(joinNonUniqueDF, Seq(Row(0, 0), Row(0, 3), Row(0, 6), Row(0, 9), Row(1, 1),
        Row(1, 4), Row(1, 7), Row(2, 2), Row(2, 5), Row(2, 8), Row(3, null), Row(4, null)))

      // test one join with non-equi condition
      val joinWithNonEquiDF = df1.join(df2.hint(hint),
        $"k1" === $"k2" % 3 && $"k1" + 3 =!= $"k2", "full_outer")
      assert(joinWithNonEquiDF.queryExecution.executedPlan.collect {
        case WholeStageCodegenExec(_ : ShuffledHashJoinExec) if hint == "SHUFFLE_HASH" => true
        case WholeStageCodegenExec(_ : SortMergeJoinExec) if hint == "SHUFFLE_MERGE" => true
      }.size === 1)
      checkAnswer(joinWithNonEquiDF, Seq(Row(0, 0), Row(0, 6), Row(0, 9), Row(1, 1),
        Row(1, 7), Row(2, 2), Row(2, 8), Row(3, null), Row(4, null), Row(null, 3), Row(null, 4),
        Row(null, 5)))

      // test two joins
      val twoJoinsDF = df1.join(df2.hint(hint), $"k1" === $"k2", "full_outer")
        .join(df3.hint(hint), $"k1" === $"k3" && $"k1" + $"k3" =!= 2, "full_outer")
      assert(twoJoinsDF.queryExecution.executedPlan.collect {
        case WholeStageCodegenExec(_ : ShuffledHashJoinExec) if hint == "SHUFFLE_HASH" => true
        case WholeStageCodegenExec(_ : SortMergeJoinExec) if hint == "SHUFFLE_MERGE" => true
      }.size === 2)
      checkAnswer(twoJoinsDF,
        Seq(Row(0, 0, 0), Row(1, 1, null), Row(2, 2, 2), Row(3, 3, null), Row(4, 4, null),
          Row(null, 5, null), Row(null, 6, null), Row(null, 7, null), Row(null, 8, null),
          Row(null, 9, null), Row(null, null, 1)))
    }
  }


  test("SPARK-44060 Code-gen for build side outer shuffled hash join") {
    val df1 = spark.range(0, 5).select($"id".as("k1"))
    val df2 = spark.range(1, 11).select($"id".as("k2"))
    val df3 = spark.range(2, 5).select($"id".as("k3"))

    withSQLConf(SQLConf.ENABLE_BUILD_SIDE_OUTER_SHUFFLED_HASH_JOIN_CODEGEN.key -> "true") {
      Seq("SHUFFLE_HASH", "SHUFFLE_MERGE").foreach { hint =>
        // test right join with unique key from build side
        val rightJoinUniqueDf = df1.join(df2.hint(hint), $"k1" === $"k2", "right_outer")
        assert(rightJoinUniqueDf.queryExecution.executedPlan.collect {
          case WholeStageCodegenExec(_: ShuffledHashJoinExec) if hint == "SHUFFLE_HASH" => true
          case WholeStageCodegenExec(_: SortMergeJoinExec) if hint == "SHUFFLE_MERGE" => true
        }.size === 1)
        checkAnswer(rightJoinUniqueDf, Seq(Row(1, 1), Row(2, 2), Row(3, 3), Row(4, 4),
          Row(null, 5), Row(null, 6), Row(null, 7), Row(null, 8), Row(null, 9),
          Row(null, 10)))
        assert(rightJoinUniqueDf.count() === 10)

        // test left join with unique key from build side
        val leftJoinUniqueDf = df1.hint(hint).join(df2, $"k1" === $"k2", "left_outer")
        assert(leftJoinUniqueDf.queryExecution.executedPlan.collect {
          case WholeStageCodegenExec(_: ShuffledHashJoinExec) if hint == "SHUFFLE_HASH" => true
          case WholeStageCodegenExec(_: SortMergeJoinExec) if hint == "SHUFFLE_MERGE" => true
        }.size === 1)
        checkAnswer(leftJoinUniqueDf, Seq(Row(0, null), Row(1, 1), Row(2, 2), Row(3, 3), Row(4, 4)))
        assert(leftJoinUniqueDf.count() === 5)

        // test right join with non-unique key from build side
        val rightJoinNonUniqueDf = df1.join(df2.hint(hint), $"k1" === $"k2" % 3, "right_outer")
        assert(rightJoinNonUniqueDf.queryExecution.executedPlan.collect {
          case WholeStageCodegenExec(_: ShuffledHashJoinExec) if hint == "SHUFFLE_HASH" => true
          case WholeStageCodegenExec(_: SortMergeJoinExec) if hint == "SHUFFLE_MERGE" => true
        }.size === 1)
        checkAnswer(rightJoinNonUniqueDf, Seq(Row(0, 3), Row(0, 6), Row(0, 9), Row(1, 1),
          Row(1, 4), Row(1, 7), Row(1, 10), Row(2, 2), Row(2, 5), Row(2, 8)))

        // test left join with non-unique key from build side
        val leftJoinNonUniqueDf = df1.hint(hint).join(df2, $"k1" === $"k2" % 3, "left_outer")
        assert(leftJoinNonUniqueDf.queryExecution.executedPlan.collect {
          case WholeStageCodegenExec(_: ShuffledHashJoinExec) if hint == "SHUFFLE_HASH" => true
          case WholeStageCodegenExec(_: SortMergeJoinExec) if hint == "SHUFFLE_MERGE" => true
        }.size === 1)
        checkAnswer(leftJoinNonUniqueDf, Seq(Row(0, 3), Row(0, 6), Row(0, 9), Row(1, 1),
          Row(1, 4), Row(1, 7), Row(1, 10), Row(2, 2), Row(2, 5), Row(2, 8), Row(3, null),
          Row(4, null)))

        // test right join with non-equi condition
        val rightJoinWithNonEquiDf = df1.join(df2.hint(hint),
          $"k1" === $"k2" % 3 && $"k1" + 3 =!= $"k2", "right_outer")
        assert(rightJoinWithNonEquiDf.queryExecution.executedPlan.collect {
          case WholeStageCodegenExec(_: ShuffledHashJoinExec) if hint == "SHUFFLE_HASH" => true
          case WholeStageCodegenExec(_: SortMergeJoinExec) if hint == "SHUFFLE_MERGE" => true
        }.size === 1)
        checkAnswer(rightJoinWithNonEquiDf, Seq(Row(0, 6), Row(0, 9), Row(1, 1), Row(1, 7),
          Row(1, 10), Row(2, 2), Row(2, 8), Row(null, 3), Row(null, 4), Row(null, 5)))

        // test left join with non-equi condition
        val leftJoinWithNonEquiDf = df1.hint(hint).join(df2,
          $"k1" === $"k2" % 3 && $"k1" + 3 =!= $"k2", "left_outer")
        assert(leftJoinWithNonEquiDf.queryExecution.executedPlan.collect {
          case WholeStageCodegenExec(_: ShuffledHashJoinExec) if hint == "SHUFFLE_HASH" => true
          case WholeStageCodegenExec(_: SortMergeJoinExec) if hint == "SHUFFLE_MERGE" => true
        }.size === 1)
        checkAnswer(leftJoinWithNonEquiDf, Seq(Row(0, 6), Row(0, 9), Row(1, 1), Row(1, 7),
          Row(1, 10), Row(2, 2), Row(2, 8), Row(3, null), Row(4, null)))

        // test two right joins
        val twoRightJoinsDf = df1.join(df2.hint(hint), $"k1" === $"k2", "right_outer")
          .join(df3.hint(hint), $"k1" === $"k3" && $"k1" + $"k3" =!= 2, "right_outer")
        assert(twoRightJoinsDf.queryExecution.executedPlan.collect {
          case WholeStageCodegenExec(_: ShuffledHashJoinExec) if hint == "SHUFFLE_HASH" => true
          case WholeStageCodegenExec(_: SortMergeJoinExec) if hint == "SHUFFLE_MERGE" => true
        }.size === 2)
        checkAnswer(twoRightJoinsDf, Seq(Row(2, 2, 2), Row(3, 3, 3), Row(4, 4, 4)))

        // test two left joins
        val twoLeftJoinsDf = df1.hint(hint).join(df2, $"k1" === $"k2", "left_outer").hint(hint)
          .join(df3, $"k1" === $"k3" && $"k1" + $"k3" =!= 2, "left_outer")
        assert(twoLeftJoinsDf.queryExecution.executedPlan.collect {
          case WholeStageCodegenExec(_: ShuffledHashJoinExec) if hint == "SHUFFLE_HASH" => true
          case WholeStageCodegenExec(_: SortMergeJoinExec) if hint == "SHUFFLE_MERGE" => true
        }.size === 2)
        checkAnswer(twoLeftJoinsDf,
          Seq(Row(0, null, null), Row(1, 1, null), Row(2, 2, 2), Row(3, 3, 3), Row(4, 4, 4)))
      }
    }
  }

  test("Left/Right Outer SortMergeJoin should be included in WholeStageCodegen") {
    val df1 = spark.range(10).select($"id".as("k1"))
    val df2 = spark.range(4).select($"id".as("k2"))
    val df3 = spark.range(6).select($"id".as("k3"))

    // test one left outer sort merge join
    val oneLeftOuterJoinDF = df1.join(df2.hint("SHUFFLE_MERGE"), $"k1" === $"k2", "left_outer")
    assert(oneLeftOuterJoinDF.queryExecution.executedPlan.collect {
      case WholeStageCodegenExec(_ : SortMergeJoinExec) => true
    }.size === 1)
    checkAnswer(oneLeftOuterJoinDF, Seq(Row(0, 0), Row(1, 1), Row(2, 2), Row(3, 3), Row(4, null),
      Row(5, null), Row(6, null), Row(7, null), Row(8, null), Row(9, null)))

    // test one right outer sort merge join
    val oneRightOuterJoinDF = df2.join(df3.hint("SHUFFLE_MERGE"), $"k2" === $"k3", "right_outer")
    assert(oneRightOuterJoinDF.queryExecution.executedPlan.collect {
      case WholeStageCodegenExec(_ : SortMergeJoinExec) => true
    }.size === 1)
    checkAnswer(oneRightOuterJoinDF, Seq(Row(0, 0), Row(1, 1), Row(2, 2), Row(3, 3), Row(null, 4),
      Row(null, 5)))

    // test two sort merge joins
    val twoJoinsDF = df3.join(df2.hint("SHUFFLE_MERGE"), $"k3" === $"k2", "left_outer")
      .join(df1.hint("SHUFFLE_MERGE"), $"k3" === $"k1", "right_outer")
    assert(twoJoinsDF.queryExecution.executedPlan.collect {
      case WholeStageCodegenExec(_ : SortMergeJoinExec) => true
    }.size === 2)
    checkAnswer(twoJoinsDF,
      Seq(Row(0, 0, 0), Row(1, 1, 1), Row(2, 2, 2), Row(3, 3, 3), Row(4, null, 4), Row(5, null, 5),
        Row(null, null, 6), Row(null, null, 7), Row(null, null, 8), Row(null, null, 9)))
  }

  test("Left Semi SortMergeJoin should be included in WholeStageCodegen") {
    val df1 = spark.range(10).select($"id".as("k1"))
    val df2 = spark.range(4).select($"id".as("k2"))
    val df3 = spark.range(6).select($"id".as("k3"))

    // test one left semi sort merge join
    val oneJoinDF = df1.join(df2.hint("SHUFFLE_MERGE"), $"k1" === $"k2", "left_semi")
    assert(oneJoinDF.queryExecution.executedPlan.collect {
      case WholeStageCodegenExec(ProjectExec(_, _ : SortMergeJoinExec)) => true
    }.size === 1)
    checkAnswer(oneJoinDF, Seq(Row(0), Row(1), Row(2), Row(3)))

    // test two left semi sort merge joins
    val twoJoinsDF = df3.join(df2.hint("SHUFFLE_MERGE"), $"k3" === $"k2", "left_semi")
      .join(df1.hint("SHUFFLE_MERGE"), $"k3" === $"k1", "left_semi")
    assert(twoJoinsDF.queryExecution.executedPlan.collect {
      case WholeStageCodegenExec(ProjectExec(_, _ : SortMergeJoinExec)) |
           WholeStageCodegenExec(_ : SortMergeJoinExec) => true
    }.size === 2)
    checkAnswer(twoJoinsDF, Seq(Row(0), Row(1), Row(2), Row(3)))
  }

  test("Left Anti SortMergeJoin should be included in WholeStageCodegen") {
    val df1 = spark.range(10).select($"id".as("k1"))
    val df2 = spark.range(4).select($"id".as("k2"))
    val df3 = spark.range(6).select($"id".as("k3"))

    // test one left anti sort merge join
    val oneJoinDF = df1.join(df2.hint("SHUFFLE_MERGE"), $"k1" === $"k2", "left_anti")
    assert(oneJoinDF.queryExecution.executedPlan.collect {
      case WholeStageCodegenExec(ProjectExec(_, _ : SortMergeJoinExec)) => true
    }.size === 1)
    checkAnswer(oneJoinDF, Seq(Row(4), Row(5), Row(6), Row(7), Row(8), Row(9)))

    // test two left anti sort merge joins
    val twoJoinsDF = df1.join(df2.hint("SHUFFLE_MERGE"), $"k1" === $"k2", "left_anti")
      .join(df3.hint("SHUFFLE_MERGE"), $"k1" === $"k3", "left_anti")
    assert(twoJoinsDF.queryExecution.executedPlan.collect {
      case WholeStageCodegenExec(ProjectExec(_, _ : SortMergeJoinExec)) |
           WholeStageCodegenExec(_ : SortMergeJoinExec) => true
    }.size === 2)
    checkAnswer(twoJoinsDF, Seq(Row(6), Row(7), Row(8), Row(9)))
  }

  test("Inner/Cross BroadcastNestedLoopJoinExec should be included in WholeStageCodegen") {
    val df1 = spark.range(4).select($"id".as("k1"))
    val df2 = spark.range(3).select($"id".as("k2"))
    val df3 = spark.range(2).select($"id".as("k3"))

    Seq(true, false).foreach { codegenEnabled =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegenEnabled.toString) {
        // test broadcast nested loop join without condition
        val oneJoinDF = df1.join(df2)
        var hasJoinInCodegen = oneJoinDF.queryExecution.executedPlan.collect {
          case WholeStageCodegenExec(_ : BroadcastNestedLoopJoinExec) => true
        }.size === 1
        assert(hasJoinInCodegen == codegenEnabled)
        checkAnswer(oneJoinDF,
          Seq(Row(0, 0), Row(0, 1), Row(0, 2), Row(1, 0), Row(1, 1), Row(1, 2),
            Row(2, 0), Row(2, 1), Row(2, 2), Row(3, 0), Row(3, 1), Row(3, 2)))

        // test broadcast nested loop join with condition
        val oneJoinDFWithCondition = df1.join(df2, $"k1" + 1 =!= $"k2")
        hasJoinInCodegen = oneJoinDFWithCondition.queryExecution.executedPlan.collect {
          case WholeStageCodegenExec(_ : BroadcastNestedLoopJoinExec) => true
        }.size === 1
        assert(hasJoinInCodegen == codegenEnabled)
        checkAnswer(oneJoinDFWithCondition,
          Seq(Row(0, 0), Row(0, 2), Row(1, 0), Row(1, 1), Row(2, 0), Row(2, 1),
            Row(2, 2), Row(3, 0), Row(3, 1), Row(3, 2)))

        // test two broadcast nested loop joins
        val twoJoinsDF = df1.join(df2, $"k1" < $"k2").crossJoin(df3)
        hasJoinInCodegen = twoJoinsDF.queryExecution.executedPlan.collect {
          case WholeStageCodegenExec(BroadcastNestedLoopJoinExec(
            _: BroadcastNestedLoopJoinExec, _, _, _, _)) => true
        }.size === 1
        assert(hasJoinInCodegen == codegenEnabled)
        checkAnswer(twoJoinsDF,
          Seq(Row(0, 1, 0), Row(0, 2, 0), Row(1, 2, 0), Row(0, 1, 1), Row(0, 2, 1), Row(1, 2, 1)))
      }
    }
  }

  test("Left/Right outer BroadcastNestedLoopJoinExec should be included in WholeStageCodegen") {
    val df1 = spark.range(4).select($"id".as("k1"))
    val df2 = spark.range(3).select($"id".as("k2"))
    val df3 = spark.range(2).select($"id".as("k3"))
    val df4 = spark.range(0).select($"id".as("k4"))

    Seq(true, false).foreach { codegenEnabled =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegenEnabled.toString) {
        // test left outer join
        val leftOuterJoinDF = df1.join(df2, $"k1" > $"k2", "left_outer")
        var hasJoinInCodegen = leftOuterJoinDF.queryExecution.executedPlan.collect {
          case WholeStageCodegenExec(_: BroadcastNestedLoopJoinExec) => true
        }.size === 1
        assert(hasJoinInCodegen == codegenEnabled)
        checkAnswer(leftOuterJoinDF,
          Seq(Row(0, null), Row(1, 0), Row(2, 0), Row(2, 1), Row(3, 0), Row(3, 1), Row(3, 2)))

        // test right outer join
        val rightOuterJoinDF = df1.join(df2, $"k1" < $"k2", "right_outer")
        hasJoinInCodegen = rightOuterJoinDF.queryExecution.executedPlan.collect {
          case WholeStageCodegenExec(_: BroadcastNestedLoopJoinExec) => true
        }.size === 1
        assert(hasJoinInCodegen == codegenEnabled)
        checkAnswer(rightOuterJoinDF, Seq(Row(null, 0), Row(0, 1), Row(0, 2), Row(1, 2)))

        // test a combination of left outer and right outer joins
        val twoJoinsDF = df1.join(df2, $"k1" > $"k2" + 1, "right_outer")
          .join(df3, $"k1" <= $"k3", "left_outer")
        hasJoinInCodegen = twoJoinsDF.queryExecution.executedPlan.collect {
          case WholeStageCodegenExec(BroadcastNestedLoopJoinExec(
            _: BroadcastNestedLoopJoinExec, _, _, _, _)) => true
        }.size === 1
        assert(hasJoinInCodegen == codegenEnabled)
        checkAnswer(twoJoinsDF,
          Seq(Row(2, 0, null), Row(3, 0, null), Row(3, 1, null), Row(null, 2, null)))

        // test build side is empty
        val buildSideIsEmptyDF = df3.join(df4, $"k3" > $"k4", "left_outer")
        hasJoinInCodegen = buildSideIsEmptyDF.queryExecution.executedPlan.collect {
          case WholeStageCodegenExec(_: BroadcastNestedLoopJoinExec) => true
        }.size === 1
        assert(hasJoinInCodegen == codegenEnabled)
        checkAnswer(buildSideIsEmptyDF, Seq(Row(0, null), Row(1, null)))
      }
    }
  }

  test("Left semi/anti BroadcastNestedLoopJoinExec should be included in WholeStageCodegen") {
    val df1 = spark.range(4).select($"id".as("k1"))
    val df2 = spark.range(3).select($"id".as("k2"))
    val df3 = spark.range(2).select($"id".as("k3"))

    Seq(true, false).foreach { codegenEnabled =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegenEnabled.toString) {
        // test left semi join
        val semiJoinDF = df1.join(df2, $"k1" + 1 <= $"k2", "left_semi")
        var hasJoinInCodegen = semiJoinDF.queryExecution.executedPlan.collect {
          case WholeStageCodegenExec(ProjectExec(_, _ : BroadcastNestedLoopJoinExec)) => true
        }.size === 1
        assert(hasJoinInCodegen == codegenEnabled)
        checkAnswer(semiJoinDF, Seq(Row(0), Row(1)))

        // test left anti join
        val antiJoinDF = df1.join(df2, $"k1" + 1 <= $"k2", "left_anti")
        hasJoinInCodegen = antiJoinDF.queryExecution.executedPlan.collect {
          case WholeStageCodegenExec(ProjectExec(_, _ : BroadcastNestedLoopJoinExec)) => true
        }.size === 1
        assert(hasJoinInCodegen == codegenEnabled)
        checkAnswer(antiJoinDF, Seq(Row(2), Row(3)))

        // test a combination of left semi and left anti joins
        val twoJoinsDF = df1.join(df2, $"k1" < $"k2", "left_semi")
          .join(df3, $"k1" > $"k3", "left_anti")
        hasJoinInCodegen = twoJoinsDF.queryExecution.executedPlan.collect {
          case WholeStageCodegenExec(ProjectExec(_, BroadcastNestedLoopJoinExec(
          _: BroadcastNestedLoopJoinExec, _, _, _, _))) => true
        }.size === 1
        assert(hasJoinInCodegen == codegenEnabled)
        checkAnswer(twoJoinsDF, Seq(Row(0)))
      }
    }
  }

  test("Sort should be included in WholeStageCodegen") {
    val df = spark.range(3, 0, -1).toDF().sort(col("id"))
    val plan = df.queryExecution.executedPlan
    assert(plan.exists(p =>
      p.isInstanceOf[WholeStageCodegenExec] &&
        p.asInstanceOf[WholeStageCodegenExec].child.isInstanceOf[SortExec]))
    assert(df.collect() === Array(Row(1), Row(2), Row(3)))
  }

  test("MapElements should be included in WholeStageCodegen") {
    import testImplicits._

    val ds = spark.range(10).map(_.toString)
    val plan = ds.queryExecution.executedPlan
    assert(plan.exists(p =>
      p.isInstanceOf[WholeStageCodegenExec] &&
      p.asInstanceOf[WholeStageCodegenExec].child.isInstanceOf[SerializeFromObjectExec]))
    assert(ds.collect() === 0.until(10).map(_.toString).toArray)
  }

  test("typed filter should be included in WholeStageCodegen") {
    val ds = spark.range(10).filter(_ % 2 == 0)
    val plan = ds.queryExecution.executedPlan
    assert(plan.exists(p =>
      p.isInstanceOf[WholeStageCodegenExec] &&
        p.asInstanceOf[WholeStageCodegenExec].child.isInstanceOf[FilterExec]))
    assert(ds.collect() === Array(0, 2, 4, 6, 8))
  }

  test("back-to-back typed filter should be included in WholeStageCodegen") {
    val ds = spark.range(10).filter(_ % 2 == 0).filter(_ % 3 == 0)
    val plan = ds.queryExecution.executedPlan
    assert(plan.exists(p =>
      p.isInstanceOf[WholeStageCodegenExec] &&
      p.asInstanceOf[WholeStageCodegenExec].child.isInstanceOf[FilterExec]))
    assert(ds.collect() === Array(0, 6))
  }

  test("cache for primitive type should be in WholeStageCodegen with InMemoryTableScanExec") {
    import testImplicits._

    val dsInt = spark.range(3).cache()
    dsInt.count()
    val dsIntFilter = dsInt.filter(_ > 0)
    val planInt = dsIntFilter.queryExecution.executedPlan
    assert(planInt.collect {
      case WholeStageCodegenExec(FilterExec(_,
          InputAdapter(_: InMemoryTableScanExec))) => ()
    }.length == 1)
    assert(dsIntFilter.collect() === Array(1, 2))

    // cache for string type is not supported for InMemoryTableScanExec
    val dsString = spark.range(3).map(_.toString).cache()
    dsString.count()
    val dsStringFilter = dsString.filter(_ == "1")
    val planString = dsStringFilter.queryExecution.executedPlan
    assert(planString.collect {
      case _: ColumnarToRowExec => ()
    }.isEmpty)
    assert(dsStringFilter.collect() === Array("1"))
  }

  test("SPARK-19512 codegen for comparing structs is incorrect") {
    // this would raise CompileException before the fix
    spark.range(10)
      .selectExpr("named_struct('a', id) as col1", "named_struct('a', id+2) as col2")
      .filter("col1 = col2").count()
    // this would raise java.lang.IndexOutOfBoundsException before the fix
    spark.range(10)
      .selectExpr("named_struct('a', id, 'b', id) as col1",
        "named_struct('a',id+2, 'b',id+2) as col2")
      .filter("col1 = col2").count()
  }

  test("SPARK-21441 SortMergeJoin codegen with CodegenFallback expressions should be disabled") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "1") {
      import testImplicits._

      val df1 = Seq((1, 1), (2, 2), (3, 3)).toDF("key", "int")
      val df2 = Seq((1, "1"), (2, "2"), (3, "3")).toDF("key", "str")

      val df = df1.join(df2, df1("key") === df2("key"))
        .filter("int = 2 or reflect('java.lang.Integer', 'valueOf', str) = 1")
        .select("int")

      val plan = df.queryExecution.executedPlan
      assert(!plan.exists(p =>
        p.isInstanceOf[WholeStageCodegenExec] &&
          p.asInstanceOf[WholeStageCodegenExec].child.children(0)
            .isInstanceOf[SortMergeJoinExec]))
      assert(df.collect() === Array(Row(1), Row(2)))
    }
  }

  def genGroupByCode(caseNum: Int): CodeAndComment = {
    val caseExp = (1 to caseNum).map { i =>
      s"case when id > $i and id <= ${i + 1} then 1 else 0 end as v$i"
    }.toList
    val keyExp = List(
      "id",
      "(id & 1023) as k1",
      "cast(id & 1023 as double) as k2",
      "cast(id & 1023 as int) as k3")

    val ds = spark.range(10)
      .selectExpr(keyExp:::caseExp: _*)
      .groupBy("k1", "k2", "k3")
      .sum()
    val plan = ds.queryExecution.executedPlan

    val wholeStageCodeGenExec = plan.find(p => p match {
      case wp: WholeStageCodegenExec => wp.child match {
        case hp: HashAggregateExec if (hp.child.isInstanceOf[ProjectExec]) => true
        case _ => false
      }
      case _ => false
    })

    assert(wholeStageCodeGenExec.isDefined)
    wholeStageCodeGenExec.get.asInstanceOf[WholeStageCodegenExec].doCodeGen()._2
  }

  def genCode(ds: Dataset[_]): Seq[CodeAndComment] = {
    val plan = ds.queryExecution.executedPlan
    val wholeStageCodeGenExecs = plan.collect { case p: WholeStageCodegenExec => p }
    assert(wholeStageCodeGenExecs.nonEmpty, "WholeStageCodegenExec is expected")
    wholeStageCodeGenExecs.map(_.doCodeGen()._2)
  }

  ignore("SPARK-21871 check if we can get large code size when compiling too long functions") {
    val codeWithShortFunctions = genGroupByCode(3)
    val (_, ByteCodeStats(maxCodeSize1, _, _)) = CodeGenerator.compile(codeWithShortFunctions)
    assert(maxCodeSize1 < SQLConf.WHOLESTAGE_HUGE_METHOD_LIMIT.defaultValue.get)
    val codeWithLongFunctions = genGroupByCode(50)
    val (_, ByteCodeStats(maxCodeSize2, _, _)) = CodeGenerator.compile(codeWithLongFunctions)
    assert(maxCodeSize2 > SQLConf.WHOLESTAGE_HUGE_METHOD_LIMIT.defaultValue.get)
  }

  ignore("bytecode of batch file scan exceeds the limit of WHOLESTAGE_HUGE_METHOD_LIMIT") {
    import testImplicits._
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val df = spark.range(10).select(Seq.tabulate(201) {i => ($"id" + i).as(s"c$i")} : _*)
      df.write.mode(SaveMode.Overwrite).parquet(path)

      withSQLConf(SQLConf.WHOLESTAGE_MAX_NUM_FIELDS.key -> "202",
        SQLConf.WHOLESTAGE_HUGE_METHOD_LIMIT.key -> "2000") {
        // wide table batch scan causes the byte code of codegen exceeds the limit of
        // WHOLESTAGE_HUGE_METHOD_LIMIT
        val df2 = spark.read.parquet(path)
        val fileScan2 = df2.queryExecution.sparkPlan.find(_.isInstanceOf[FileSourceScanExec]).get
        assert(fileScan2.asInstanceOf[FileSourceScanExec].supportsColumnar)
        checkAnswer(df2, df)
      }
    }
  }

  test("Control splitting consume function by operators with config") {
    import testImplicits._
    val df = spark.range(10).select(Seq.tabulate(2) {i => ($"id" + i).as(s"c$i")} : _*)

    Seq(true, false).foreach { config =>
      withSQLConf(SQLConf.WHOLESTAGE_SPLIT_CONSUME_FUNC_BY_OPERATOR.key -> s"$config") {
        val plan = df.queryExecution.executedPlan
        val wholeStageCodeGenExec = plan.find(p => p match {
          case wp: WholeStageCodegenExec => true
          case _ => false
        })
        assert(wholeStageCodeGenExec.isDefined)
        val code = wholeStageCodeGenExec.get.asInstanceOf[WholeStageCodegenExec].doCodeGen()._2
        assert(code.body.contains("project_doConsume") == config)
      }
    }
  }

  test("Skip splitting consume function when parameter number exceeds JVM limit") {
    // since every field is nullable we have 2 params for each input column (one for the value
    // and one for the isNull variable)
    Seq((128, false), (127, true)).foreach { case (columnNum, hasSplit) =>
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        spark.range(10).select(Seq.tabulate(columnNum) {i => lit(i).as(s"c$i")} : _*)
          .write.mode(SaveMode.Overwrite).parquet(path)

        withSQLConf(SQLConf.WHOLESTAGE_MAX_NUM_FIELDS.key -> "255",
            SQLConf.WHOLESTAGE_SPLIT_CONSUME_FUNC_BY_OPERATOR.key -> "true") {
          val projection = Seq.tabulate(columnNum)(i => s"c$i + c$i as newC$i")
          val df = spark.read.parquet(path).selectExpr(projection: _*)

          val plan = df.queryExecution.executedPlan
          val wholeStageCodeGenExec = plan.find {
            case _: WholeStageCodegenExec => true
            case _ => false
          }
          assert(wholeStageCodeGenExec.isDefined)
          val code = wholeStageCodeGenExec.get.asInstanceOf[WholeStageCodegenExec].doCodeGen()._2
          assert(code.body.contains("project_doConsume") == hasSplit)
        }
      }
    }
  }

  test("codegen stage IDs should be preserved in transformations after CollapseCodegenStages") {
    // test case adapted from DataFrameSuite to trigger ReuseExchange
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2") {
      val df = spark.range(100)
      val join = df.join(df, "id")
      val plan = join.queryExecution.executedPlan
      assert(!plan.exists(p =>
        p.isInstanceOf[WholeStageCodegenExec] &&
          p.asInstanceOf[WholeStageCodegenExec].codegenStageId == 0),
        "codegen stage IDs should be preserved through ReuseExchange")
      checkAnswer(join, df.toDF())
    }
  }

  test("including codegen stage ID in generated class name should not regress codegen caching") {
    import testImplicits._

    withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_USE_ID_IN_CLASS_NAME.key -> "true") {
      // the same query run twice should produce identical code, which would imply a hit in
      // the generated code cache.
      val ds1 = spark.range(3).select($"id" + 2)
      val code1 = genCode(ds1)
      val ds2 = spark.range(3).select($"id" + 2)
      val code2 = genCode(ds2) // same query shape as above, deliberately
      assert(code1 == code2, "Should produce same code")
    }
  }

  ignore("SPARK-23598: Codegen working for lots of aggregation operations without runtime errors") {
    withSQLConf(SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      var df = Seq((8, "bat"), (15, "mouse"), (5, "horse")).toDF("age", "name")
      for (i <- 0 until 70) {
        df = df.groupBy("name").agg(avg("age").alias("age"))
      }
      assert(df.limit(1).collect() === Array(Row("bat", 8.0)))
    }
  }

  test("SPARK-25767: Lazy evaluated stream of expressions handled correctly") {
    val a = Seq(1).toDF("key")
    val b = Seq((1, "a")).toDF("key", "value")
    val c = Seq(1).toDF("key")

    val ab = a.join(b, LazyList("key"), "left")
    val abc = ab.join(c, Seq("key"), "left")

    checkAnswer(abc, Row(1, "a"))
  }

  test("SPARK-26680: Stream in groupBy does not cause StackOverflowError") {
    val groupByCols = LazyList(col("key"))
    val df = Seq((1, 2), (2, 3), (1, 3)).toDF("key", "value")
      .groupBy(groupByCols: _*)
      .max("value")

    checkAnswer(df, Seq(Row(1, 3), Row(2, 3)))
  }

  test("SPARK-26572: evaluate non-deterministic expressions for aggregate results") {
    withSQLConf(
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> Long.MaxValue.toString,
      SQLConf.SHUFFLE_PARTITIONS.key -> "1") {
      val baseTable = Seq(1, 1).toDF("idx")

      // BroadcastHashJoinExec with a HashAggregateExec child containing no aggregate expressions
      val distinctWithId = baseTable.distinct().withColumn("id", monotonically_increasing_id())
        .join(baseTable, "idx")
      assert(distinctWithId.queryExecution.executedPlan.exists {
        case WholeStageCodegenExec(
          ProjectExec(_, BroadcastHashJoinExec(_, _, _, _, _, _: HashAggregateExec, _, _))) => true
        case _ => false
      })
      checkAnswer(distinctWithId, Seq(Row(1, 0), Row(1, 0)))

      // BroadcastHashJoinExec with a HashAggregateExec child containing a Final mode aggregate
      // expression
      val groupByWithId =
        baseTable.groupBy("idx").sum().withColumn("id", monotonically_increasing_id())
        .join(baseTable, "idx")
      assert(groupByWithId.queryExecution.executedPlan.exists {
        case WholeStageCodegenExec(
          ProjectExec(_, BroadcastHashJoinExec(_, _, _, _, _, _: HashAggregateExec, _, _))) => true
        case _ => false
      })
      checkAnswer(groupByWithId, Seq(Row(1, 2, 0), Row(1, 2, 0)))
    }
  }

  test("SPARK-28520: WholeStageCodegen does not work properly for LocalTableScanExec") {
    // Case1: LocalTableScanExec is the root of a query plan tree.
    // In this case, WholeStageCodegenExec should not be inserted
    // as the direct parent of LocalTableScanExec.
    val df = Seq(1, 2, 3).toDF()
    val rootOfExecutedPlan = df.queryExecution.executedPlan

    // Ensure WholeStageCodegenExec is not inserted and
    // LocalTableScanExec is still the root.
    assert(rootOfExecutedPlan.isInstanceOf[LocalTableScanExec],
      "LocalTableScanExec should be still the root.")

    // Case2: The parent of a LocalTableScanExec supports WholeStageCodegen.
    // In this case, the LocalTableScanExec should be within a WholeStageCodegen domain
    // and no more InputAdapter is inserted as the direct parent of the LocalTableScanExec.
    val aggregatedDF = Seq(1, 2, 3).toDF().groupBy("value").sum()
    val executedPlan = aggregatedDF.queryExecution.executedPlan

    // HashAggregateExec supports WholeStageCodegen and it's the parent of
    // LocalTableScanExec so LocalTableScanExec should be within a WholeStageCodegen domain.
    assert(
      executedPlan.exists {
        case WholeStageCodegenExec(
          HashAggregateExec(_, _, _, _, _, _, _, _, _: LocalTableScanExec)) => true
        case _ => false
      },
      "LocalTableScanExec should be within a WholeStageCodegen domain.")
  }

  test("Give up splitting aggregate code if a parameter length goes over the limit") {
    withSQLConf(
        SQLConf.CODEGEN_SPLIT_AGGREGATE_FUNC.key -> "true",
        SQLConf.CODEGEN_METHOD_SPLIT_THRESHOLD.key -> "1",
        "spark.sql.CodeGenerator.validParamLength" -> "0") {
      withTable("t") {
        val expectedErrMsg = "Failed to split aggregate code into small functions.*"
        Seq(
          // Test case without keys
          "SELECT AVG(v) FROM VALUES(1) t(v)",
          // Tet case with keys
          "SELECT k, AVG(v) FROM VALUES((1, 1)) t(k, v) GROUP BY k").foreach { query =>
          checkError(
            exception = intercept[SparkException] {
              sql(query).collect()
            },
            errorClass = "INTERNAL_ERROR",
            parameters = Map("message" -> expectedErrMsg),
            matchPVals = true)
        }
      }
    }
  }

  test("Give up splitting subexpression code if a parameter length goes over the limit") {
    withSQLConf(
        SQLConf.CODEGEN_SPLIT_AGGREGATE_FUNC.key -> "false",
        SQLConf.CODEGEN_METHOD_SPLIT_THRESHOLD.key -> "1",
        "spark.sql.CodeGenerator.validParamLength" -> "0") {
      withTable("t") {
        val expectedErrMsg = "Failed to split subexpression code into small functions.*"
        Seq(
          // Test case without keys
          "SELECT AVG(a + b), SUM(a + b + c) FROM VALUES((1, 1, 1)) t(a, b, c)",
          // Tet case with keys
          "SELECT k, AVG(a + b), SUM(a + b + c) FROM VALUES((1, 1, 1, 1)) t(k, a, b, c) " +
            "GROUP BY k").foreach { query =>
          checkError(
            exception = intercept[SparkException] {
              sql(query).collect()
            },
            errorClass = "INTERNAL_ERROR",
            parameters = Map("message" -> expectedErrMsg),
            matchPVals = true)
        }
      }
    }
  }

  test("SPARK-47238: Test broadcast threshold for generated code") {
    // case 1: threshold is -1, shouldn't broadcast since smaller than 0 means disabled.
    // case 2: threshold is a larger number, shouldn't broadcast since not yet exceeded.
    // case 3: threshold is 0, should broadcast since it's always smaller than generated code size.
    Seq((-1, false), (1000000000, false), (0, true)).foreach { case (threshold, shouldBroadcast) =>
      withSQLConf(SQLConf.WHOLESTAGE_BROADCAST_CLEANED_SOURCE_THRESHOLD.key -> threshold.toString,
        SQLConf.USE_PARTITION_EVALUATOR.key -> "true") {
        val df = Seq(0, 1, 2).toDF().groupBy("value").sum()
        // Invoke WholeStageCodegenExec.execute and cast the rdd, and then get
        // the evaluatorFactory to check whether it uses broadcast as expected.
        val wscgExec = df.queryExecution.executedPlan.collectFirst {
          case exec: WholeStageCodegenExec => exec
        }
        assert(wscgExec match {
          case Some(exec) =>
            val rdd = exec.execute().asInstanceOf[MapPartitionsWithEvaluatorRDD[_, _]]
            val evaluatorFactoryField = rdd.getClass.getDeclaredField("evaluatorFactory")
            evaluatorFactoryField.setAccessible(true)
            val evaluatorFactory = evaluatorFactoryField.get(rdd)
            val cleanedSourceOptField = evaluatorFactory.getClass.getDeclaredField(
              "org$apache$spark$sql$execution$WholeStageCodegenEvaluatorFactory$$cleanedSourceOpt")
            cleanedSourceOptField.setAccessible(true)
            cleanedSourceOptField.get(evaluatorFactory).asInstanceOf[Either[_, _]] match {
              case Left(_) => shouldBroadcast
              case Right(_) => !shouldBroadcast
            }
          case None => false
        })
        // Execute and validate that the executor side still yields the correct result.
        checkAnswer(df, Seq(Row(0, 0), Row(1, 1), Row(2, 2)))
      }
    }
  }
}
