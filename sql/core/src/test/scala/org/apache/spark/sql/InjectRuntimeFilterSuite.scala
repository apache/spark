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

import org.apache.spark.sql.catalyst.expressions.{Alias, BloomFilterMightContain, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, BloomFilterAggregate}
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types.{IntegerType, StructType}

class InjectRuntimeFilterSuite extends QueryTest with SQLTestUtils with SharedSparkSession {

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    val schema = new StructType().add("a1", IntegerType, nullable = true)
      .add("b1", IntegerType, nullable = true)
      .add("c1", IntegerType, nullable = true)
      .add("d1", IntegerType, nullable = true)
      .add("e1", IntegerType, nullable = true)
      .add("f1", IntegerType, nullable = true)

    val data1 = Seq(Seq(null, 47, null, 4, 6, 48),
      Seq(73, 63, null, 92, null, null),
      Seq(76, 10, 74, 98, 37, 5),
      Seq(0, 63, null, null, null, null),
      Seq(15, 77, null, null, null, null),
      Seq(null, 57, 33, 55, null, 58),
      Seq(4, 0, 86, null, 96, 14),
      Seq(28, 16, 58, null, null, null),
      Seq(1, 88, null, 8, null, 79),
      Seq(59, null, null, null, 20, 25),
      Seq(1, 50, null, 94, 94, null),
      Seq(null, null, null, 67, 51, 57),
      Seq(77, 50, 8, 90, 16, 21),
      Seq(34, 28, null, 5, null, 64),
      Seq(null, null, 88, 11, 63, 79),
      Seq(92, 94, 23, 1, null, 64),
      Seq(57, 56, null, 83, null, null),
      Seq(null, 35, 8, 35, null, 70),
      Seq(null, 8, null, 35, null, 87),
      Seq(9, null, null, 60, null, 5),
      Seq(null, 15, 66, null, 83, null))
    val rdd1 = spark.sparkContext.parallelize(data1)
    val rddRow1 = rdd1.map(s => Row.fromSeq(s))
    spark.createDataFrame(rddRow1, schema).write.saveAsTable("bf1")

    val schema2 = new StructType().add("a2", IntegerType, nullable = true)
      .add("b2", IntegerType, nullable = true)
      .add("c2", IntegerType, nullable = true)
      .add("d2", IntegerType, nullable = true)
      .add("e2", IntegerType, nullable = true)
      .add("f2", IntegerType, nullable = true)


    val data2 = Seq(Seq(67, 17, 45, 91, null, null),
      Seq(98, 63, 0, 89, null, 40),
      Seq(null, 76, 68, 75, 20, 19),
      Seq(8, null, null, null, 78, null),
      Seq(48, 62, null, null, 11, 98),
      Seq(84, null, 99, 65, 66, 51),
      Seq(98, null, null, null, 42, 51),
      Seq(10, 3, 29, null, 68, 8),
      Seq(85, 36, 41, null, 28, 71),
      Seq(89, null, 94, 95, 67, 21),
      Seq(44, null, 24, 33, null, 6),
      Seq(null, 6, 78, 31, null, 69),
      Seq(59, 2, 63, 9, 66, 20),
      Seq(5, 23, 10, 86, 68, null),
      Seq(null, 63, 99, 55, 9, 65),
      Seq(57, 62, 68, 5, null, 0),
      Seq(75, null, 15, null, 81, null),
      Seq(53, null, 6, 68, 28, 13),
      Seq(null, null, null, null, 89, 23),
      Seq(36, 73, 40, null, 8, null),
      Seq(24, null, null, 40, null, null))
    val rdd2 = spark.sparkContext.parallelize(data2)
    val rddRow2 = rdd2.map(s => Row.fromSeq(s))
    spark.createDataFrame(rddRow2, schema2).write.saveAsTable("bf2")

    val schema3 = new StructType().add("a3", IntegerType, nullable = true)
      .add("b3", IntegerType, nullable = true)
      .add("c3", IntegerType, nullable = true)
      .add("d3", IntegerType, nullable = true)
      .add("e3", IntegerType, nullable = true)
      .add("f3", IntegerType, nullable = true)

    val data3 = Seq(Seq(67, 17, 45, 91, null, null),
      Seq(98, 63, 0, 89, null, 40),
      Seq(null, 76, 68, 75, 20, 19),
      Seq(8, null, null, null, 78, null),
      Seq(48, 62, null, null, 11, 98),
      Seq(84, null, 99, 65, 66, 51),
      Seq(98, null, null, null, 42, 51),
      Seq(10, 3, 29, null, 68, 8),
      Seq(85, 36, 41, null, 28, 71),
      Seq(89, null, 94, 95, 67, 21),
      Seq(44, null, 24, 33, null, 6),
      Seq(null, 6, 78, 31, null, 69),
      Seq(59, 2, 63, 9, 66, 20),
      Seq(5, 23, 10, 86, 68, null),
      Seq(null, 63, 99, 55, 9, 65),
      Seq(57, 62, 68, 5, null, 0),
      Seq(75, null, 15, null, 81, null),
      Seq(53, null, 6, 68, 28, 13),
      Seq(null, null, null, null, 89, 23),
      Seq(36, 73, 40, null, 8, null),
      Seq(24, null, null, 40, null, null))
    val rdd3 = spark.sparkContext.parallelize(data3)
    val rddRow3 = rdd3.map(s => Row.fromSeq(s))
    spark.createDataFrame(rddRow3, schema3).write.saveAsTable("bf3")


    val schema4 = new StructType().add("a4", IntegerType, nullable = true)
      .add("b4", IntegerType, nullable = true)
      .add("c4", IntegerType, nullable = true)
      .add("d4", IntegerType, nullable = true)
      .add("e4", IntegerType, nullable = true)
      .add("f4", IntegerType, nullable = true)

    val data4 = Seq(Seq(67, 17, 45, 91, null, null),
      Seq(98, 63, 0, 89, null, 40),
      Seq(null, 76, 68, 75, 20, 19),
      Seq(8, null, null, null, 78, null),
      Seq(48, 62, null, null, 11, 98),
      Seq(84, null, 99, 65, 66, 51),
      Seq(98, null, null, null, 42, 51),
      Seq(10, 3, 29, null, 68, 8),
      Seq(85, 36, 41, null, 28, 71),
      Seq(89, null, 94, 95, 67, 21),
      Seq(44, null, 24, 33, null, 6),
      Seq(null, 6, 78, 31, null, 69),
      Seq(59, 2, 63, 9, 66, 20),
      Seq(5, 23, 10, 86, 68, null),
      Seq(null, 63, 99, 55, 9, 65),
      Seq(57, 62, 68, 5, null, 0),
      Seq(75, null, 15, null, 81, null),
      Seq(53, null, 6, 68, 28, 13),
      Seq(null, null, null, null, 89, 23),
      Seq(36, 73, 40, null, 8, null),
      Seq(24, null, null, 40, null, null))
    val rdd4 = spark.sparkContext.parallelize(data4)
    val rddRow4 = rdd4.map(s => Row.fromSeq(s))
    spark.createDataFrame(rddRow4, schema4).write.saveAsTable("bf4")

    val schema5part = new StructType().add("a5", IntegerType, nullable = true)
      .add("b5", IntegerType, nullable = true)
      .add("c5", IntegerType, nullable = true)
      .add("d5", IntegerType, nullable = true)
      .add("e5", IntegerType, nullable = true)
      .add("f5", IntegerType, nullable = true)

    val data5part = Seq(Seq(67, 17, 45, 91, null, null),
      Seq(98, 63, 0, 89, null, 40),
      Seq(null, 76, 68, 75, 20, 19),
      Seq(8, null, null, null, 78, null),
      Seq(48, 62, null, null, 11, 98),
      Seq(84, null, 99, 65, 66, 51),
      Seq(98, null, null, null, 42, 51),
      Seq(10, 3, 29, null, 68, 8),
      Seq(85, 36, 41, null, 28, 71),
      Seq(89, null, 94, 95, 67, 21),
      Seq(44, null, 24, 33, null, 6),
      Seq(null, 6, 78, 31, null, 69),
      Seq(59, 2, 63, 9, 66, 20),
      Seq(5, 23, 10, 86, 68, null),
      Seq(null, 63, 99, 55, 9, 65),
      Seq(57, 62, 68, 5, null, 0),
      Seq(75, null, 15, null, 81, null),
      Seq(53, null, 6, 68, 28, 13),
      Seq(null, null, null, null, 89, 23),
      Seq(36, 73, 40, null, 8, null),
      Seq(24, null, null, 40, null, null))
    val rdd5part = spark.sparkContext.parallelize(data5part)
    val rddRow5part = rdd5part.map(s => Row.fromSeq(s))
    spark.createDataFrame(rddRow5part, schema5part).write.partitionBy("f5")
      .saveAsTable("bf5part")
    spark.createDataFrame(rddRow5part, schema5part).filter("a5 > 30")
      .write.partitionBy("f5")
      .saveAsTable("bf5filtered")

    sql("analyze table bf1 compute statistics for columns a1, b1, c1, d1, e1, f1")
    sql("analyze table bf2 compute statistics for columns a2, b2, c2, d2, e2, f2")
    sql("analyze table bf3 compute statistics for columns a3, b3, c3, d3, e3, f3")
    sql("analyze table bf4 compute statistics for columns a4, b4, c4, d4, e4, f4")
    sql("analyze table bf5part compute statistics for columns a5, b5, c5, d5, e5, f5")
    sql("analyze table bf5filtered compute statistics for columns a5, b5, c5, d5, e5, f5")
  }

  protected override def afterAll(): Unit = try {
    sql("DROP TABLE IF EXISTS bf1")
    sql("DROP TABLE IF EXISTS bf2")
    sql("DROP TABLE IF EXISTS bf3")
    sql("DROP TABLE IF EXISTS bf4")
    sql("DROP TABLE IF EXISTS bf5part")
    sql("DROP TABLE IF EXISTS bf5filtered")
  } finally {
    super.afterAll()
  }

  private def ensureLeftSemiJoinExists(plan: LogicalPlan): Unit = {
    assert(
      plan.find {
        case j: Join if j.joinType == LeftSemi => true
        case _ => false
      }.isDefined
    )
  }

  def checkWithAndWithoutFeatureEnabled(query: String, testSemiJoin: Boolean,
      shouldReplace: Boolean): Unit = {
    var planDisabled: LogicalPlan = null
    var planEnabled: LogicalPlan = null
    var expectedAnswer: Array[Row] = null

    withSQLConf(SQLConf.RUNTIME_FILTER_SEMI_JOIN_REDUCTION_ENABLED.key -> "false",
      SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false") {
      planDisabled = sql(query).queryExecution.optimizedPlan
      expectedAnswer = sql(query).collect()
    }

    if (testSemiJoin) {
      withSQLConf(SQLConf.RUNTIME_FILTER_SEMI_JOIN_REDUCTION_ENABLED.key -> "true",
        SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false") {
        planEnabled = sql(query).queryExecution.optimizedPlan
        checkAnswer(sql(query), expectedAnswer)
      }
      if (shouldReplace) {
        val normalizedEnabled = normalizePlan(normalizeExprIds(planEnabled))
        val normalizedDisabled = normalizePlan(normalizeExprIds(planDisabled))
        ensureLeftSemiJoinExists(planEnabled)
        assert(normalizedEnabled != normalizedDisabled)
      } else {
        comparePlans(planDisabled, planEnabled)
      }
    } else {
      withSQLConf(SQLConf.RUNTIME_FILTER_SEMI_JOIN_REDUCTION_ENABLED.key -> "false",
        SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true") {
        planEnabled = sql(query).queryExecution.optimizedPlan
        checkAnswer(sql(query), expectedAnswer)
        if (shouldReplace) {
          assert(!columnPruningTakesEffect(planEnabled))
          assert(getNumBloomFilters(planEnabled) > getNumBloomFilters(planDisabled))
        } else {
          assert(getNumBloomFilters(planEnabled) == getNumBloomFilters(planDisabled))
        }
      }
    }
  }

  def getNumBloomFilters(plan: LogicalPlan): Integer = {
    val numBloomFilterAggs = plan.collect {
      case Filter(condition, _) => condition.collect {
        case subquery: org.apache.spark.sql.catalyst.expressions.ScalarSubquery
        => subquery.plan.collect {
          case Aggregate(_, aggregateExpressions, _) =>
            aggregateExpressions.map {
              case Alias(AggregateExpression(bfAgg : BloomFilterAggregate, _, _, _, _),
              _) =>
                assert(bfAgg.estimatedNumItemsExpression.isInstanceOf[Literal])
                assert(bfAgg.numBitsExpression.isInstanceOf[Literal])
                1
            }.sum
        }.sum
      }.sum
    }.sum
    val numMightContains = plan.collect {
      case Filter(condition, _) => condition.collect {
        case BloomFilterMightContain(_, _) => 1
      }.sum
    }.sum
    assert(numBloomFilterAggs == numMightContains)
    numMightContains
  }

  def columnPruningTakesEffect(plan: LogicalPlan): Boolean = {
    def takesEffect(plan: LogicalPlan): Boolean = {
      val result = org.apache.spark.sql.catalyst.optimizer.ColumnPruning.apply(plan)
      !result.fastEquals(plan)
    }

    plan.collectFirst {
      case Filter(condition, _) if condition.collectFirst {
        case subquery: org.apache.spark.sql.catalyst.expressions.ScalarSubquery
          if takesEffect(subquery.plan) => true
      }.nonEmpty => true
    }.nonEmpty
  }

  def assertRewroteSemiJoin(query: String): Unit = {
    checkWithAndWithoutFeatureEnabled(query, testSemiJoin = true, shouldReplace = true)
  }

  def assertDidNotRewriteSemiJoin(query: String): Unit = {
    checkWithAndWithoutFeatureEnabled(query, testSemiJoin = true, shouldReplace = false)
  }

  def assertRewroteWithBloomFilter(query: String): Unit = {
    checkWithAndWithoutFeatureEnabled(query, testSemiJoin = false, shouldReplace = true)
  }

  def assertDidNotRewriteWithBloomFilter(query: String): Unit = {
    checkWithAndWithoutFeatureEnabled(query, testSemiJoin = false, shouldReplace = false)
  }

  test("Runtime semi join reduction: simple") {
    // Filter creation side is 3409 bytes
    // Filter application side scan is 3362 bytes
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000") {
      assertRewroteSemiJoin("select * from bf1 join bf2 on bf1.c1 = bf2.c2 where bf2.a2 = 62")
      assertDidNotRewriteSemiJoin("select * from bf1 join bf2 on bf1.c1 = bf2.c2")
    }
  }

  test("Runtime semi join reduction: two joins") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000") {
      assertRewroteSemiJoin("select * from bf1 join bf2 join bf3 on bf1.c1 = bf2.c2 " +
        "and bf3.c3 = bf2.c2 where bf2.a2 = 5")
    }
  }

  test("Runtime semi join reduction: three joins") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000") {
      assertRewroteSemiJoin("select * from bf1 join bf2 join bf3 join bf4 on " +
        "bf1.c1 = bf2.c2 and bf2.c2 = bf3.c3 and bf3.c3 = bf4.c4 where bf1.a1 = 5")
    }
  }

  test("Runtime semi join reduction: simple expressions only") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000") {
      val squared = (s: Long) => {
        s * s
      }
      spark.udf.register("square", squared)
      assertDidNotRewriteSemiJoin("select * from bf1 join bf2 on " +
        "bf1.c1 = bf2.c2 where square(bf2.a2) = 62")
      assertDidNotRewriteSemiJoin("select * from bf1 join bf2 on " +
        "bf1.c1 = square(bf2.c2) where bf2.a2= 62")
    }
  }

  test("Runtime bloom filter join: simple") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000") {
      assertRewroteWithBloomFilter("select * from bf1 join bf2 on bf1.c1 = bf2.c2 " +
        "where bf2.a2 = 62")
      assertDidNotRewriteWithBloomFilter("select * from bf1 join bf2 on bf1.c1 = bf2.c2")
    }
  }

  test("Runtime bloom filter join: two filters single join") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000") {
      var planDisabled: LogicalPlan = null
      var planEnabled: LogicalPlan = null
      var expectedAnswer: Array[Row] = null

      val query = "select * from bf1 join bf2 on bf1.c1 = bf2.c2 and " +
        "bf1.b1 = bf2.b2 where bf2.a2 = 62"

      withSQLConf(SQLConf.RUNTIME_FILTER_SEMI_JOIN_REDUCTION_ENABLED.key -> "false",
        SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false") {
        planDisabled = sql(query).queryExecution.optimizedPlan
        expectedAnswer = sql(query).collect()
      }

      withSQLConf(SQLConf.RUNTIME_FILTER_SEMI_JOIN_REDUCTION_ENABLED.key -> "false",
        SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true") {
        planEnabled = sql(query).queryExecution.optimizedPlan
        checkAnswer(sql(query), expectedAnswer)
      }
      assert(getNumBloomFilters(planEnabled) == getNumBloomFilters(planDisabled) + 2)
    }
  }

  test("Runtime bloom filter join: test the number of filter threshold") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000") {
      var planDisabled: LogicalPlan = null
      var planEnabled: LogicalPlan = null
      var expectedAnswer: Array[Row] = null

      val query = "select * from bf1 join bf2 on bf1.c1 = bf2.c2 and " +
        "bf1.b1 = bf2.b2 where bf2.a2 = 62"

      withSQLConf(SQLConf.RUNTIME_FILTER_SEMI_JOIN_REDUCTION_ENABLED.key -> "false",
        SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false") {
        planDisabled = sql(query).queryExecution.optimizedPlan
        expectedAnswer = sql(query).collect()
      }

      for (numFilterThreshold <- 0 to 3) {
        withSQLConf(SQLConf.RUNTIME_FILTER_SEMI_JOIN_REDUCTION_ENABLED.key -> "false",
          SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true",
          SQLConf.RUNTIME_FILTER_NUMBER_THRESHOLD.key -> numFilterThreshold.toString) {
          planEnabled = sql(query).queryExecution.optimizedPlan
          checkAnswer(sql(query), expectedAnswer)
        }
        if (numFilterThreshold < 3) {
          assert(getNumBloomFilters(planEnabled) == getNumBloomFilters(planDisabled)
            + numFilterThreshold)
        } else {
          assert(getNumBloomFilters(planEnabled) == getNumBloomFilters(planDisabled) + 2)
        }
      }
    }
  }

  test("Runtime bloom filter join: insert one bloom filter per column") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000") {
      var planDisabled: LogicalPlan = null
      var planEnabled: LogicalPlan = null
      var expectedAnswer: Array[Row] = null

      val query = "select * from bf1 join bf2 on bf1.c1 = bf2.c2 and " +
        "bf1.c1 = bf2.b2 where bf2.a2 = 62"

      withSQLConf(SQLConf.RUNTIME_FILTER_SEMI_JOIN_REDUCTION_ENABLED.key -> "false",
        SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "false") {
        planDisabled = sql(query).queryExecution.optimizedPlan
        expectedAnswer = sql(query).collect()
      }

      withSQLConf(SQLConf.RUNTIME_FILTER_SEMI_JOIN_REDUCTION_ENABLED.key -> "false",
        SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true") {
        planEnabled = sql(query).queryExecution.optimizedPlan
        checkAnswer(sql(query), expectedAnswer)
      }
      assert(getNumBloomFilters(planEnabled) == getNumBloomFilters(planDisabled) + 1)
    }
  }

  test("Runtime bloom filter join: do not add bloom filter if dpp filter exists " +
    "on the same column") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000") {
      assertDidNotRewriteWithBloomFilter("select * from bf5part join bf2 on " +
        "bf5part.f5 = bf2.c2 where bf2.a2 = 62")
    }
  }

  test("Runtime bloom filter join: add bloom filter if dpp filter exists on " +
    "a different column") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000") {
      assertRewroteWithBloomFilter("select * from bf5part join bf2 on " +
        "bf5part.c5 = bf2.c2 and bf5part.f5 = bf2.f2 where bf2.a2 = 62")
    }
  }

  test("Runtime bloom filter join: BF rewrite triggering threshold test") {
    // Filter creation side data size is 3409 bytes. On the filter application side, an individual
    // scan's byte size is 3362.
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "3000",
      SQLConf.RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> "4000"
    ) {
      assertRewroteWithBloomFilter("select * from bf1 join bf2 on bf1.c1 = bf2.c2 " +
        "where bf2.a2 = 62")
    }
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "50",
      SQLConf.RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> "50"
    ) {
      assertDidNotRewriteWithBloomFilter("select * from bf1 join bf2 on bf1.c1 = bf2.c2 " +
        "where bf2.a2 = 62")
    }
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "5000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "3000",
      SQLConf.RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> "4000"
    ) {
      // Rewrite should not be triggered as the Bloom filter application side scan size is small.
      assertDidNotRewriteWithBloomFilter("select * from bf1 join bf2 on bf1.c1 = bf2.c2 "
        + "where bf2.a2 = 62")
    }
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "32",
      SQLConf.RUNTIME_BLOOM_FILTER_CREATION_SIDE_THRESHOLD.key -> "4000") {
      // Test that the max scan size rather than an individual scan size on the filter
      // application side matters. `bf5filtered` has 14168 bytes and `bf2` has 3409 bytes.
      withSQLConf(
        SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "5000") {
        assertRewroteWithBloomFilter("select * from " +
          "(select * from bf5filtered union all select * from bf2) t " +
          "join bf3 on t.c5 = bf3.c3 where bf3.a3 = 5")
      }
      withSQLConf(
        SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "15000") {
        assertDidNotRewriteWithBloomFilter("select * from " +
          "(select * from bf5filtered union all select * from bf2) t " +
          "join bf3 on t.c5 = bf3.c3 where bf3.a3 = 5")
      }
    }
  }

  test("Runtime bloom filter join: simple expressions only") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "2000") {
      val squared = (s: Long) => {
        s * s
      }
      spark.udf.register("square", squared)
      assertDidNotRewriteWithBloomFilter("select * from bf1 join bf2 on " +
        "bf1.c1 = bf2.c2 where square(bf2.a2) = 62" )
      assertDidNotRewriteWithBloomFilter("select * from bf1 join bf2 on " +
        "bf1.c1 = square(bf2.c2) where bf2.a2 = 62" )
    }
  }

  test("Support Left Semi join in row level runtime filters") {
    withSQLConf(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "3000",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "32") {
      assertRewroteWithBloomFilter(
        """
          |SELECT *
          |FROM   bf1 LEFT SEMI
          |JOIN   (SELECT * FROM bf2 WHERE bf2.a2 = 62) tmp
          |ON     bf1.c1 = tmp.c2
        """.stripMargin)
    }
  }
}
