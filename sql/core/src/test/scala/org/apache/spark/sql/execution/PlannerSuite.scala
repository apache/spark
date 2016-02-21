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

import org.apache.spark.sql.{execution, Row, SQLConf}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Repartition}
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.joins.{BroadcastHashJoin, SortMergeJoin}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

class PlannerSuite extends SharedSQLContext {
  import testImplicits._

  setupTestData()

  private def testPartialAggregationPlan(query: LogicalPlan): Unit = {
    val planner = sqlContext.planner
    import planner._
    val plannedOption = Aggregation(query).headOption
    val planned =
      plannedOption.getOrElse(
        fail(s"Could query play aggregation query $query. Is it an aggregation query?"))
    val aggregations = planned.collect { case n if n.nodeName contains "Aggregate" => n }

    // For the new aggregation code path, there will be four aggregate operator for
    // distinct aggregations.
    assert(
      aggregations.size == 2 || aggregations.size == 4,
      s"The plan of query $query does not have partial aggregations.")
  }

  test("count is partially aggregated") {
    val query = testData.groupBy('value).agg(count('key)).queryExecution.analyzed
    testPartialAggregationPlan(query)
  }

  test("count distinct is partially aggregated") {
    val query = testData.groupBy('value).agg(countDistinct('key)).queryExecution.analyzed
    testPartialAggregationPlan(query)
  }

  test("mixed aggregates are partially aggregated") {
    val query =
      testData.groupBy('value).agg(count('value), countDistinct('key)).queryExecution.analyzed
    testPartialAggregationPlan(query)
  }

  test("sizeInBytes estimation of limit operator for broadcast hash join optimization") {
    def checkPlan(fieldTypes: Seq[DataType]): Unit = {
      withTempTable("testLimit") {
        val fields = fieldTypes.zipWithIndex.map {
          case (dataType, index) => StructField(s"c${index}", dataType, true)
        } :+ StructField("key", IntegerType, true)
        val schema = StructType(fields)
        val row = Row.fromSeq(Seq.fill(fields.size)(null))
        val rowRDD = sparkContext.parallelize(row :: Nil)
        sqlContext.createDataFrame(rowRDD, schema).registerTempTable("testLimit")

        val planned = sql(
          """
            |SELECT l.a, l.b
            |FROM testData2 l JOIN (SELECT * FROM testLimit LIMIT 1) r ON (l.a = r.key)
          """.stripMargin).queryExecution.sparkPlan

        val broadcastHashJoins = planned.collect { case join: BroadcastHashJoin => join }
        val sortMergeJoins = planned.collect { case join: SortMergeJoin => join }

        assert(broadcastHashJoins.size === 1, "Should use broadcast hash join")
        assert(sortMergeJoins.isEmpty, "Should not use sort merge join")
      }
    }

    val simpleTypes =
      NullType ::
      BooleanType ::
      ByteType ::
      ShortType ::
      IntegerType ::
      LongType ::
      FloatType ::
      DoubleType ::
      DecimalType(10, 5) ::
      DecimalType.SYSTEM_DEFAULT ::
      DateType ::
      TimestampType ::
      StringType ::
      BinaryType :: Nil

    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "16434") {
      checkPlan(simpleTypes)
    }

    val complexTypes =
      ArrayType(DoubleType, true) ::
      ArrayType(StringType, false) ::
      MapType(IntegerType, StringType, true) ::
      MapType(IntegerType, ArrayType(DoubleType), false) ::
      StructType(Seq(
        StructField("a", IntegerType, nullable = true),
        StructField("b", ArrayType(DoubleType), nullable = false),
        StructField("c", DoubleType, nullable = false))) :: Nil

    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "901617") {
      checkPlan(complexTypes)
    }
  }

  test("InMemoryRelation statistics propagation") {
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "81920") {
      withTempTable("tiny") {
        testData.limit(3).registerTempTable("tiny")
        sql("CACHE TABLE tiny")

        val a = testData.as("a")
        val b = sqlContext.table("tiny").as("b")
        val planned = a.join(b, $"a.key" === $"b.key").queryExecution.sparkPlan

        val broadcastHashJoins = planned.collect { case join: BroadcastHashJoin => join }
        val sortMergeJoins = planned.collect { case join: SortMergeJoin => join }

        assert(broadcastHashJoins.size === 1, "Should use broadcast hash join")
        assert(sortMergeJoins.isEmpty, "Should not use sort merge join")

        sqlContext.clearCache()
      }
    }
  }

  test("SPARK-11390 explain should print PushedFilters of PhysicalRDD") {
    withTempPath { file =>
      val path = file.getCanonicalPath
      testData.write.parquet(path)
      val df = sqlContext.read.parquet(path)
      sqlContext.registerDataFrameAsTable(df, "testPushed")

      withTempTable("testPushed") {
        val exp = sql("select * from testPushed where key = 15").queryExecution.sparkPlan
        assert(exp.toString.contains("PushedFilters: [EqualTo(key,15)]"))
      }
    }
  }

  test("efficient terminal limit -> sort should use TakeOrderedAndProject") {
    val query = testData.select('key, 'value).sort('key).limit(2)
    val planned = query.queryExecution.executedPlan
    assert(planned.isInstanceOf[execution.TakeOrderedAndProject])
    assert(planned.output === testData.select('key, 'value).logicalPlan.output)
  }

  test("terminal limit -> project -> sort should use TakeOrderedAndProject") {
    val query = testData.select('key, 'value).sort('key).select('value, 'key).limit(2)
    val planned = query.queryExecution.executedPlan
    assert(planned.isInstanceOf[execution.TakeOrderedAndProject])
    assert(planned.output === testData.select('value, 'key).logicalPlan.output)
  }

  test("terminal limits that are not handled by TakeOrderedAndProject should use CollectLimit") {
    val query = testData.select('value).limit(2)
    val planned = query.queryExecution.sparkPlan
    assert(planned.isInstanceOf[CollectLimit])
    assert(planned.output === testData.select('value).logicalPlan.output)
  }

  test("TakeOrderedAndProject can appear in the middle of plans") {
    val query = testData.select('key, 'value).sort('key).limit(2).filter('key === 3)
    val planned = query.queryExecution.executedPlan
    assert(planned.find(_.isInstanceOf[TakeOrderedAndProject]).isDefined)
  }

  test("CollectLimit can appear in the middle of a plan when caching is used") {
    val query = testData.select('key, 'value).limit(2).cache()
    val planned = query.queryExecution.optimizedPlan.asInstanceOf[InMemoryRelation]
    assert(planned.child.isInstanceOf[CollectLimit])
  }

  test("collapse adjacent repartitions") {
    val doubleRepartitioned = testData.repartition(10).repartition(20).coalesce(5)
    def countRepartitions(plan: LogicalPlan): Int = plan.collect { case r: Repartition => r }.length
    assert(countRepartitions(doubleRepartitioned.queryExecution.logical) === 3)
    assert(countRepartitions(doubleRepartitioned.queryExecution.optimizedPlan) === 1)
    doubleRepartitioned.queryExecution.optimizedPlan match {
      case r: Repartition =>
        assert(r.numPartitions === 5)
        assert(r.shuffle === false)
    }
  }
}
