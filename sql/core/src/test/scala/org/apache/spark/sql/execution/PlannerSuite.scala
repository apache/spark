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

import org.apache.spark.SparkFunSuite
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.TestData._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions  .{Ascending, Literal, Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.joins.{BroadcastHashJoin, ShuffledHashJoin}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.{SQLTestUtils, TestSQLContext}
import org.apache.spark.sql.test.TestSQLContext._
import org.apache.spark.sql.test.TestSQLContext.implicits._
import org.apache.spark.sql.test.TestSQLContext.planner._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SQLContext, Row, SQLConf, execution}


class PlannerSuite extends SparkFunSuite with SQLTestUtils {

  override def sqlContext: SQLContext = TestSQLContext

  private def testPartialAggregationPlan(query: LogicalPlan): Unit = {
    val plannedOption = HashAggregation(query).headOption.orElse(Aggregation(query).headOption)
    val planned =
      plannedOption.getOrElse(
        fail(s"Could query play aggregation query $query. Is it an aggregation query?"))
    val aggregations = planned.collect { case n if n.nodeName contains "Aggregate" => n }

    // For the new aggregation code path, there will be three aggregate operator for
    // distinct aggregations.
    assert(
      aggregations.size == 2 || aggregations.size == 3,
      s"The plan of query $query does not have partial aggregations.")
  }

  test("unions are collapsed") {
    val query = testData.unionAll(testData).unionAll(testData).logicalPlan
    val planned = BasicOperators(query).head
    val logicalUnions = query collect { case u: logical.Union => u }
    val physicalUnions = planned collect { case u: execution.Union => u }

    assert(logicalUnions.size === 2)
    assert(physicalUnions.size === 1)
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
    def checkPlan(fieldTypes: Seq[DataType], newThreshold: Int): Unit = {
      setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, newThreshold)
      val fields = fieldTypes.zipWithIndex.map {
        case (dataType, index) => StructField(s"c${index}", dataType, true)
      } :+ StructField("key", IntegerType, true)
      val schema = StructType(fields)
      val row = Row.fromSeq(Seq.fill(fields.size)(null))
      val rowRDD = org.apache.spark.sql.test.TestSQLContext.sparkContext.parallelize(row :: Nil)
      createDataFrame(rowRDD, schema).registerTempTable("testLimit")

      val planned = sql(
        """
          |SELECT l.a, l.b
          |FROM testData2 l JOIN (SELECT * FROM testLimit LIMIT 1) r ON (l.a = r.key)
        """.stripMargin).queryExecution.executedPlan

      val broadcastHashJoins = planned.collect { case join: BroadcastHashJoin => join }
      val shuffledHashJoins = planned.collect { case join: ShuffledHashJoin => join }

      assert(broadcastHashJoins.size === 1, "Should use broadcast hash join")
      assert(shuffledHashJoins.isEmpty, "Should not use shuffled hash join")

      dropTempTable("testLimit")
    }

    val origThreshold = conf.autoBroadcastJoinThreshold

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

    checkPlan(simpleTypes, newThreshold = 16434)

    val complexTypes =
      ArrayType(DoubleType, true) ::
      ArrayType(StringType, false) ::
      MapType(IntegerType, StringType, true) ::
      MapType(IntegerType, ArrayType(DoubleType), false) ::
      StructType(Seq(
        StructField("a", IntegerType, nullable = true),
        StructField("b", ArrayType(DoubleType), nullable = false),
        StructField("c", DoubleType, nullable = false))) :: Nil

    checkPlan(complexTypes, newThreshold = 901617)

    setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, origThreshold)
  }

  test("InMemoryRelation statistics propagation") {
    val origThreshold = conf.autoBroadcastJoinThreshold
    setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, 81920)

    testData.limit(3).registerTempTable("tiny")
    sql("CACHE TABLE tiny")

    val a = testData.as("a")
    val b = table("tiny").as("b")
    val planned = a.join(b, $"a.key" === $"b.key").queryExecution.executedPlan

    val broadcastHashJoins = planned.collect { case join: BroadcastHashJoin => join }
    val shuffledHashJoins = planned.collect { case join: ShuffledHashJoin => join }

    assert(broadcastHashJoins.size === 1, "Should use broadcast hash join")
    assert(shuffledHashJoins.isEmpty, "Should not use shuffled hash join")

    setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, origThreshold)
  }

  test("efficient limit -> project -> sort") {
    val query = testData.sort('key).select('value).limit(2).logicalPlan
    val planned = planner.TakeOrderedAndProject(query)
    assert(planned.head.isInstanceOf[execution.TakeOrderedAndProject])
  }

  test("PartitioningCollection") {
    withTempTable("normal", "small", "tiny") {
      testData.registerTempTable("normal")
      testData.limit(10).registerTempTable("small")
      testData.limit(3).registerTempTable("tiny")

      // Disable broadcast join
      withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
        {
          val numExchanges = sql(
            """
              |SELECT *
              |FROM
              |  normal JOIN small ON (normal.key = small.key)
              |  JOIN tiny ON (small.key = tiny.key)
            """.stripMargin
          ).queryExecution.executedPlan.collect {
            case exchange: Exchange => exchange
          }.length
          assert(numExchanges === 3)
        }

        {
          // This second query joins on different keys:
          val numExchanges = sql(
            """
              |SELECT *
              |FROM
              |  normal JOIN small ON (normal.key = small.key)
              |  JOIN tiny ON (normal.key = tiny.key)
            """.stripMargin
          ).queryExecution.executedPlan.collect {
            case exchange: Exchange => exchange
          }.length
          assert(numExchanges === 3)
        }

      }
    }
  }

  // --- Unit tests of EnsureRequirements ---------------------------------------------------------

  test("EnsureRequirements ensures that children produce same number of partitions when required") {
    val clustering = Literal(1) :: Nil
    val distribution = ClusteredDistribution(clustering)
    val inputPlan = DummyPlan(
      children = Seq(
        DummyPlan(outputPartitioning = HashPartitioning(clustering, 1)),
        DummyPlan(outputPartitioning = HashPartitioning(clustering, 2))
      ),
      requiresChildrenToProduceSameNumberOfPartitions = true,
      requiredChildDistribution = Seq(distribution, distribution),
      requiredChildOrdering = Seq(Seq.empty, Seq.empty)
    )
    val outputPlan = EnsureRequirements(sqlContext).apply(inputPlan)
    assert (outputPlan.children.map(_.outputPartitioning.numPartitions).toSet.size === 1)
  }

  test("EnsureRequirements should not repartition if only ordering requirement is unsatisfied") {
    val outputOrdering = Seq(SortOrder(Literal(1), Ascending))
    val distribution = ClusteredDistribution(Literal(1) :: Nil)
    val inputPlan = DummyPlan(
      children = Seq(
        DummyPlan(outputPartitioning = SinglePartition),
        DummyPlan(outputPartitioning = SinglePartition)
      ),
      requiresChildrenToProduceSameNumberOfPartitions = true,
      requiredChildDistribution = Seq(distribution, distribution),
      requiredChildOrdering = Seq(outputOrdering, outputOrdering)
    )
    val outputPlan = EnsureRequirements(sqlContext).apply(inputPlan)
    if (outputPlan.collect { case Exchange(_, _) => true }.nonEmpty) {
      fail(s"No Exchanges should have been added:\n$outputPlan")
    }
  }

  // ---------------------------------------------------------------------------------------------
}

// Used for unit-testing EnsureRequirements
private case class DummyPlan(
    override val children: Seq[SparkPlan] = Nil,
    override val outputOrdering: Seq[SortOrder] = Nil,
    override val outputPartitioning: Partitioning = UnknownPartitioning(0),
    override val requiresChildrenToProduceSameNumberOfPartitions: Boolean = false,
    override val requiredChildDistribution: Seq[Distribution] = Nil,
    override val requiredChildOrdering: Seq[Seq[SortOrder]] = Nil
  ) extends SparkPlan {
  override protected def doExecute(): RDD[InternalRow] = throw new NotImplementedError
  override def output: Seq[Attribute] = Seq.empty
}
