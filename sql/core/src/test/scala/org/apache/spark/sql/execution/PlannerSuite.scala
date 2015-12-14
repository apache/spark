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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{execution, Row, SQLConf}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, Literal, SortOrder}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.joins.{SortMergeJoin, BroadcastHashJoin}
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

  test("unions are collapsed") {
    val planner = sqlContext.planner
    import planner._
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
          """.stripMargin).queryExecution.executedPlan

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
        val planned = a.join(b, $"a.key" === $"b.key").queryExecution.executedPlan

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
        val exp = sql("select * from testPushed where key = 15").queryExecution.executedPlan
        assert(exp.toString.contains("PushedFilters: [EqualTo(key,15)]"))
      }
    }
  }

  test("efficient limit -> project -> sort") {
    {
      val query =
        testData.select('key, 'value).sort('key).limit(2).logicalPlan
      val planned = sqlContext.planner.TakeOrderedAndProject(query)
      assert(planned.head.isInstanceOf[execution.TakeOrderedAndProject])
      assert(planned.head.output === testData.select('key, 'value).logicalPlan.output)
    }

    {
      // We need to make sure TakeOrderedAndProject's output is correct when we push a project
      // into it.
      val query =
        testData.select('key, 'value).sort('key).select('value, 'key).limit(2).logicalPlan
      val planned = sqlContext.planner.TakeOrderedAndProject(query)
      assert(planned.head.isInstanceOf[execution.TakeOrderedAndProject])
      assert(planned.head.output === testData.select('value, 'key).logicalPlan.output)
    }
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

  // When it comes to testing whether EnsureRequirements properly ensures distribution requirements,
  // there two dimensions that need to be considered: are the child partitionings compatible and
  // do they satisfy the distribution requirements? As a result, we need at least four test cases.

  private def assertDistributionRequirementsAreSatisfied(outputPlan: SparkPlan): Unit = {
    if (outputPlan.children.length > 1
        && outputPlan.requiredChildDistribution.toSet != Set(UnspecifiedDistribution)) {
      val childPartitionings = outputPlan.children.map(_.outputPartitioning)
      if (!Partitioning.allCompatible(childPartitionings)) {
        fail(s"Partitionings are not compatible: $childPartitionings")
      }
    }
    outputPlan.children.zip(outputPlan.requiredChildDistribution).foreach {
      case (child, requiredDist) =>
        assert(child.outputPartitioning.satisfies(requiredDist),
          s"$child output partitioning does not satisfy $requiredDist:\n$outputPlan")
    }
  }

  test("EnsureRequirements with incompatible child partitionings which satisfy distribution") {
    // Consider an operator that requires inputs that are clustered by two expressions (e.g.
    // sort merge join where there are multiple columns in the equi-join condition)
    val clusteringA = Literal(1) :: Nil
    val clusteringB = Literal(2) :: Nil
    val distribution = ClusteredDistribution(clusteringA ++ clusteringB)
    // Say that the left and right inputs are each partitioned by _one_ of the two join columns:
    val leftPartitioning = HashPartitioning(clusteringA, 1)
    val rightPartitioning = HashPartitioning(clusteringB, 1)
    // Individually, each input's partitioning satisfies the clustering distribution:
    assert(leftPartitioning.satisfies(distribution))
    assert(rightPartitioning.satisfies(distribution))
    // However, these partitionings are not compatible with each other, so we still need to
    // repartition both inputs prior to performing the join:
    assert(!leftPartitioning.compatibleWith(rightPartitioning))
    assert(!rightPartitioning.compatibleWith(leftPartitioning))
    val inputPlan = DummySparkPlan(
      children = Seq(
        DummySparkPlan(outputPartitioning = leftPartitioning),
        DummySparkPlan(outputPartitioning = rightPartitioning)
      ),
      requiredChildDistribution = Seq(distribution, distribution),
      requiredChildOrdering = Seq(Seq.empty, Seq.empty)
    )
    val outputPlan = EnsureRequirements(sqlContext).apply(inputPlan)
    assertDistributionRequirementsAreSatisfied(outputPlan)
    if (outputPlan.collect { case e: Exchange => true }.isEmpty) {
      fail(s"Exchange should have been added:\n$outputPlan")
    }
  }

  test("EnsureRequirements with child partitionings with different numbers of output partitions") {
    // This is similar to the previous test, except it checks that partitionings are not compatible
    // unless they produce the same number of partitions.
    val clustering = Literal(1) :: Nil
    val distribution = ClusteredDistribution(clustering)
    val inputPlan = DummySparkPlan(
      children = Seq(
        DummySparkPlan(outputPartitioning = HashPartitioning(clustering, 1)),
        DummySparkPlan(outputPartitioning = HashPartitioning(clustering, 2))
      ),
      requiredChildDistribution = Seq(distribution, distribution),
      requiredChildOrdering = Seq(Seq.empty, Seq.empty)
    )
    val outputPlan = EnsureRequirements(sqlContext).apply(inputPlan)
    assertDistributionRequirementsAreSatisfied(outputPlan)
  }

  test("EnsureRequirements with compatible child partitionings that do not satisfy distribution") {
    val distribution = ClusteredDistribution(Literal(1) :: Nil)
    // The left and right inputs have compatible partitionings but they do not satisfy the
    // distribution because they are clustered on different columns. Thus, we need to shuffle.
    val childPartitioning = HashPartitioning(Literal(2) :: Nil, 1)
    assert(!childPartitioning.satisfies(distribution))
    val inputPlan = DummySparkPlan(
      children = Seq(
        DummySparkPlan(outputPartitioning = childPartitioning),
        DummySparkPlan(outputPartitioning = childPartitioning)
      ),
      requiredChildDistribution = Seq(distribution, distribution),
      requiredChildOrdering = Seq(Seq.empty, Seq.empty)
    )
    val outputPlan = EnsureRequirements(sqlContext).apply(inputPlan)
    assertDistributionRequirementsAreSatisfied(outputPlan)
    if (outputPlan.collect { case e: Exchange => true }.isEmpty) {
      fail(s"Exchange should have been added:\n$outputPlan")
    }
  }

  test("EnsureRequirements with compatible child partitionings that satisfy distribution") {
    // In this case, all requirements are satisfied and no exchange should be added.
    val distribution = ClusteredDistribution(Literal(1) :: Nil)
    val childPartitioning = HashPartitioning(Literal(1) :: Nil, 5)
    assert(childPartitioning.satisfies(distribution))
    val inputPlan = DummySparkPlan(
      children = Seq(
        DummySparkPlan(outputPartitioning = childPartitioning),
        DummySparkPlan(outputPartitioning = childPartitioning)
      ),
      requiredChildDistribution = Seq(distribution, distribution),
      requiredChildOrdering = Seq(Seq.empty, Seq.empty)
    )
    val outputPlan = EnsureRequirements(sqlContext).apply(inputPlan)
    assertDistributionRequirementsAreSatisfied(outputPlan)
    if (outputPlan.collect { case e: Exchange => true }.nonEmpty) {
      fail(s"Exchange should not have been added:\n$outputPlan")
    }
  }

  // This is a regression test for SPARK-9703
  test("EnsureRequirements should not repartition if only ordering requirement is unsatisfied") {
    // Consider an operator that imposes both output distribution and  ordering requirements on its
    // children, such as sort sort merge join. If the distribution requirements are satisfied but
    // the output ordering requirements are unsatisfied, then the planner should only add sorts and
    // should not need to add additional shuffles / exchanges.
    val outputOrdering = Seq(SortOrder(Literal(1), Ascending))
    val distribution = ClusteredDistribution(Literal(1) :: Nil)
    val inputPlan = DummySparkPlan(
      children = Seq(
        DummySparkPlan(outputPartitioning = SinglePartition),
        DummySparkPlan(outputPartitioning = SinglePartition)
      ),
      requiredChildDistribution = Seq(distribution, distribution),
      requiredChildOrdering = Seq(outputOrdering, outputOrdering)
    )
    val outputPlan = EnsureRequirements(sqlContext).apply(inputPlan)
    assertDistributionRequirementsAreSatisfied(outputPlan)
    if (outputPlan.collect { case e: Exchange => true }.nonEmpty) {
      fail(s"No Exchanges should have been added:\n$outputPlan")
    }
  }

  test("EnsureRequirements adds sort when there is no existing ordering") {
    val orderingA = SortOrder(Literal(1), Ascending)
    val orderingB = SortOrder(Literal(2), Ascending)
    assert(orderingA != orderingB)
    val inputPlan = DummySparkPlan(
      children = DummySparkPlan(outputOrdering = Seq.empty) :: Nil,
      requiredChildOrdering = Seq(Seq(orderingB)),
      requiredChildDistribution = Seq(UnspecifiedDistribution)
    )
    val outputPlan = EnsureRequirements(sqlContext).apply(inputPlan)
    assertDistributionRequirementsAreSatisfied(outputPlan)
    if (outputPlan.collect { case s: Sort => true }.isEmpty) {
      fail(s"Sort should have been added:\n$outputPlan")
    }
  }

  test("EnsureRequirements skips sort when required ordering is prefix of existing ordering") {
    val orderingA = SortOrder(Literal(1), Ascending)
    val orderingB = SortOrder(Literal(2), Ascending)
    assert(orderingA != orderingB)
    val inputPlan = DummySparkPlan(
      children = DummySparkPlan(outputOrdering = Seq(orderingA, orderingB)) :: Nil,
      requiredChildOrdering = Seq(Seq(orderingA)),
      requiredChildDistribution = Seq(UnspecifiedDistribution)
    )
    val outputPlan = EnsureRequirements(sqlContext).apply(inputPlan)
    assertDistributionRequirementsAreSatisfied(outputPlan)
    if (outputPlan.collect { case s: Sort => true }.nonEmpty) {
      fail(s"No sorts should have been added:\n$outputPlan")
    }
  }

  // This is a regression test for SPARK-11135
  test("EnsureRequirements adds sort when required ordering isn't a prefix of existing ordering") {
    val orderingA = SortOrder(Literal(1), Ascending)
    val orderingB = SortOrder(Literal(2), Ascending)
    assert(orderingA != orderingB)
    val inputPlan = DummySparkPlan(
      children = DummySparkPlan(outputOrdering = Seq(orderingA)) :: Nil,
      requiredChildOrdering = Seq(Seq(orderingA, orderingB)),
      requiredChildDistribution = Seq(UnspecifiedDistribution)
    )
    val outputPlan = EnsureRequirements(sqlContext).apply(inputPlan)
    assertDistributionRequirementsAreSatisfied(outputPlan)
    if (outputPlan.collect { case s: Sort => true }.isEmpty) {
      fail(s"Sort should have been added:\n$outputPlan")
    }
  }

  // ---------------------------------------------------------------------------------------------
}

// Used for unit-testing EnsureRequirements
private case class DummySparkPlan(
    override val children: Seq[SparkPlan] = Nil,
    override val outputOrdering: Seq[SortOrder] = Nil,
    override val outputPartitioning: Partitioning = UnknownPartitioning(0),
    override val requiredChildDistribution: Seq[Distribution] = Nil,
    override val requiredChildOrdering: Seq[Seq[SortOrder]] = Nil
  ) extends SparkPlan {
  override protected def doExecute(): RDD[InternalRow] = throw new NotImplementedError
  override def output: Seq[Attribute] = Seq.empty
}
