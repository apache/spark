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
import org.apache.spark.sql.{execution, DataFrame, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Range, Repartition, Sort, Union}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.columnar.{InMemoryRelation, InMemoryTableScanExec}
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, ReusedExchangeExec, ReuseExchange, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

class PlannerSuite extends SharedSQLContext {
  import testImplicits._

  setupTestData()

  private def testPartialAggregationPlan(query: LogicalPlan): Unit = {
    val planner = spark.sessionState.planner
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

  test("mixed aggregates with same distinct columns") {
    def assertNoExpand(plan: SparkPlan): Unit = {
      assert(plan.collect { case e: ExpandExec => e }.isEmpty)
    }

    withTempView("v") {
      Seq((1, 1.0, 1.0), (1, 2.0, 2.0)).toDF("i", "j", "k").createTempView("v")
      // one distinct column
      val query1 = sql("SELECT sum(DISTINCT j), max(DISTINCT j) FROM v GROUP BY i")
      assertNoExpand(query1.queryExecution.executedPlan)

      // 2 distinct columns
      val query2 = sql("SELECT corr(DISTINCT j, k), count(DISTINCT j, k) FROM v GROUP BY i")
      assertNoExpand(query2.queryExecution.executedPlan)

      // 2 distinct columns with different order
      val query3 = sql("SELECT corr(DISTINCT j, k), count(DISTINCT k, j) FROM v GROUP BY i")
      assertNoExpand(query3.queryExecution.executedPlan)
    }
  }

  test("sizeInBytes estimation of limit operator for broadcast hash join optimization") {
    def checkPlan(fieldTypes: Seq[DataType]): Unit = {
      withTempView("testLimit") {
        val fields = fieldTypes.zipWithIndex.map {
          case (dataType, index) => StructField(s"c${index}", dataType, true)
        } :+ StructField("key", IntegerType, true)
        val schema = StructType(fields)
        val row = Row.fromSeq(Seq.fill(fields.size)(null))
        val rowRDD = sparkContext.parallelize(row :: Nil)
        spark.createDataFrame(rowRDD, schema).createOrReplaceTempView("testLimit")

        val planned = sql(
          """
            |SELECT l.a, l.b
            |FROM testData2 l JOIN (SELECT * FROM testLimit LIMIT 1) r ON (l.a = r.key)
          """.stripMargin).queryExecution.sparkPlan

        val broadcastHashJoins = planned.collect { case join: BroadcastHashJoinExec => join }
        val sortMergeJoins = planned.collect { case join: SortMergeJoinExec => join }

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
      withTempView("tiny") {
        testData.limit(3).createOrReplaceTempView("tiny")
        sql("CACHE TABLE tiny")

        val a = testData.as("a")
        val b = spark.table("tiny").as("b")
        val planned = a.join(b, $"a.key" === $"b.key").queryExecution.sparkPlan

        val broadcastHashJoins = planned.collect { case join: BroadcastHashJoinExec => join }
        val sortMergeJoins = planned.collect { case join: SortMergeJoinExec => join }

        assert(broadcastHashJoins.size === 1, "Should use broadcast hash join")
        assert(sortMergeJoins.isEmpty, "Should not use shuffled hash join")

        spark.catalog.clearCache()
      }
    }
  }

  test("SPARK-11390 explain should print PushedFilters of PhysicalRDD") {
    withTempPath { file =>
      val path = file.getCanonicalPath
      testData.write.parquet(path)
      val df = spark.read.parquet(path)
      df.createOrReplaceTempView("testPushed")

      withTempView("testPushed") {
        val exp = sql("select * from testPushed where key = 15").queryExecution.sparkPlan
        assert(exp.toString.contains("PushedFilters: [IsNotNull(key), EqualTo(key,15)]"))
      }
    }
  }

  test("efficient terminal limit -> sort should use TakeOrderedAndProject") {
    val query = testData.select('key, 'value).sort('key).limit(2)
    val planned = query.queryExecution.executedPlan
    assert(planned.isInstanceOf[execution.TakeOrderedAndProjectExec])
    assert(planned.output === testData.select('key, 'value).logicalPlan.output)
  }

  test("terminal limit -> project -> sort should use TakeOrderedAndProject") {
    val query = testData.select('key, 'value).sort('key).select('value, 'key).limit(2)
    val planned = query.queryExecution.executedPlan
    assert(planned.isInstanceOf[execution.TakeOrderedAndProjectExec])
    assert(planned.output === testData.select('value, 'key).logicalPlan.output)
  }

  test("terminal limits that are not handled by TakeOrderedAndProject should use CollectLimit") {
    val query = testData.select('value).limit(2)
    val planned = query.queryExecution.sparkPlan
    assert(planned.isInstanceOf[CollectLimitExec])
    assert(planned.output === testData.select('value).logicalPlan.output)
  }

  test("TakeOrderedAndProject can appear in the middle of plans") {
    val query = testData.select('key, 'value).sort('key).limit(2).filter('key === 3)
    val planned = query.queryExecution.executedPlan
    assert(planned.find(_.isInstanceOf[TakeOrderedAndProjectExec]).isDefined)
  }

  test("CollectLimit can appear in the middle of a plan when caching is used") {
    val query = testData.select('key, 'value).limit(2).cache()
    val planned = query.queryExecution.optimizedPlan.asInstanceOf[InMemoryRelation]
    assert(planned.cachedPlan.isInstanceOf[CollectLimitExec])
  }

  test("TakeOrderedAndProjectExec appears only when number of limit is below the threshold.") {
    withSQLConf(SQLConf.TOP_K_SORT_FALLBACK_THRESHOLD.key -> "1000") {
      val query0 = testData.select('value).orderBy('key).limit(100)
      val planned0 = query0.queryExecution.executedPlan
      assert(planned0.find(_.isInstanceOf[TakeOrderedAndProjectExec]).isDefined)

      val query1 = testData.select('value).orderBy('key).limit(2000)
      val planned1 = query1.queryExecution.executedPlan
      assert(planned1.find(_.isInstanceOf[TakeOrderedAndProjectExec]).isEmpty)
    }
  }

  test("PartitioningCollection") {
    withTempView("normal", "small", "tiny") {
      testData.createOrReplaceTempView("normal")
      testData.limit(10).createOrReplaceTempView("small")
      testData.limit(3).createOrReplaceTempView("tiny")

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
            case exchange: ShuffleExchangeExec => exchange
          }.length
          assert(numExchanges === 5)
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
            case exchange: ShuffleExchangeExec => exchange
          }.length
          assert(numExchanges === 5)
        }

      }
    }
  }

  test("collapse adjacent repartitions") {
    val doubleRepartitioned = testData.repartition(10).repartition(20).coalesce(5)
    def countRepartitions(plan: LogicalPlan): Int = plan.collect { case r: Repartition => r }.length
    assert(countRepartitions(doubleRepartitioned.queryExecution.analyzed) === 3)
    assert(countRepartitions(doubleRepartitioned.queryExecution.optimizedPlan) === 2)
    doubleRepartitioned.queryExecution.optimizedPlan match {
      case Repartition (numPartitions, shuffle, Repartition(_, shuffleChild, _)) =>
        assert(numPartitions === 5)
        assert(shuffle === false)
        assert(shuffleChild === true)
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Unit tests of EnsureRequirements for Exchange
  ///////////////////////////////////////////////////////////////////////////

  // When it comes to testing whether EnsureRequirements properly ensures distribution requirements,
  // there two dimensions that need to be considered: are the child partitionings compatible and
  // do they satisfy the distribution requirements? As a result, we need at least four test cases.

  private def assertDistributionRequirementsAreSatisfied(outputPlan: SparkPlan): Unit = {
    if (outputPlan.children.length > 1) {
      val childPartitionings = outputPlan.children.zip(outputPlan.requiredChildDistribution)
        .filter {
          case (_, UnspecifiedDistribution) => false
          case (_, _: BroadcastDistribution) => false
          case _ => true
        }.map(_._1.outputPartitioning)

      if (childPartitionings.map(_.numPartitions).toSet.size > 1) {
        fail(s"Partitionings doesn't have same number of partitions: $childPartitionings")
      }
    }
    outputPlan.children.zip(outputPlan.requiredChildDistribution).foreach {
      case (child, requiredDist) =>
        assert(child.outputPartitioning.satisfies(requiredDist),
          s"$child output partitioning does not satisfy $requiredDist:\n$outputPlan")
    }
  }

  test("EnsureRequirements with child partitionings with different numbers of output partitions") {
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
    val outputPlan = EnsureRequirements(spark.sessionState.conf).apply(inputPlan)
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
    val outputPlan = EnsureRequirements(spark.sessionState.conf).apply(inputPlan)
    assertDistributionRequirementsAreSatisfied(outputPlan)
    if (outputPlan.collect { case e: ShuffleExchangeExec => true }.isEmpty) {
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
    val outputPlan = EnsureRequirements(spark.sessionState.conf).apply(inputPlan)
    assertDistributionRequirementsAreSatisfied(outputPlan)
    if (outputPlan.collect { case e: ShuffleExchangeExec => true }.nonEmpty) {
      fail(s"Exchange should not have been added:\n$outputPlan")
    }
  }

  // This is a regression test for SPARK-9703
  test("EnsureRequirements should not repartition if only ordering requirement is unsatisfied") {
    // Consider an operator that imposes both output distribution and  ordering requirements on its
    // children, such as sort merge join. If the distribution requirements are satisfied but
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
    val outputPlan = EnsureRequirements(spark.sessionState.conf).apply(inputPlan)
    assertDistributionRequirementsAreSatisfied(outputPlan)
    if (outputPlan.collect { case e: ShuffleExchangeExec => true }.nonEmpty) {
      fail(s"No Exchanges should have been added:\n$outputPlan")
    }
  }

  test("EnsureRequirements eliminates Exchange if child has same partitioning") {
    val distribution = ClusteredDistribution(Literal(1) :: Nil)
    val partitioning = HashPartitioning(Literal(1) :: Nil, 5)
    assert(partitioning.satisfies(distribution))

    val inputPlan = ShuffleExchangeExec(
      partitioning,
      DummySparkPlan(outputPartitioning = partitioning),
      None)
    val outputPlan = EnsureRequirements(spark.sessionState.conf).apply(inputPlan)
    assertDistributionRequirementsAreSatisfied(outputPlan)
    if (outputPlan.collect { case e: ShuffleExchangeExec => true }.size == 2) {
      fail(s"Topmost Exchange should have been eliminated:\n$outputPlan")
    }
  }

  test("EnsureRequirements does not eliminate Exchange with different partitioning") {
    val distribution = ClusteredDistribution(Literal(1) :: Nil)
    val partitioning = HashPartitioning(Literal(2) :: Nil, 5)
    assert(!partitioning.satisfies(distribution))

    val inputPlan = ShuffleExchangeExec(
      partitioning,
      DummySparkPlan(outputPartitioning = partitioning),
      None)
    val outputPlan = EnsureRequirements(spark.sessionState.conf).apply(inputPlan)
    assertDistributionRequirementsAreSatisfied(outputPlan)
    if (outputPlan.collect { case e: ShuffleExchangeExec => true }.size == 1) {
      fail(s"Topmost Exchange should not have been eliminated:\n$outputPlan")
    }
  }

  test("EnsureRequirements should respect ClusteredDistribution's num partitioning") {
    val distribution = ClusteredDistribution(Literal(1) :: Nil, Some(13))
    // Number of partitions differ
    val finalPartitioning = HashPartitioning(Literal(1) :: Nil, 13)
    val childPartitioning = HashPartitioning(Literal(1) :: Nil, 5)
    assert(!childPartitioning.satisfies(distribution))
    val inputPlan = DummySparkPlan(
        children = DummySparkPlan(outputPartitioning = childPartitioning) :: Nil,
        requiredChildDistribution = Seq(distribution),
        requiredChildOrdering = Seq(Seq.empty))

    val outputPlan = EnsureRequirements(spark.sessionState.conf).apply(inputPlan)
    val shuffle = outputPlan.collect { case e: ShuffleExchangeExec => e }
    assert(shuffle.size === 1)
    assert(shuffle.head.newPartitioning === finalPartitioning)
  }

  test("Reuse exchanges") {
    val distribution = ClusteredDistribution(Literal(1) :: Nil)
    val finalPartitioning = HashPartitioning(Literal(1) :: Nil, 5)
    val childPartitioning = HashPartitioning(Literal(2) :: Nil, 5)
    assert(!childPartitioning.satisfies(distribution))
    val shuffle = ShuffleExchangeExec(finalPartitioning,
      DummySparkPlan(
        children = DummySparkPlan(outputPartitioning = childPartitioning) :: Nil,
        requiredChildDistribution = Seq(distribution),
        requiredChildOrdering = Seq(Seq.empty)),
      None)

    val inputPlan = SortMergeJoinExec(
      Literal(1) :: Nil,
      Literal(1) :: Nil,
      Inner,
      None,
      shuffle,
      shuffle)

    val outputPlan = ReuseExchange(spark.sessionState.conf).apply(inputPlan)
    if (outputPlan.collect { case e: ReusedExchangeExec => true }.size != 1) {
      fail(s"Should re-use the shuffle:\n$outputPlan")
    }
    if (outputPlan.collect { case e: ShuffleExchangeExec => true }.size != 1) {
      fail(s"Should have only one shuffle:\n$outputPlan")
    }

    // nested exchanges
    val inputPlan2 = SortMergeJoinExec(
      Literal(1) :: Nil,
      Literal(1) :: Nil,
      Inner,
      None,
      ShuffleExchangeExec(finalPartitioning, inputPlan),
      ShuffleExchangeExec(finalPartitioning, inputPlan))

    val outputPlan2 = ReuseExchange(spark.sessionState.conf).apply(inputPlan2)
    if (outputPlan2.collect { case e: ReusedExchangeExec => true }.size != 2) {
      fail(s"Should re-use the two shuffles:\n$outputPlan2")
    }
    if (outputPlan2.collect { case e: ShuffleExchangeExec => true }.size != 2) {
      fail(s"Should have only two shuffles:\n$outputPlan")
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Unit tests of EnsureRequirements for Sort
  ///////////////////////////////////////////////////////////////////////////

  private val exprA = Literal(1)
  private val exprB = Literal(2)
  private val exprC = Literal(3)
  private val orderingA = SortOrder(exprA, Ascending)
  private val orderingB = SortOrder(exprB, Ascending)
  private val orderingC = SortOrder(exprC, Ascending)
  private val planA = DummySparkPlan(outputOrdering = Seq(orderingA),
    outputPartitioning = HashPartitioning(exprA :: Nil, 5))
  private val planB = DummySparkPlan(outputOrdering = Seq(orderingB),
    outputPartitioning = HashPartitioning(exprB :: Nil, 5))
  private val planC = DummySparkPlan(outputOrdering = Seq(orderingC),
    outputPartitioning = HashPartitioning(exprC :: Nil, 5))

  assert(orderingA != orderingB && orderingA != orderingC && orderingB != orderingC)

  private def assertSortRequirementsAreSatisfied(
      childPlan: SparkPlan,
      requiredOrdering: Seq[SortOrder],
      shouldHaveSort: Boolean): Unit = {
    val inputPlan = DummySparkPlan(
      children = childPlan :: Nil,
      requiredChildOrdering = Seq(requiredOrdering),
      requiredChildDistribution = Seq(UnspecifiedDistribution)
    )
    val outputPlan = EnsureRequirements(spark.sessionState.conf).apply(inputPlan)
    assertDistributionRequirementsAreSatisfied(outputPlan)
    if (shouldHaveSort) {
      if (outputPlan.collect { case s: SortExec => true }.isEmpty) {
        fail(s"Sort should have been added:\n$outputPlan")
      }
    } else {
      if (outputPlan.collect { case s: SortExec => true }.nonEmpty) {
        fail(s"No sorts should have been added:\n$outputPlan")
      }
    }
  }

  test("EnsureRequirements skips sort when either side of join keys is required after inner SMJ") {
    Seq(Inner, Cross).foreach { joinType =>
      val innerSmj = SortMergeJoinExec(exprA :: Nil, exprB :: Nil, joinType, None, planA, planB)
      // Both left and right keys should be sorted after the SMJ.
      Seq(orderingA, orderingB).foreach { ordering =>
        assertSortRequirementsAreSatisfied(
          childPlan = innerSmj,
          requiredOrdering = Seq(ordering),
          shouldHaveSort = false)
      }
    }
  }

  test("EnsureRequirements skips sort when key order of a parent SMJ is propagated from its " +
    "child SMJ") {
    Seq(Inner, Cross).foreach { joinType =>
      val childSmj = SortMergeJoinExec(exprA :: Nil, exprB :: Nil, joinType, None, planA, planB)
      val parentSmj = SortMergeJoinExec(exprB :: Nil, exprC :: Nil, joinType, None, childSmj, planC)
      // After the second SMJ, exprA, exprB and exprC should all be sorted.
      Seq(orderingA, orderingB, orderingC).foreach { ordering =>
        assertSortRequirementsAreSatisfied(
          childPlan = parentSmj,
          requiredOrdering = Seq(ordering),
          shouldHaveSort = false)
      }
    }
  }

  test("EnsureRequirements for sort operator after left outer sort merge join") {
    // Only left key is sorted after left outer SMJ (thus doesn't need a sort).
    val leftSmj = SortMergeJoinExec(exprA :: Nil, exprB :: Nil, LeftOuter, None, planA, planB)
    Seq((orderingA, false), (orderingB, true)).foreach { case (ordering, needSort) =>
      assertSortRequirementsAreSatisfied(
        childPlan = leftSmj,
        requiredOrdering = Seq(ordering),
        shouldHaveSort = needSort)
    }
  }

  test("EnsureRequirements for sort operator after right outer sort merge join") {
    // Only right key is sorted after right outer SMJ (thus doesn't need a sort).
    val rightSmj = SortMergeJoinExec(exprA :: Nil, exprB :: Nil, RightOuter, None, planA, planB)
    Seq((orderingA, true), (orderingB, false)).foreach { case (ordering, needSort) =>
      assertSortRequirementsAreSatisfied(
        childPlan = rightSmj,
        requiredOrdering = Seq(ordering),
        shouldHaveSort = needSort)
    }
  }

  test("EnsureRequirements adds sort after full outer sort merge join") {
    // Neither keys is sorted after full outer SMJ, so they both need sorts.
    val fullSmj = SortMergeJoinExec(exprA :: Nil, exprB :: Nil, FullOuter, None, planA, planB)
    Seq(orderingA, orderingB).foreach { ordering =>
      assertSortRequirementsAreSatisfied(
        childPlan = fullSmj,
        requiredOrdering = Seq(ordering),
        shouldHaveSort = true)
    }
  }

  test("EnsureRequirements adds sort when there is no existing ordering") {
    assertSortRequirementsAreSatisfied(
      childPlan = DummySparkPlan(outputOrdering = Seq.empty),
      requiredOrdering = Seq(orderingB),
      shouldHaveSort = true)
  }

  test("EnsureRequirements skips sort when required ordering is prefix of existing ordering") {
    assertSortRequirementsAreSatisfied(
      childPlan = DummySparkPlan(outputOrdering = Seq(orderingA, orderingB)),
      requiredOrdering = Seq(orderingA),
      shouldHaveSort = false)
  }

  test("EnsureRequirements skips sort when required ordering is semantically equal to " +
    "existing ordering") {
    val exprId: ExprId = NamedExpression.newExprId
    val attribute1 =
      AttributeReference(
        name = "col1",
        dataType = LongType,
        nullable = false
      ) (exprId = exprId,
        qualifier = Seq("col1_qualifier")
      )

    val attribute2 =
      AttributeReference(
        name = "col1",
        dataType = LongType,
        nullable = false
      ) (exprId = exprId)

    val orderingA1 = SortOrder(attribute1, Ascending)
    val orderingA2 = SortOrder(attribute2, Ascending)

    assert(orderingA1 != orderingA2, s"$orderingA1 should NOT equal to $orderingA2")
    assert(orderingA1.semanticEquals(orderingA2),
      s"$orderingA1 should be semantically equal to $orderingA2")

    assertSortRequirementsAreSatisfied(
      childPlan = DummySparkPlan(outputOrdering = Seq(orderingA1)),
      requiredOrdering = Seq(orderingA2),
      shouldHaveSort = false)
  }

  // This is a regression test for SPARK-11135
  test("EnsureRequirements adds sort when required ordering isn't a prefix of existing ordering") {
    assertSortRequirementsAreSatisfied(
      childPlan = DummySparkPlan(outputOrdering = Seq(orderingA)),
      requiredOrdering = Seq(orderingA, orderingB),
      shouldHaveSort = true)
  }

  test("SPARK-24242: RangeExec should have correct output ordering and partitioning") {
    val df = spark.range(10)
    val rangeExec = df.queryExecution.executedPlan.collect {
      case r: RangeExec => r
    }
    val range = df.queryExecution.optimizedPlan.collect {
      case r: Range => r
    }
    assert(rangeExec.head.outputOrdering == range.head.outputOrdering)
    assert(rangeExec.head.outputPartitioning ==
      RangePartitioning(rangeExec.head.outputOrdering, df.rdd.getNumPartitions))

    val rangeInOnePartition = spark.range(1, 10, 1, 1)
    val rangeExecInOnePartition = rangeInOnePartition.queryExecution.executedPlan.collect {
      case r: RangeExec => r
    }
    assert(rangeExecInOnePartition.head.outputPartitioning == SinglePartition)

    val rangeInZeroPartition = spark.range(-10, -9, -20, 1)
    val rangeExecInZeroPartition = rangeInZeroPartition.queryExecution.executedPlan.collect {
      case r: RangeExec => r
    }
    assert(rangeExecInZeroPartition.head.outputPartitioning == UnknownPartitioning(0))
  }

  test("SPARK-24495: EnsureRequirements can return wrong plan when reusing the same key in join") {
    val plan1 = DummySparkPlan(outputOrdering = Seq(orderingA),
      outputPartitioning = HashPartitioning(exprA :: exprA :: Nil, 5))
    val plan2 = DummySparkPlan(outputOrdering = Seq(orderingB),
      outputPartitioning = HashPartitioning(exprB :: Nil, 5))
    val smjExec = SortMergeJoinExec(
      exprA :: exprA :: Nil, exprB :: exprC :: Nil, Inner, None, plan1, plan2)

    val outputPlan = EnsureRequirements(spark.sessionState.conf).apply(smjExec)
    outputPlan match {
      case SortMergeJoinExec(leftKeys, rightKeys, _, _, _, _) =>
        assert(leftKeys == Seq(exprA, exprA))
        assert(rightKeys == Seq(exprB, exprC))
      case _ => fail()
    }
  }

  test("SPARK-27485: EnsureRequirements.reorder should handle duplicate expressions") {
    val plan1 = DummySparkPlan(
      outputPartitioning = HashPartitioning(exprA :: exprB :: exprA :: Nil, 5))
    val plan2 = DummySparkPlan()
    val smjExec = SortMergeJoinExec(
      leftKeys = exprA :: exprB :: exprB :: Nil,
      rightKeys = exprA :: exprC :: exprC :: Nil,
      joinType = Inner,
      condition = None,
      left = plan1,
      right = plan2)
    val outputPlan = EnsureRequirements(spark.sessionState.conf).apply(smjExec)
    outputPlan match {
      case SortMergeJoinExec(leftKeys, rightKeys, _, _,
             SortExec(_, _,
               ShuffleExchangeExec(HashPartitioning(leftPartitioningExpressions, _), _, _), _),
             SortExec(_, _,
               ShuffleExchangeExec(HashPartitioning(rightPartitioningExpressions, _), _, _), _)) =>
        assert(leftKeys === smjExec.leftKeys)
        assert(rightKeys === smjExec.rightKeys)
        assert(leftKeys === leftPartitioningExpressions)
        assert(rightKeys === rightPartitioningExpressions)
      case _ => fail(outputPlan.toString)
    }
  }

  test("SPARK-24500: create union with stream of children") {
    val df = Union(Stream(
      Range(1, 1, 1, 1),
      Range(1, 2, 1, 1)))
    df.queryExecution.executedPlan.execute()
  }

  test("SPARK-25278: physical nodes should be different instances for same logical nodes") {
    val range = Range(1, 1, 1, 1)
    val df = Union(range, range)
    val ranges = df.queryExecution.optimizedPlan.collect {
      case r: Range => r
    }
    assert(ranges.length == 2)
    // Ensure the two Range instances are equal according to their equal method
    assert(ranges.head == ranges.last)
    val execRanges = df.queryExecution.sparkPlan.collect {
      case r: RangeExec => r
    }
    assert(execRanges.length == 2)
    // Ensure the two RangeExec instances are different instances
    assert(!execRanges.head.eq(execRanges.last))
  }

  test("SPARK-24556: always rewrite output partitioning in ReusedExchangeExec " +
    "and InMemoryTableScanExec") {
    def checkOutputPartitioningRewrite(
        plans: Seq[SparkPlan],
        expectedPartitioningClass: Class[_]): Unit = {
      assert(plans.size == 1)
      val plan = plans.head
      val partitioning = plan.outputPartitioning
      assert(partitioning.getClass == expectedPartitioningClass)
      val partitionedAttrs = partitioning.asInstanceOf[Expression].references
      assert(partitionedAttrs.subsetOf(plan.outputSet))
    }

    def checkReusedExchangeOutputPartitioningRewrite(
        df: DataFrame,
        expectedPartitioningClass: Class[_]): Unit = {
      val reusedExchange = df.queryExecution.executedPlan.collect {
        case r: ReusedExchangeExec => r
      }
      checkOutputPartitioningRewrite(reusedExchange, expectedPartitioningClass)
    }

    def checkInMemoryTableScanOutputPartitioningRewrite(
        df: DataFrame,
        expectedPartitioningClass: Class[_]): Unit = {
      val inMemoryScan = df.queryExecution.executedPlan.collect {
        case m: InMemoryTableScanExec => m
      }
      checkOutputPartitioningRewrite(inMemoryScan, expectedPartitioningClass)
    }

    // ReusedExchange is HashPartitioning
    val df1 = Seq(1 -> "a").toDF("i", "j").repartition($"i")
    val df2 = Seq(1 -> "a").toDF("i", "j").repartition($"i")
    checkReusedExchangeOutputPartitioningRewrite(df1.union(df2), classOf[HashPartitioning])

    // ReusedExchange is RangePartitioning
    val df3 = Seq(1 -> "a").toDF("i", "j").orderBy($"i")
    val df4 = Seq(1 -> "a").toDF("i", "j").orderBy($"i")
    checkReusedExchangeOutputPartitioningRewrite(df3.union(df4), classOf[RangePartitioning])

    // InMemoryTableScan is HashPartitioning
    Seq(1 -> "a").toDF("i", "j").repartition($"i").persist()
    checkInMemoryTableScanOutputPartitioningRewrite(
      Seq(1 -> "a").toDF("i", "j").repartition($"i"), classOf[HashPartitioning])

    // InMemoryTableScan is RangePartitioning
    spark.range(1, 100, 1, 10).toDF().persist()
    checkInMemoryTableScanOutputPartitioningRewrite(
      spark.range(1, 100, 1, 10).toDF(), classOf[RangePartitioning])

    // InMemoryTableScan is PartitioningCollection
    withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      Seq(1 -> "a").toDF("i", "j").join(Seq(1 -> "a").toDF("m", "n"), $"i" === $"m").persist()
      checkInMemoryTableScanOutputPartitioningRewrite(
        Seq(1 -> "a").toDF("i", "j").join(Seq(1 -> "a").toDF("m", "n"), $"i" === $"m"),
        classOf[PartitioningCollection])
    }
  }

  test("SPARK-26812: wrong nullability for complex datatypes in union") {
    def testUnionOutputType(input1: DataType, input2: DataType, output: DataType): Unit = {
      val query = Union(
        LocalRelation(StructField("a", input1)), LocalRelation(StructField("a", input2)))
      assert(query.output.head.dataType == output)
    }

    // Map
    testUnionOutputType(
      MapType(StringType, StringType, valueContainsNull = false),
      MapType(StringType, StringType, valueContainsNull = true),
      MapType(StringType, StringType, valueContainsNull = true))
    testUnionOutputType(
      MapType(StringType, StringType, valueContainsNull = true),
      MapType(StringType, StringType, valueContainsNull = false),
      MapType(StringType, StringType, valueContainsNull = true))
    testUnionOutputType(
      MapType(StringType, StringType, valueContainsNull = false),
      MapType(StringType, StringType, valueContainsNull = false),
      MapType(StringType, StringType, valueContainsNull = false))

    // Array
    testUnionOutputType(
      ArrayType(StringType, containsNull = false),
      ArrayType(StringType, containsNull = true),
      ArrayType(StringType, containsNull = true))
    testUnionOutputType(
      ArrayType(StringType, containsNull = true),
      ArrayType(StringType, containsNull = false),
      ArrayType(StringType, containsNull = true))
    testUnionOutputType(
      ArrayType(StringType, containsNull = false),
      ArrayType(StringType, containsNull = false),
      ArrayType(StringType, containsNull = false))

    // Struct
    testUnionOutputType(
      StructType(Seq(
        StructField("f1", StringType, nullable = false),
        StructField("f2", StringType, nullable = true),
        StructField("f3", StringType, nullable = false))),
      StructType(Seq(
        StructField("f1", StringType, nullable = true),
        StructField("f2", StringType, nullable = false),
        StructField("f3", StringType, nullable = false))),
      StructType(Seq(
        StructField("f1", StringType, nullable = true),
        StructField("f2", StringType, nullable = true),
        StructField("f3", StringType, nullable = false))))
  }
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
