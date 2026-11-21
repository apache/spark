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

package org.apache.spark.sql.execution.ui

import scala.concurrent.duration._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.plans.physical.IdentityBroadcastMode
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, BroadcastQueryStageExec, LogicalQueryStage}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType
import org.apache.spark.util.JsonProtocol

class SparkPlanInfoSuite extends SharedSparkSession {

  import testImplicits._

  private def collectSparkPlanInfo(sparkPlanInfo: SparkPlanInfo): Seq[SparkPlanInfo] = {
    sparkPlanInfo +: sparkPlanInfo.children.flatMap(collectSparkPlanInfo)
  }

  private def findSparkPlanInfo(sparkPlanInfo: SparkPlanInfo, nodeName: String): SparkPlanInfo = {
    collectSparkPlanInfo(sparkPlanInfo)
      .find(_.nodeName == nodeName)
      .getOrElse(fail(s"Could not find $nodeName in ${sparkPlanInfo.simpleString}"))
  }

  private def collectSparkPlanGraphMetrics(
      df: DataFrame): (SparkPlanGraph, Map[Long, String]) = {
    val statusStore = spark.sharedState.statusStore
    spark.sparkContext.listenerBus.waitUntilEmpty(10000)
    val previousExecutionIds = statusStore.executionsList().map(_.executionId).toSet

    df.collect()
    spark.sparkContext.listenerBus.waitUntilEmpty(10000)

    eventually(timeout(10.seconds), interval(10.milliseconds)) {
      assert(statusStore.executionsList().map(_.executionId).toSet
        .diff(previousExecutionIds).size === 1)
    }
    val executionIds = statusStore.executionsList().map(_.executionId).toSet
      .diff(previousExecutionIds)
    val executionId = executionIds.head
    eventually(timeout(10.seconds), interval(10.milliseconds)) {
      assert(statusStore.execution(executionId).exists(_.metricValues != null))
    }

    (statusStore.planGraph(executionId), statusStore.executionMetrics(executionId))
  }

  def validateSparkPlanInfo(sparkPlanInfo: SparkPlanInfo): Unit = {
    sparkPlanInfo.nodeName match {
      case "InMemoryTableScan" => assert(sparkPlanInfo.children.length == 1)
      case _ => sparkPlanInfo.children.foreach(validateSparkPlanInfo)
    }
  }

  test("SparkPlanInfo creation from SparkPlan with InMemoryTableScan node") {
    val dfWithCache = Seq(
      (1, 1),
      (2, 2)
    ).toDF().filter("_1 > 1").cache().repartition(10)

    val planInfoResult = SparkPlanInfo.fromSparkPlan(dfWithCache.queryExecution.executedPlan)

    validateSparkPlanInfo(planInfoResult)
  }

  test("SPARK-47017: SparkPlanInfo and SQL UI include SQL plan inside RDDScanExec") {
    val source = spark.range(10).where($"id" > 3).select($"id".as("age"))
    val recreated = spark.createDataFrame(source.rdd, source.schema)

    val planInfo = SparkPlanInfo.fromSparkPlan(recreated.queryExecution.executedPlan)
    val rddScanInfo = findSparkPlanInfo(planInfo, "Scan ExistingRDD")
    val internalRDDPlanInfos = rddScanInfo.children.flatMap(collectSparkPlanInfo)
    val filterInfo = internalRDDPlanInfos
      .find(_.nodeName == "Filter")
      .getOrElse(fail(s"Could not find Filter under Scan ExistingRDD in ${planInfo.simpleString}"))

    assert(rddScanInfo.children.nonEmpty)
    assert(filterInfo.metrics.exists(_.name == "number of output rows"))

    val unionRDD = spark.sparkContext.union(source.rdd, source.rdd)
    val unionPlanInfo = SparkPlanInfo.fromSparkPlan(
      spark.createDataFrame(unionRDD, source.schema).queryExecution.executedPlan)
    val unionRDDScanInfo = findSparkPlanInfo(unionPlanInfo, "Scan ExistingRDD")

    assert(unionRDDScanInfo.children.size === 1)

    val nested = spark.createDataFrame(recreated.rdd, recreated.schema)
    val nestedPlanInfo = SparkPlanInfo.fromSparkPlan(nested.queryExecution.executedPlan)
    val nestedRDDScanInfo = findSparkPlanInfo(nestedPlanInfo, "Scan ExistingRDD")

    assert(nestedRDDScanInfo.children.size === 1)
    assert(collectSparkPlanInfo(nestedRDDScanInfo.children.head).exists(_.nodeName == "Filter"))

    val (planGraph, metricValues) = collectSparkPlanGraphMetrics(recreated)
    val filterNode = planGraph.allNodes
      .find(_.name == "Filter")
      .getOrElse(fail(s"Could not find Filter in ${recreated.queryExecution.executedPlan}"))
    val filterMetric = filterNode.metrics
      .find(_.name == "number of output rows")
      .getOrElse(fail("Could not find number of output rows metric for Filter"))
    val filterMetricValue = metricValues
      .getOrElse(filterMetric.accumulatorId, fail("Could not find Filter metric value"))
    val outputRows = "\\d+".r.findFirstIn(filterMetricValue.replace(",", ""))
      .map(_.toLong)
      .getOrElse(fail(s"Could not parse Filter metric value $filterMetricValue"))

    assert(outputRows === 6L)
  }

  private val versionKey = "spark.sql.dpp.planInfo.version"
  private val idKey = "spark.sql.dpp.planInfo.id"
  private val refKey = "spark.sql.dpp.planInfo.ref"

  // The fixture fails if capture accidentally prepares or executes any physical dependency.
  private case class DescriptionPlan(
      label: String,
      override val children: Seq[SparkPlan] = Nil,
      predicates: Seq[Expression] = Nil) extends SparkPlan {
    override val output: Seq[Attribute] = Seq(AttributeReference("key", LongType)())
    override def nodeName: String = label
    override def simpleString(maxFields: Int): String = label
    override lazy val metrics = Map("rows" -> SQLMetrics.createMetric(sparkContext, "rows"))
    override protected def doPrepare(): Unit = sys.error("description must not prepare")
    override protected def doExecute(): RDD[InternalRow] =
      sys.error("description must not execute")
    override protected def withNewChildrenInternal(
        newChildren: IndexedSeq[SparkPlan]): SparkPlan = copy(children = newChildren)
  }

  private def dpp(plan: BaseSubqueryExec): Expression =
    DynamicPruningExpression(InSubqueryExec(Literal(1L), plan, NamedExpression.newExprId))

  private def all(info: SparkPlanInfo): Seq[SparkPlanInfo] =
    Seq(info) ++ info.children.flatMap(all)

  private def roundTrip(info: SparkPlanInfo): SparkPlanInfo = {
    val event = SparkListenerSQLExecutionStart(17L, Some(17L), "DPP capture", "", "", info, 1L)
    val json = JsonProtocol.sparkEventToJsonString(event)
    JsonProtocol.sparkEventFromJson(json).asInstanceOf[SparkListenerSQLExecutionStart].sparkPlanInfo
  }

  private def assertReferences(info: SparkPlanInfo): Seq[SparkPlanInfo] = {
    val nodes = all(info)
    val details = nodes.filter(_.metadata.contains(idKey))
    val refs = nodes.filter(_.metadata.contains(refKey))
    assert(details.map(_.metadata(idKey)).distinct.size == details.size)
    refs.foreach { ref =>
      assert(ref.nodeName == "DPPSharedPlanReference")
      assert(ref.children.isEmpty)
      assert(ref.metrics.isEmpty)
      assert(ref.metadata.keySet == Set(versionKey, refKey))
      assert(ref.metadata(versionKey) == "1")
      assert(details.count(_.metadata(idKey) == ref.metadata(refKey)) == 1)
    }
    details.foreach { detail =>
      assert(detail.metadata(versionKey) == "1")
      assert(!detail.metadata.contains(refKey))
    }
    refs
  }

  test("SPARK-48486: embedded SQL plans have distinct event-local DPP references") {
    val innerLeaf = DescriptionPlan("inner-leaf")
    val innerSubquery = SubqueryExec("inner", innerLeaf)
    val inner = DescriptionPlan("inner-root",
      predicates = Seq(dpp(innerSubquery), dpp(innerSubquery)))
    val innerInfo = SparkPlanInfo.fromSparkPlan(inner)
    val rdd = new SQLExecutionRDD(
      spark.sparkContext.emptyRDD[InternalRow], spark.sessionState.conf, innerInfo)
    val scan = RDDScanExec(inner.output, rdd, "embedded")
    val stage = LogicalQueryStage(LocalRelation(inner.output), inner)
    val empty = EmptyRelationExec(stage)

    val outerLeaf = DescriptionPlan("outer-leaf")
    val outerSubquery = SubqueryExec("outer", outerLeaf)
    val outer = DescriptionPlan("outer-root", Seq(scan, empty),
      Seq(dpp(outerSubquery), dpp(outerSubquery)))
    val restored = roundTrip(SparkPlanInfo.fromSparkPlan(outer))
    assert(assertReferences(restored).size == 3)
    val graph = SparkPlanGraph(restored)
    val ids = graph.allNodes.map(_.id).toSet
    assert(graph.edges.forall(edge => ids(edge.fromId) && ids(edge.toId)))
  }

  test("SPARK-48486: main-tree-first DPP propagation and expression occurrences") {
    val producer = DescriptionPlan("main-first-producer")
    val shared = SubqueryExec("shared", producer)
    val root = DescriptionPlan("root", Seq(shared, producer), Seq(
      InSubqueryExec(Literal(1L), shared, NamedExpression.newExprId), dpp(shared)))
    val (info, stats) = SparkPlanInfo.fromSparkPlanWithStats(root)
    val restored = roundTrip(info)
    val refs = assertReferences(restored)
    assert(restored.children.size == 4)
    val sharedDetail = restored.children.head
    val producerDetail = sharedDetail.children.head
    assert(sharedDetail.metadata.contains(idKey))
    assert(producerDetail.metadata.contains(idKey))
    assert(restored.children(1).metadata(refKey) == producerDetail.metadata(idKey))
    assert(restored.children.drop(2).forall(
      _.metadata(refKey) == sharedDetail.metadata(idKey)))
    assert(refs.size == 3)
    assert(all(restored).size == 6)
    assert(stats.compressedTargets == 2)
    assert(stats.referenceNodes == 3)
    assert(stats.discoveredDppStates == 2)
    // The one outgoing producer edge is not counted twice for DPP/non-DPP states.
    assert(stats.traversedEdges == 5)
    assert(all(restored).flatMap(_.metrics).count(
      _.accumulatorId == producer.metrics("rows").id) == 1)
  }

  test("SPARK-48486: nested diamond JSON size and unique metrics") {
    for (depth <- Seq(2, 5, 9)) {
      var producer: SparkPlan = DescriptionPlan("diamond-leaf")
      for (level <- 0 until depth) {
        val shared = SubqueryExec(s"level-$level", producer)
        producer = DescriptionPlan(s"diamond-$level", predicates = Seq(dpp(shared), dpp(shared)))
      }
      val (info, stats) = SparkPlanInfo.fromSparkPlanWithStats(producer)
      val restored = roundTrip(info)
      val refs = assertReferences(restored)
      assert(refs.size == depth)
      assert(all(restored).size == 1 + 3 * depth)
      assert(stats.compressedTargets == depth)
      assert(stats.detailNodes == 1 + 2 * depth)
      assert(stats.referenceNodes == depth)
      val metrics = all(restored).flatMap(_.metrics).map(_.accumulatorId)
      assert(metrics.distinct.size == metrics.size)
    }
  }

  test("SPARK-48486: ordinary Scalar and IN preserve repeated descriptions") {
    val producer = DescriptionPlan("ordinary-producer")
    val shared = SubqueryExec("ordinary", producer)
    val root = DescriptionPlan("root", predicates = Seq(
      org.apache.spark.sql.execution.ScalarSubquery(shared, NamedExpression.newExprId),
      InSubqueryExec(Literal(1L), shared, NamedExpression.newExprId)))
    val (info, stats) = SparkPlanInfo.fromSparkPlanWithStats(root)
    val restored = roundTrip(info)
    assert(all(restored).forall(_.metadata.isEmpty))
    assert(restored.children.size == 2)
    assert(restored.children.head == restored.children(1))
    assert(all(restored).size == 5)
    assert(stats.compressedTargets == 0)
    assert(stats.referenceNodes == 0)
    assert(stats.discoveredDppStates == 0)
    assert(all(restored).flatMap(_.metrics).count(
      _.accumulatorId == producer.metrics("rows").id) == 2)
  }

  test("SPARK-48486: capture and object identity isolation") {
    val left = SubqueryExec("equal", DescriptionPlan("equal-producer"))
    val right = left.copy()
    val root = DescriptionPlan("root", predicates = Seq(dpp(left), dpp(right)))
    val first = roundTrip(SparkPlanInfo.fromSparkPlan(root))
    // Equal subquery wrappers are separate identities; only their shared physical child repeats.
    assert(!first.children.head.metadata.contains(idKey))
    assert(!first.children(1).metadata.contains(idKey))
    assert(assertReferences(first).size == 1)
    val second = roundTrip(SparkPlanInfo.fromSparkPlan(left))
    assert(all(second).forall(_.metadata.isEmpty))
    val again = roundTrip(SparkPlanInfo.fromSparkPlan(root))
    assert(first == again)
    assert(all(first).map(_.metadata) == all(again).map(_.metadata))
  }

  test("SPARK-48486: intermediate label bindings without logical build plan") {
    val producer = DescriptionPlan("physical-producer")
    val secret = "configuration-secret-must-not-leak"
    val logical = LocalRelation(AttributeReference(secret, LongType)())
    val keys = Seq(producer.output.head, Add(Literal(2L), Literal(3L)))
    val intermediate = SubqueryAdaptiveBroadcastExec(
      "binding-name", Seq(1), true, logical, keys, producer)(None)
    val root = DescriptionPlan("root", predicates = Seq(dpp(intermediate), dpp(intermediate)))
    withSQLConf("spark.test.dpp.secret" -> secret) {
      val restored = roundTrip(SparkPlanInfo.fromSparkPlan(root))
      val detail = restored.children.head
      Seq("binding-name", "1", "true", keys.head.toString, keys(1).toString).foreach { text =>
        assert(detail.simpleString.contains(text))
      }
      assert(!all(restored).exists(_.simpleString.contains(secret)))
      assert(assertReferences(restored).size == 1)
      assert(restored.children.last.simpleString ==
        s"DPP shared plan reference ${detail.metadata(idKey)}")
    }
  }

  test("SPARK-48486: ReusedExchange, ReusedSubquery and query stage edges") {
    val leaf = DescriptionPlan("wrapper-leaf")
    val exchange = BroadcastExchangeExec(IdentityBroadcastMode, leaf)
    val stage = BroadcastQueryStageExec(1, exchange, exchange)
    val reusedExchange = ReusedExchangeExec(exchange.output, exchange)
    val shared = SubqueryExec("wrapped", DescriptionPlan("branches", Seq(stage, reusedExchange)))
    val reusedSubquery = ReusedSubqueryExec(shared)
    val root = DescriptionPlan("root", predicates = Seq(dpp(reusedSubquery), dpp(shared)))
    val restored = roundTrip(SparkPlanInfo.fromSparkPlan(root))
    assert(assertReferences(restored).size == 2)
    val nodes = all(restored)
    Seq(stage.nodeName, reusedExchange.nodeName, reusedSubquery.nodeName).foreach { name =>
      val wrapper = nodes.find(_.nodeName == name).get
      assert(wrapper.children.size == 1)
    }
    assert(nodes.count(_.simpleString == "wrapper-leaf") == 1)
  }

  test("SPARK-48486: AQE executedPlan edge without initialPlan expansion") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      val adaptive = spark.range(2).repartition(2).queryExecution.executedPlan
        .asInstanceOf[AdaptiveSparkPlanExec]
      val shared = SubqueryExec("aqe", adaptive)
      val root = DescriptionPlan("root", predicates = Seq(dpp(shared), dpp(shared)))
      val restored = roundTrip(SparkPlanInfo.fromSparkPlan(root))
      assert(assertReferences(restored).size == 1)
      val adaptiveInfo = all(restored).find(_.nodeName == adaptive.nodeName).get
      assert(adaptiveInfo.children.size == 1)
      assert(adaptiveInfo.children.head.simpleString == adaptive.executedPlan.simpleString(
        spark.sessionState.conf.maxToStringFields))
      assert(!adaptive.isFinalPlan)
    }
  }

  test("SPARK-48486: reserved file scan metadata collisions fail explicitly") {
    withTempPath { path =>
      spark.range(1).write.parquet(path.getCanonicalPath)
      val source = spark.read.parquet(path.getCanonicalPath).queryExecution.executedPlan
        .collectFirst { case scan: FileSourceScanExec => scan }.get
      Seq(versionKey, idKey, refKey).foreach { key =>
        val colliding = new FileSourceScanExec(source.relation, source.stream, source.output,
            source.requiredSchema, source.partitionFilters, source.optionalBucketSet,
            source.optionalNumCoalescedBuckets, source.dataFilters, source.tableIdentifier,
            source.disableBucketedScan) {
          override lazy val metadata: Map[String, String] = Map(key -> "original-value")
        }
        val shared = SubqueryExec("collision", colliding)
        val root = DescriptionPlan("root", Seq(colliding), Seq(dpp(shared)))
        val error = intercept[IllegalArgumentException] {
          SparkPlanInfo.fromSparkPlan(root)
        }
        assert(error.getMessage.contains("DPP plan info metadata key conflict"))
        assert(colliding.metadata(key) == "original-value")
      }
    }
  }
}
