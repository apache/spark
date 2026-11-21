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

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{DynamicPruningExpression, Expression, PlanExpression}
import org.apache.spark.sql.catalyst.plans.logical.{EmptyRelation, LogicalPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, LogicalQueryStage, QueryStageExec}
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.metric.SQLMetricInfo
import org.apache.spark.sql.internal.SQLConf

/**
 * :: DeveloperApi ::
 * Stores information about a SQL SparkPlan.
 *
 * Shared DPP physical dependencies use `spark.sql.dpp.planInfo.version` and an event-local
 * `spark.sql.dpp.planInfo.id`. Later occurrences are `DPPSharedPlanReference`
 * leaves with the same version and `spark.sql.dpp.planInfo.ref`, but no children or metrics.
 * Readers unaware of references can display the leaves. The SQL UI restores edges for supported
 * versions and rejects unknown versions. Events without these keys retain their interpretation.
 */
@DeveloperApi
class SparkPlanInfo(
    val nodeName: String,
    val simpleString: String,
    val children: Seq[SparkPlanInfo],
    val metadata: Map[String, String],
    val metrics: Seq[SQLMetricInfo]) {

  override def hashCode(): Int = {
    // hashCode of simpleString should be good enough to distinguish the plans from each other
    // within a plan
    simpleString.hashCode
  }

  override def equals(other: Any): Boolean = other match {
    case o: SparkPlanInfo =>
      nodeName == o.nodeName && simpleString == o.simpleString && children == o.children
    case _ => false
  }
}

private[execution] object SparkPlanInfo {

  private def fromLogicalPlan(
      plan: LogicalPlan,
      context: DppPlanInfoCapture): SparkPlanInfo = {
    val childrenInfo = plan match {
      case LogicalQueryStage(_, physical) =>
        Seq(namespaceImported(fromSparkPlan(physical), context))
      case EmptyRelation(logical) => Seq(fromLogicalPlan(logical, context))
      case _ => (plan.children ++ plan.subqueries).map(fromLogicalPlan(_, context))
    }
    new SparkPlanInfo(
      plan.nodeName,
      plan.simpleString(SQLConf.get.maxToStringFields),
      childrenInfo,
      Map[String, String](),
      Seq.empty)
  }

  private val VersionKey = "spark.sql.dpp.planInfo.version"
  private val IdKey = "spark.sql.dpp.planInfo.id"
  private val RefKey = "spark.sql.dpp.planInfo.ref"

  // SparkPlan.equals is structural. The capture contract is deliberately object identity based.
  private final class Target(val plan: SparkPlan) {
    override def hashCode(): Int = System.identityHashCode(plan)

    override def equals(other: Any): Boolean = other match {
      case that: Target => plan eq that.plan
      case _ => false
    }
  }

  private final case class DescriptionEdge(target: Target, entersDpp: Boolean)

  private final class DppPlanInfoCapture {
    val edges = mutable.HashMap.empty[Target, Seq[DescriptionEdge]]
    val incoming = mutable.HashMap.empty[Target, Int]
    val dppTargets = mutable.HashSet.empty[Target]
    val ids = mutable.HashMap.empty[Target, String]
    var discoveredDppStates = 0
    var detailNodes = 0
    var referenceNodes = 0
    var traversedEdges = 0
    var embeddedSequence = 0

    def isShared(target: Target): Boolean =
      dppTargets.contains(target) && incoming.getOrElse(target, 0) > 1
  }

  private[execution] final case class DppPlanInfoStats(
      discoveredDppStates: Int,
      compressedTargets: Int,
      detailNodes: Int,
      referenceNodes: Int,
      traversedEdges: Int)

  def fromSparkPlan(plan: SparkPlan): SparkPlanInfo = {
    val context = discoverDppSharing(plan)
    capture(plan, context)
  }

  private[execution] def fromSparkPlanWithStats(
      plan: SparkPlan): (SparkPlanInfo, DppPlanInfoStats) = {
    val context = discoverDppSharing(plan)
    val info = capture(plan, context)
    (info, DppPlanInfoStats(context.discoveredDppStates, context.ids.size,
      context.detailNodes, context.referenceNodes, context.traversedEdges))
  }

  private def descriptionEdges(target: Target): Seq[DescriptionEdge] = {
    val plan = target.plan
    def edge(child: SparkPlan, dpp: Boolean = false): DescriptionEdge =
      DescriptionEdge(new Target(child), dpp)

    plan match {
      case ReusedExchangeExec(_, child) => Seq(edge(child))
      case ReusedSubqueryExec(child) => Seq(edge(child))
      case a: AdaptiveSparkPlanExec => Seq(edge(a.executedPlan))
      case stage: QueryStageExec => Seq(edge(stage.plan))
      case inMemory: InMemoryTableScanExec => Seq(edge(inMemory.relation.cachedPlan))
      case _: RDDScanExec | _: EmptyRelationExec => Seq.empty
      case _ =>
        // Follow the same pre-order expression occurrences as QueryPlan.subqueries, while
        // recording the actual DPP edge rather than classifying a subquery by its identity.
        val subqueries = mutable.ArrayBuffer.empty[DescriptionEdge]
        def visit(expression: Expression, inDpp: Boolean): Unit = {
          expression match {
            case _: DynamicPruningExpression =>
              expression.children.foreach(visit(_, inDpp = true))
            case in: InSubqueryExec =>
              subqueries += edge(in.plan, inDpp)
              expression.children.foreach(visit(_, inDpp))
            case subquery: PlanExpression[_] =>
              subqueries += edge(subquery.plan.asInstanceOf[SparkPlan])
              expression.children.foreach(visit(_, inDpp))
            case _ => expression.children.foreach(visit(_, inDpp))
          }
        }
        plan.expressions.foreach(visit(_, inDpp = false))
        plan.children.map(edge(_)) ++ subqueries
    }
  }

  private def discoverDppSharing(root: SparkPlan): DppPlanInfoCapture = {
    val context = new DppPlanInfoCapture
    val visited = mutable.HashSet.empty[(Target, Boolean)]
    val pending = mutable.Stack.empty[(Target, Boolean)]
    val rootTarget = new Target(root)
    context.incoming(rootTarget) = 1
    pending.push((rootTarget, false))
    while (pending.nonEmpty) {
      val (target, inDpp) = pending.pop()
      if (visited.add((target, inDpp))) {
        if (inDpp) {
          context.dppTargets += target
          context.discoveredDppStates += 1
        }
        val edges = context.edges.getOrElseUpdate(target, {
          val found = descriptionEdges(target)
          found.foreach { edge =>
            context.incoming(edge.target) = context.incoming.getOrElse(edge.target, 0) + 1
          }
          context.traversedEdges += found.size
          found
        })
        edges.reverseIterator.foreach { edge =>
          pending.push((edge.target, inDpp || edge.entersDpp))
        }
      }
    }
    context
  }

  private def capture(plan: SparkPlan, context: DppPlanInfoCapture): SparkPlanInfo = {
    val target = new Target(plan)
    val shared = context.isShared(target)
    if (shared && context.ids.contains(target)) {
      context.referenceNodes += 1
      val id = context.ids(target)
      return new SparkPlanInfo("DPPSharedPlanReference", s"DPP shared plan reference $id",
        Nil, Map(VersionKey -> "1", RefKey -> id), Nil)
    }

    val originalMetadata = plan match {
      case fileScan: FileSourceScanLike => fileScan.metadata
      case _ => Map.empty[String, String]
    }
    val metadata = if (shared) {
      require(!Seq(VersionKey, IdKey, RefKey).exists(originalMetadata.contains),
        "Internal error: DPP plan info metadata key conflict")
      val id = context.ids.size.toString
      // Publish the ID before descending, so a back edge is also a finite leaf reference.
      context.ids(target) = id
      originalMetadata ++ Map(VersionKey -> "1", IdKey -> id)
    } else {
      originalMetadata
    }
    val metrics = plan.metrics.toSeq.map { case (key, metric) =>
      new SQLMetricInfo(metric.name.getOrElse(key), metric.id, metric.metricType)
    }
    context.detailNodes += 1
    val children = plan match {
      case rddScan: RDDScanExec =>
        sparkPlanInfosFromRDD(rddScan.rdd).map(namespaceImported(_, context))
      case EmptyRelationExec(logical) => Seq(fromLogicalPlan(logical, context))
      case _ => context.edges(target).map(edge => capture(edge.target.plan, context))
    }
    new SparkPlanInfo(
      plan.nodeName, plan.simpleString(SQLConf.get.maxToStringFields), children, metadata, metrics)
  }

  // RDDScan and logical query stages can embed plans captured in another SQL execution.
  // Their reference IDs are local to that capture and must not collide in the enclosing event.
  private def namespaceImported(
      root: SparkPlanInfo,
      context: DppPlanInfoCapture): SparkPlanInfo = {
    val prefix = s"embedded-${context.embeddedSequence}-"
    context.embeddedSequence += 1
    def remap(info: SparkPlanInfo): SparkPlanInfo = {
      val metadata = info.metadata.map {
        case (key, value) if key == IdKey || key == RefKey => key -> (prefix + value)
        case entry => entry
      }
      val children = info.children.map(remap)
      if (metadata == info.metadata &&
          children.zip(info.children).forall { case (a, b) => a eq b }) {
        info
      } else {
        val simpleString = if (info.nodeName == "DPPSharedPlanReference") {
          s"DPP shared plan reference ${metadata(RefKey)}"
        } else {
          info.simpleString
        }
        new SparkPlanInfo(info.nodeName, simpleString, children, metadata, info.metrics)
      }
    }
    remap(root)
  }

  final lazy val EMPTY: SparkPlanInfo = new SparkPlanInfo("", "", Nil, Map.empty, Nil)

  private def sparkPlanInfosFromRDD(rdd: RDD[_]): Seq[SparkPlanInfo] = {
    // Walk only driver-side RDD dependency metadata. Dedupe by RDD id so shared lineage does not
    // duplicate the same internal SQL plan under a single RDDScanExec.
    val visitedRDDs = scala.collection.mutable.HashSet.empty[Int]
    val rddsToVisit = scala.collection.mutable.Queue.empty[RDD[_]]
    val planInfos = scala.collection.mutable.ArrayBuffer.empty[SparkPlanInfo]

    rddsToVisit.enqueue(rdd)
    while (rddsToVisit.nonEmpty) {
      val current = rddsToVisit.dequeue()
      if (visitedRDDs.add(current.id)) {
        current match {
          case sqlRDD: SQLExecutionRDD if sqlRDD.sparkPlanInfo != EMPTY =>
            planInfos += sqlRDD.sparkPlanInfo
          case _ =>
            try {
              current.dependencies.foreach(dep => rddsToVisit.enqueue(dep.rdd))
            } catch {
              case NonFatal(_) =>
            }
          }
      }
    }

    planInfos.toSeq
  }
}
