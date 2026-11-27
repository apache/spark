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

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import org.apache.commons.text.StringEscapeUtils

import org.apache.spark.sql.execution.{SparkPlanInfo, WholeStageCodegenExec}


/**
 * A graph used for storing information of an executionPlan of DataFrame.
 *
 * Each graph is defined with a set of nodes and a set of edges. Each node represents a node in the
 * SparkPlan tree, and each edge represents a parent-child relationship between two nodes.
 */
case class SparkPlanGraph(
    nodes: collection.Seq[SparkPlanGraphNode],
    edges: collection.Seq[SparkPlanGraphEdge]) {

  def makeDotFile(metrics: Map[Long, String]): String = {
    val dotFile = new StringBuilder
    dotFile.append("digraph G {\n")
    nodes.foreach(node => dotFile.append(node.makeDotNode(metrics) + "\n"))
    val nodeMap = allNodes.map(n => n.id -> n).toMap
    edges.foreach(edge => dotFile.append(edge.makeDotEdge(nodeMap, metrics) + "\n"))
    dotFile.append("}")
    dotFile.toString()
  }

  /**
   * Generate a JSON string containing node details (name, metrics, and optional children)
   * for the detail side panel. This keeps the DOT node labels compact (name only) while
   * providing full metrics on demand via JavaScript.
   */
  def makeNodeDetailsJson(metrics: Map[Long, String]): String = {
    val entries = allNodes.map { node =>
      val metricsJson = node.metrics.flatMap { m =>
        metrics.get(m.accumulatorId).map { v =>
          val n = StringEscapeUtils.escapeJson(m.name)
          val mv = StringEscapeUtils.escapeJson(v)
          val mt = StringEscapeUtils.escapeJson(m.metricType)
          s"""{"name":"$n","value":"$mv","type":"$mt"}"""
        }
      }.mkString("[", ",", "]")
      val (prefix, extra) = node match {
        case cluster: SparkPlanGraphCluster =>
          val childIds = cluster.nodes
            .map(n => s""""node${n.id}"""").mkString("[", ",", "]")
          ("cluster", s""","children":$childIds""")
        case _ => ("node", "")
      }
      s"""  "$prefix${node.id}":{"name":"${StringEscapeUtils.escapeJson(node.name)}",""" +
        s""""desc":"${StringEscapeUtils.escapeJson(node.desc)}",""" +
        s""""metrics":$metricsJson$extra}"""
    }
    entries.mkString("{\n", ",\n", "\n}")
  }

  /**
   * All the SparkPlanGraphNodes, including those inside of WholeStageCodegen.
   */
  val allNodes: collection.Seq[SparkPlanGraphNode] = {
    nodes.flatMap {
      case cluster: SparkPlanGraphCluster => cluster.nodes :+ cluster
      case node => Seq(node)
    }
  }
}

object SparkPlanGraph {

  /** Maximum character length for DOT graph node labels. */
  private val MAX_LABEL_LENGTH = 36

  /** Pattern to detect dot-separated qualified names (e.g., catalog.schema.table). */
  private val QUALIFIED_NAME_PATTERN =
    java.util.regex.Pattern.compile(".*\\w+\\.\\w+\\.\\w+.*")

  /**
   * Truncate a node label for compact display in the plan visualization graph.
   * Only truncates labels containing dot-separated qualified names (e.g.,
   * catalog.schema.table) which can become very long with custom catalogs.
   * Uses middle-ellipsis to preserve the operator prefix and the table name.
   * The full name is always available in the tooltip and side panel.
   */
  private[ui] def truncateLabel(label: String): String = {
    if (label == null || label.length <= MAX_LABEL_LENGTH ||
        !QUALIFIED_NAME_PATTERN.matcher(label).matches()) {
      label
    } else {
      val half = (MAX_LABEL_LENGTH - 3) / 2
      label.substring(0, half) + "..." +
        label.substring(label.length - half)
    }
  }

  private val DppVersion = "spark.sql.dpp.planInfo.version"
  private val DppId = "spark.sql.dpp.planInfo.id"
  private val DppRef = "spark.sql.dpp.planInfo.ref"
  private val DppReferenceName = "DPPSharedPlanReference"

  private sealed trait DppDisplayTarget
  private final case class DppVisibleNode(node: SparkPlanGraphNode) extends DppDisplayTarget
  private final case class DppTargetAlias(id: String) extends DppDisplayTarget
  private sealed trait DppMaterializationState
  private case object DppBuilding extends DppMaterializationState
  private final case class DppReady(target: DppDisplayTarget) extends DppMaterializationState
  private final case class DppLegacyGraphEntry(
      planInfo: SparkPlanInfo, node: SparkPlanGraphNode)

  private def invalidDpp(message: String): Nothing = {
    throw new IllegalStateException(s"Invalid DPP SparkPlanInfo: $message")
  }

  private def indexDppDetails(root: SparkPlanInfo): Option[Map[String, SparkPlanInfo]] = {
    lazy val details = mutable.HashMap[String, SparkPlanInfo]()
    var hasProtocol = false
    val visited = new java.util.IdentityHashMap[SparkPlanInfo, java.lang.Boolean]()
    def index(plan: SparkPlanInfo): Unit = {
      if (visited.put(plan, true) == null) {
        val metadata = plan.metadata
        if (metadata.contains(DppVersion) || metadata.contains(DppId) ||
            metadata.contains(DppRef) || plan.nodeName == DppReferenceName) {
          hasProtocol = true
          if (!metadata.get(DppVersion).contains("1")) {
            invalidDpp("missing or unsupported version")
          }
          (metadata.get(DppId), metadata.get(DppRef)) match {
            case (Some(id), None) if id.nonEmpty && plan.nodeName != DppReferenceName =>
              details.get(id).foreach { other =>
                if (!(other eq plan)) invalidDpp(s"conflicting detail ID $id")
              }
              details(id) = plan
            case (None, Some(ref)) if ref.nonEmpty && plan.nodeName == DppReferenceName &&
                plan.children.isEmpty && plan.metrics.isEmpty &&
                metadata.keySet == Set(DppVersion, DppRef) =>
            case _ => invalidDpp("malformed detail or reference")
          }
        }
        plan.children.foreach(index)
      }
    }
    index(root)
    // Reference-only graphs still need validation; graphs without markers use the legacy path.
    if (!hasProtocol) return None
    // Check the resolved graph as well as the physical tree. A reference is always a leaf in
    // the latter, but must not hide a cycle in the former.
    val active = new java.util.IdentityHashMap[SparkPlanInfo, java.lang.Boolean]()
    val done = new java.util.IdentityHashMap[SparkPlanInfo, java.lang.Boolean]()
    def validate(plan: SparkPlanInfo): Unit = {
      if (!done.containsKey(plan)) {
        if (active.put(plan, true) != null) invalidDpp("cyclic reference graph")
        plan.metadata.get(DppRef) match {
          case Some(id) => validate(details.getOrElse(id, invalidDpp(s"missing detail ID $id")))
          case _ => plan.children.foreach(validate)
        }
        active.remove(plan)
        done.put(plan, true)
      }
    }
    validate(root)
    Some(details.toMap)
  }

  private final class DppGraphBuildContext(
      index: Option[Map[String, SparkPlanInfo]],
      val edges: mutable.ArrayBuffer[SparkPlanGraphEdge],
      materialize: (SparkPlanInfo, SparkPlanGraphCluster, DppGraphBuildContext) =>
        DppDisplayTarget) {
    val enabled: Boolean = index.isDefined
    private val details = index.getOrElse(Map.empty)
    // Share the graph-building algorithm, but allocate reference state only for protocol graphs.
    lazy val targets = mutable.HashMap[String, DppDisplayTarget]()
    lazy val pending = mutable.ArrayBuffer[(DppDisplayTarget, SparkPlanGraphNode)]()
    private lazy val states = mutable.HashMap[String, DppMaterializationState]()
    private lazy val referenceOwners = mutable.HashMap[String, SparkPlanGraphCluster]()
    private lazy val resolving = mutable.HashSet[String]()
    private lazy val nodeTargets =
      new java.util.IdentityHashMap[SparkPlanInfo, DppDisplayTarget]()
    private lazy val equivalent = new java.util.IdentityHashMap[SparkPlanInfo,
      java.util.IdentityHashMap[SparkPlanInfo, java.lang.Boolean]]()

    private def resolve(plan: SparkPlanInfo): SparkPlanInfo = {
      plan.metadata.get(DppRef).map { id =>
        details.getOrElse(id, invalidDpp(s"missing detail ID $id"))
      }.getOrElse(plan)
    }

    def legacyHash(plan: SparkPlanInfo): Int = {
      if (enabled) resolve(plan).simpleString.hashCode else plan.hashCode()
    }

    def legacyEquivalent(left: SparkPlanInfo, right: SparkPlanInfo): Boolean = {
      if (!enabled) return left == right
      val l = resolve(left)
      val r = resolve(right)
      var row = equivalent.get(l)
      if (row == null) {
        row = new java.util.IdentityHashMap[SparkPlanInfo, java.lang.Boolean]()
        equivalent.put(l, row)
      }
      val cached = row.get(r)
      if (cached != null) return cached.booleanValue()
      val result = l.nodeName == r.nodeName && l.simpleString == r.simpleString &&
        l.children.length == r.children.length &&
        l.children.zip(r.children).forall { case (a, b) => legacyEquivalent(a, b) }
      row.put(r, result)
      result
    }

    def rememberReferenceOwner(id: String, subgraph: SparkPlanGraphCluster): Unit = {
      if (!referenceOwners.contains(id)) referenceOwners(id) = subgraph
    }

    def ensureDetailTarget(id: String): DppDisplayTarget = {
      if (!resolving.add(id)) invalidDpp(s"cyclic display alias $id")
      try {
        val target = states.get(id) match {
          case Some(DppReady(ready)) => ready
          case Some(DppBuilding) => invalidDpp(s"reentrant detail construction $id")
          case None =>
            val detail = details.getOrElse(id, invalidDpp(s"missing detail ID $id"))
            materialize(detail, referenceOwners.getOrElse(id, null), this)
            states.get(id) match {
              case Some(DppReady(ready)) => ready
              case _ => invalidDpp(s"incomplete detail construction $id")
            }
        }
        target match {
          case alias: DppTargetAlias => ensureDetailTarget(alias.id)
          case visible => visible
        }
      } finally {
        resolving.remove(id)
      }
    }

    def buildOrReuseDetail(
        planInfo: SparkPlanInfo,
        parent: SparkPlanGraphNode)(build: => DppDisplayTarget): DppDisplayTarget = {
      if (!enabled) return build
      planInfo.metadata.get(DppId) match {
        case None => build
        case Some(id) => states.get(id) match {
          case Some(DppReady(target)) =>
            if (parent != null) pending += ((target, parent))
            target
          case Some(DppBuilding) => invalidDpp(s"reentrant detail construction $id")
          case None =>
            states(id) = DppBuilding
            val target = build
            states(id) = DppReady(target)
            target
        }
      }
    }

    def bind(plan: SparkPlanInfo, target: DppDisplayTarget): DppDisplayTarget = {
      if (!enabled) return target
      val existing = Option(nodeTargets.get(plan)).collect { case v: DppVisibleNode => v }
        .orElse(plan.metadata.get(DppId).flatMap(targets.get).collect {
          case v: DppVisibleNode => v
        })
      val selected = existing.getOrElse(target)
      nodeTargets.put(plan, selected)
      plan.metadata.get(DppId).foreach { id => targets(id) = selected }
      selected
    }

    def bindEquivalentDetails(current: SparkPlanInfo, existing: SparkPlanInfo): Unit = {
      if (!enabled) return
      val seen = new java.util.IdentityHashMap[SparkPlanInfo,
        java.util.IdentityHashMap[SparkPlanInfo, java.lang.Boolean]]()
      def bindPair(left: SparkPlanInfo, right: SparkPlanInfo): Unit = {
        val l = resolve(left)
        val r = resolve(right)
        var row = seen.get(l)
        if (row == null) {
          row = new java.util.IdentityHashMap[SparkPlanInfo, java.lang.Boolean]()
          seen.put(l, row)
        }
        if (row.put(r, true) == null) {
          r.metadata.get(DppId).foreach(ensureDetailTarget)
          val target = Option(nodeTargets.get(r)).getOrElse(
            invalidDpp(s"missing visible representative for ${r.nodeName}"))
          val bound = bind(l, target)
          l.children.zip(r.children).foreach { case (a, b) => bindPair(a, b) }
          l.metadata.get(DppId).foreach { id => states(id) = DppReady(bound) }
        }
      }
      bindPair(current, existing)
    }
  }

  private final class DppComparablePlanInfo(
      val planInfo: SparkPlanInfo, val context: DppGraphBuildContext) {
    override def hashCode(): Int = context.legacyHash(planInfo)
    override def equals(other: Any): Boolean = other match {
      case that: DppComparablePlanInfo =>
        (context eq that.context) && context.legacyEquivalent(planInfo, that.planInfo)
      case _ => false
    }
  }

  private def resolveDppReferenceEdges(context: DppGraphBuildContext): Unit = {
    def visible(target: DppDisplayTarget): SparkPlanGraphNode = target match {
      case DppVisibleNode(node) => node
      case DppTargetAlias(id) => visible(context.ensureDetailTarget(id))
    }
    context.targets.keys.toVector.foreach(context.ensureDetailTarget)
    var index = 0
    while (index < context.pending.length) {
      val (target, parent) = context.pending(index)
      context.edges += SparkPlanGraphEdge(visible(target).id, parent.id)
      index += 1
    }
  }

  /** Build a SparkPlanGraph from the root of a SparkPlan tree. */
  def apply(planInfo: SparkPlanInfo): SparkPlanGraph = {
    val nodeIdGenerator = new AtomicLong(0)
    val nodes = mutable.ArrayBuffer[SparkPlanGraphNode]()
    val edges = mutable.ArrayBuffer[SparkPlanGraphEdge]()
    val exchanges = mutable.HashMap[DppComparablePlanInfo, DppLegacyGraphEntry]()
    val dpp = new DppGraphBuildContext(indexDppDetails(planInfo), edges,
      (detail, owner, context) => buildSparkPlanGraphNode(
        detail, nodeIdGenerator, nodes, edges, null, owner, exchanges, context))
    buildSparkPlanGraphNode(planInfo, nodeIdGenerator, nodes, edges, null, null, exchanges, dpp)
    if (dpp.enabled) resolveDppReferenceEdges(dpp)
    new SparkPlanGraph(nodes.toSeq, edges.toSeq)
  }

  private def buildSparkPlanGraphNode(
      planInfo: SparkPlanInfo,
      nodeIdGenerator: AtomicLong,
      nodes: mutable.ArrayBuffer[SparkPlanGraphNode],
      edges: mutable.ArrayBuffer[SparkPlanGraphEdge],
      parent: SparkPlanGraphNode,
      subgraph: SparkPlanGraphCluster,
      exchanges: mutable.HashMap[DppComparablePlanInfo, DppLegacyGraphEntry],
      dpp: DppGraphBuildContext): DppDisplayTarget = {
    if (planInfo.nodeName == "Subquery" && subgraph != null) {
      return buildSparkPlanGraphNode(
        planInfo, nodeIdGenerator, nodes, edges, parent, null, exchanges, dpp)
    }
    def buildUnmaterialized(): DppDisplayTarget = {
      def build(child: SparkPlanInfo, cluster: SparkPlanGraphCluster): DppDisplayTarget = {
        buildSparkPlanGraphNode(child, nodeIdGenerator, nodes, edges, parent, cluster,
          exchanges, dpp)
      }
      def reused(plan: SparkPlanInfo): Option[DppDisplayTarget] = {
        exchanges.get(new DppComparablePlanInfo(plan, dpp)).map { entry =>
          dpp.bindEquivalentDetails(plan, entry.planInfo)
          if (parent != null) {
            val edge = SparkPlanGraphEdge(entry.node.id, parent.id)
            if (dpp.enabled || planInfo.nodeName != "Subquery" || !edges.contains(edge)) {
              edges += edge
            }
          }
          DppVisibleNode(entry.node)
        }
      }
      def buildVisible(): DppDisplayTarget = {
        val metrics = planInfo.metrics.map { metric =>
          SQLPlanMetric(metric.name, metric.accumulatorId, metric.metricType)
        }
        val node = new SparkPlanGraphNode(nodeIdGenerator.getAndIncrement(), planInfo.nodeName,
          planInfo.simpleString, metrics)
        if (subgraph == null) nodes += node else subgraph.nodes += node
        if (planInfo.nodeName.contains("Exchange") || planInfo.nodeName == "Subquery") {
          exchanges += new DppComparablePlanInfo(planInfo, dpp) ->
            DppLegacyGraphEntry(planInfo, node)
        }
        // Register before descending so descendants skipped by legacy reuse can bind aliases.
        dpp.bind(planInfo, DppVisibleNode(node))
        if (parent != null) edges += SparkPlanGraphEdge(node.id, parent.id)
        planInfo.children.foreach { child =>
          buildSparkPlanGraphNode(
            child, nodeIdGenerator, nodes, edges, node, subgraph, exchanges, dpp)
        }
        DppVisibleNode(node)
      }
      val target = planInfo.nodeName match {
        case DppReferenceName =>
          val id = planInfo.metadata(DppRef)
          dpp.rememberReferenceOwner(id, subgraph)
          val alias = DppTargetAlias(id)
          if (parent != null) dpp.pending += ((alias, parent))
          alias
        case name if name.startsWith("WholeStageCodegen") =>
          val metrics = planInfo.metrics.map { metric =>
            SQLPlanMetric(metric.name, metric.accumulatorId, metric.metricType)
          }
          val cluster = new SparkPlanGraphCluster(
            nodeIdGenerator.getAndIncrement(), planInfo.nodeName, planInfo.simpleString,
            mutable.ArrayBuffer[SparkPlanGraphNode](), metrics)
          nodes += cluster
          build(planInfo.children.head, cluster)
        case "InputAdapter" => build(planInfo.children.head, null)
        case "BroadcastQueryStage" | "ShuffleQueryStage" =>
          reused(planInfo.children.head).getOrElse(build(planInfo.children.head, null))
        case "TableCacheQueryStage" | "ResultQueryStage" =>
          build(planInfo.children.head, null)
        case "Subquery" => reused(planInfo).getOrElse(buildVisible())
        case "ReusedSubquery" => build(planInfo.children.head, subgraph)
        case "ReusedExchange" =>
          reused(planInfo.children.head).getOrElse(buildVisible())
        case _ => buildVisible()
      }
      dpp.bind(planInfo, target)
    }
    dpp.buildOrReuseDetail(planInfo, parent) {
      buildUnmaterialized()
    }
  }

}

/**
 * Represent a node in the SparkPlan tree, along with its metrics.
 *
 * @param id generated by "SparkPlanGraph". There is no duplicate id in a graph
 * @param name the name of this SparkPlan node
 * @param metrics metrics that this SparkPlan node will track
 */
class SparkPlanGraphNode(
    val id: Long,
    val name: String,
    val desc: String,
    val metrics: collection.Seq[SQLPlanMetric]) {

  def makeDotNode(metricsValue: Map[Long, String]): String = {
    val nodeId = s"node$id"
    val tooltip = StringEscapeUtils.escapeJava(desc)
    val labelStr = StringEscapeUtils.escapeJava(
      SparkPlanGraph.truncateLabel(name))
    s"""  $id [id="$nodeId" label="$labelStr" tooltip="$tooltip"];"""
  }
}

/**
 * Represent a tree of SparkPlan for WholeStageCodegen.
 */
class SparkPlanGraphCluster(
    id: Long,
    name: String,
    desc: String,
    val nodes: mutable.ArrayBuffer[SparkPlanGraphNode],
    metrics: collection.Seq[SQLPlanMetric])
  extends SparkPlanGraphNode(id, name, desc, metrics) {

  override def makeDotNode(metricsValue: Map[Long, String]): String = {
    val duration = metrics.filter(m =>
      m.name != null && m.name.startsWith(WholeStageCodegenExec.PIPELINE_DURATION_METRIC))
    // Extract short label: "(1)" from "WholeStageCodegen (1)"
    val shortName = if (name != null) {
      "\\(\\d+\\)".r.findFirstIn(name).getOrElse(name)
    } else {
      ""
    }
    val labelStr = if (duration.nonEmpty) {
      require(duration.length == 1)
      val durationMetricId = duration(0).accumulatorId
      if (metricsValue.contains(durationMetricId)) {
        // For multi-line values like "total (min, med, max)\n10.0 s (...)",
        // extract just the total number from the data line
        val raw = metricsValue(durationMetricId)
        val lines = raw.split("\n")
        val dataLine = if (lines.length > 1) lines(1).trim else lines(0).trim
        // Extract just the total value (first token before any parenthesized breakdown)
        val total = dataLine.split("\\s*\\(")(0).trim
        shortName + " / " + total
      } else {
        shortName
      }
    } else {
      shortName
    }
    val clusterId = s"cluster$id"
    s"""
       |  subgraph $clusterId {
       |    isCluster="true";
       |    id="$clusterId";
       |    label="${StringEscapeUtils.escapeJava(labelStr)}";
       |    tooltip="${StringEscapeUtils.escapeJava(desc)}";
       |    ${nodes.map(_.makeDotNode(metricsValue)).mkString("    \n")}
       |  }
     """.stripMargin
  }
}


/**
 * Represent an edge in the SparkPlan tree. `fromId` is the child node id, and `toId` is the parent
 * node id.
 */
case class SparkPlanGraphEdge(fromId: Long, toId: Long) {

  def makeDotEdge(
      nodeMap: Map[Long, SparkPlanGraphNode],
      metricsValue: Map[Long, String]): String = {
    val label = nodeMap.get(fromId).flatMap { node =>
      node.metrics
        .find(_.name == "number of output rows")
        .flatMap(m => metricsValue.get(m.accumulatorId))
    }
    label match {
      case Some(rows) =>
        val escapedRows = StringEscapeUtils.escapeJava(rows)
        s"""  $fromId->$toId [label="$escapedRows" labeltooltip="$escapedRows rows"];\n"""
      case None =>
        s"""  $fromId->$toId;\n"""
    }
  }
}
