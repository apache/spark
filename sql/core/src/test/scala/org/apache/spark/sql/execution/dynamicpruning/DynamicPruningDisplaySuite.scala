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

package org.apache.spark.sql.execution.dynamicpruning

import java.nio.charset.StandardCharsets.UTF_8
import java.util.IdentityHashMap
import java.util.concurrent.ConcurrentLinkedQueue

import scala.jdk.CollectionConverters._

import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, DynamicPruningExpression, DynamicPruningSubquery, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.ui.{SparkListenerSQLAdaptiveExecutionUpdate, SparkListenerSQLExecutionStart, SparkPlanGraph, SparkPlanGraphCluster}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.LongType
import org.apache.spark.util.JsonProtocol

class DynamicPruningDisplaySuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  private def flatten(info: SparkPlanInfo): Seq[SparkPlanInfo] =
    info +: info.children.flatMap(flatten)

  private def checkDescription(info: SparkPlanInfo): Unit = {
    val nodes = flatten(info)
    val ids = nodes.flatMap(_.metadata.get("spark.sql.dpp.planInfo.id"))
    assert(ids.distinct.size == ids.size)
    nodes.filter(_.metadata.contains("spark.sql.dpp.planInfo.ref")).foreach { ref =>
      assert(ids.contains(ref.metadata("spark.sql.dpp.planInfo.ref")))
      assert(ref.children.isEmpty && ref.metrics.isEmpty)
    }
    val graph = SparkPlanGraph(info)
    val visible = graph.allNodes.filterNot(_.isInstanceOf[SparkPlanGraphCluster]).map(_.id).toSet
    assert(graph.edges.forall(e => visible(e.fromId) && visible(e.toId)))
    assert(!graph.allNodes.exists(_.name == "DPPSharedPlanReference"))
  }

  test("SPARK-48486: ordinary and AQE subquery text defines each shared body once") {
    for (adaptive <- Seq(false, true); depth <- Seq(2, 8, 16)) {
      def leaf(): SparkPlan =
        LocalTableScanExec(Seq(AttributeReference("k", LongType)()), Nil, None)
      var body = leaf()
      for (level <- 1 to depth) {
        def subquery(name: String): BaseSubqueryExec = {
          if (adaptive) {
            SubqueryAdaptiveBroadcastExec(name, Seq(0), true,
              LocalRelation(body.output), body.output, body)(None)
          } else {
            SubqueryExec(name, body)
          }
        }
        val left = subquery(s"left-$level")
        val right = subquery(s"right-$level")
        val expressions = Seq(left, right).map { sub =>
          Alias(DynamicPruningExpression(
            InSubqueryExec(Literal(1L), sub, NamedExpression.newExprId)), sub.name)()
        }
        body = ProjectExec(expressions, leaf())
      }
      val text = body.treeString
      val refs = "Shared subquery plan, first shown at subquery id=#([0-9]+)".r
        .findAllMatchIn(text).toSeq
      assert(refs.size == depth)
      refs.foreach(m => assert(text.contains(s"[id=#${m.group(1)}]")))
      assert(text.split("\n").count(_.contains("Project")) == depth)
      val withOutput = body.treeString(verbose = false, addSuffix = false, maxFields = 100,
        printOperatorId = false, printOutputColumns = true)
      val references = withOutput.linesIterator.filter(_.contains("Shared subquery plan")).toSeq
      assert(references.size == depth)
      assert(references.forall(_.contains("<output=")), withOutput)
      // A failed append must not leak the render's visited state into a later call.
      intercept[IllegalStateException] {
        body.treeString((_: String) => throw new IllegalStateException("append failed"),
          verbose = true, addSuffix = false, maxFields = 100, printOperatorId = false,
          printOutputColumns = false)
      }
      assert(body.treeString == text)
    }
  }

  private def query(depth: Int, broadcast: Boolean, fact: Int => String): String = {
    val hint = if (broadcast) "/*+ BROADCAST(q) */" else ""
    val nested = (1 to depth).foldLeft(
        "SELECT k, x, CAST(0 AS BIGINT) payload FROM display_dim WHERE flag = 1") {
      (child, level) => s"SELECT $hint q.k, q.x, q.payload + f.payload payload " +
        s"FROM ($child) q JOIN ${fact(level)} f ON q.k = f.k AND q.x = f.x"
    }
    s"SELECT sum(payload) FROM ($nested) q"
  }

  private def createDimension(): Unit = {
    Seq((0L, 0L, 1), (1L, 1L, 0), (2L, 2L, 0)).toDF("k", "x", "flag")
      .coalesce(1).write.saveAsTable("display_dim")
  }

  private def createFact(name: String, size: Long): Unit = {
    spark.range(size).selectExpr("id % 3 k", "id x", "id + 1 payload")
      .coalesce(1).write.partitionBy("k").saveAsTable(name)
  }

  test("SPARK-48486: real nested DPP descriptions stay bounded after JSON serialization") {
    withTable("display_dim", "display_fact") {
      createDimension()
      createFact("display_fact", 30)
      for (adaptive <- Seq(false, true); depth <- Seq(5, 9, 13)) {
        withSQLConf(
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
            SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> "true",
            SQLConf.SUBQUERY_REUSE_ENABLED.key -> "true",
            SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true",
            SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
            SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
          val df = sql(query(depth, broadcast = true, _ => "display_fact"))
          val dpp = df.queryExecution.optimizedPlan.flatMap { node =>
            node.expressions.flatMap(_.collect { case d: DynamicPruningSubquery => d })
          }
          assert(dpp.size == depth)
          val plan = df.queryExecution.executedPlan
          // Independently enumerate description edges by physical identity, without traversing
          // a repeated object's descendants. Do not infer this bound from the capture counters.
          val seen = new IdentityHashMap[SparkPlan, java.lang.Boolean]()
          var edgeCount = 0
          def visit(p: SparkPlan): Unit = {
            if (seen.put(p, true) == null) {
              val children = p match {
                case r: ReusedExchangeExec => Seq(r.child)
                case r: ReusedSubqueryExec => Seq(r.child)
                case a: AdaptiveSparkPlanExec => Seq(a.executedPlan)
                case s: QueryStageExec => Seq(s.plan)
                case _ => p.children ++ p.subqueries
              }
              edgeCount += children.size
              children.foreach(visit)
            }
          }
          visit(plan)
          val tree = plan.treeString
          val formatted = df.queryExecution.explainString(FormattedMode)
          val info = SparkPlanInfo.fromSparkPlan(plan)
          val start = SparkListenerSQLExecutionStart(
            0L, Some(0L), "nested DPP", "", formatted, info, 0L)
          val json = JsonProtocol.sparkEventToJsonString(start)
          val jsonBytes = json.getBytes(UTF_8).length
          val restored = JsonProtocol.sparkEventFromJson(json)
            .asInstanceOf[SparkListenerSQLExecutionStart]
          val nodes = flatten(restored.sparkPlanInfo)
          assert(nodes.size <= edgeCount + 1)
          assert(nodes.exists(_.nodeName == "DPPSharedPlanReference"))
          assert(restored.physicalPlanDescription == formatted)
          checkDescription(restored.sparkPlanInfo)
          // Indentation and per-node expressions may grow with depth. These bounds allow that
          // growth but reject the previous path-wise expansion, without elapsed-time assertions.
          assert(tree.split("\n").length <= 4 * (edgeCount + 1))
          assert(formatted.split("\n").length <= 40 * (edgeCount + 1))
          val maxLabel = nodes.map(_.simpleString.length).max
          assert(jsonBytes <= formatted.length * 2 + (edgeCount + 1) * (maxLabel * 2 + 4096))
          this.info(s"DPP_DISPLAY adaptive=$adaptive depth=$depth objects=${seen.size()} " +
            s"edges=$edgeCount nodes=${nodes.size} tree=${tree.length} " +
            s"formatted=${formatted.length} jsonBytes=$jsonBytes")
        }
      }
    }
  }

  test("SPARK-48486: actions serialize start and AQE events across DPP and reuse modes") {
    withTable("display_dim", "display_fact_1", "display_fact_2") {
      createDimension()
      createFact("display_fact_1", 30)
      createFact("display_fact_2", 300)
      for (adaptive <- Seq(false, true); broadcast <- Seq(false, true);
          exchangeReuse <- Seq(false, true); subqueryReuse <- Seq(false, true)) {
        withSQLConf(
            SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> adaptive.toString,
            SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
            SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
            SQLConf.DYNAMIC_PARTITION_PRUNING_REUSE_BROADCAST_ONLY.key -> broadcast.toString,
            SQLConf.DYNAMIC_PARTITION_PRUNING_USE_STATS.key -> "false",
            SQLConf.DYNAMIC_PARTITION_PRUNING_FALLBACK_FILTER_RATIO.key -> "1.0",
            SQLConf.EXCHANGE_REUSE_ENABLED.key -> exchangeReuse.toString,
            SQLConf.SUBQUERY_REUSE_ENABLED.key -> subqueryReuse.toString,
            SQLConf.UI_EXPLAIN_MODE.key -> "formatted",
            SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
          val events = new ConcurrentLinkedQueue[String]()
          val listener = new SparkListener {
            override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
              case _: SparkListenerSQLExecutionStart | _: SparkListenerSQLAdaptiveExecutionUpdate =>
                events.add(JsonProtocol.sparkEventToJsonString(event))
              case _ =>
            }
          }
          val sc = spark.sparkContext
          sc.listenerBus.waitUntilEmpty()
          sc.addSparkListener(listener)
          val df = sql(query(2, broadcast, level => s"display_fact_$level"))
          try {
            assert(df.collect().toSeq == Seq(Row(2L)))
            val finalText = df.queryExecution.executedPlan.treeString
            assert(finalText.nonEmpty)
            if (adaptive) {
              assert(finalText.contains("== Final Plan =="))
              assert(finalText.contains("== Initial Plan =="))
            }
            sc.listenerBus.waitUntilEmpty()
          } finally {
            sc.removeSparkListener(listener)
          }
          val decoded = events.asScala.map(JsonProtocol.sparkEventFromJson).toSeq
          assert(decoded.collect { case start: SparkListenerSQLExecutionStart => start }
            .exists(_.physicalPlanDescription.contains("display_fact_2")))
          if (adaptive) {
            val updates = decoded.collect {
              case update: SparkListenerSQLAdaptiveExecutionUpdate => update
            }
            assert(updates
              .exists(_.physicalPlanDescription.contains("display_fact_2")))
          }
          decoded.foreach {
            case start: SparkListenerSQLExecutionStart =>
              assert(start.physicalPlanDescription.nonEmpty)
              checkDescription(start.sparkPlanInfo)
            case update: SparkListenerSQLAdaptiveExecutionUpdate =>
              assert(update.physicalPlanDescription.nonEmpty)
              checkDescription(update.sparkPlanInfo)
          }
        }
      }
    }
  }
}
