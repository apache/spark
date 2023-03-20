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

import java.util.{IdentityHashMap => JavaIdentityMap}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.columnar.{CachedRDDBuilder, InMemoryTableScanExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.metric.SQLMetricInfo
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.IdGenerator

/**
 * :: DeveloperApi ::
 * Stores information about a SQL SparkPlan.
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

  def fromSparkPlan(plan: SparkPlan, inMemoryRelationEncountered:
  JavaIdentityMap[CachedRDDBuilder, Integer] = new JavaIdentityMap[CachedRDDBuilder, Integer](),
  idGenerator: IdGenerator = new IdGenerator()): SparkPlanInfo = {
    val (children, decoratedPlanForStringGeneration) = plan match {
      case ReusedExchangeExec(_, child) => (child :: Nil) -> plan
      case ReusedSubqueryExec(child) => (child :: Nil) -> plan
      case a: AdaptiveSparkPlanExec => (a.executedPlan :: Nil) -> plan
      case stage: QueryStageExec => (stage.plan :: Nil) -> plan
      case inMemTab: InMemoryTableScanExec =>
        if (inMemoryRelationEncountered.containsKey(inMemTab.relation.cacheBuilder)) {
          // get the id from the map
          val id = inMemoryRelationEncountered.get(inMemTab.relation.cacheBuilder)
          Seq.empty -> WrapperInMemoryTableScanExec(id, inMemTab)
        } else {
          val id = idGenerator.next
          inMemoryRelationEncountered.put(inMemTab.relation.cacheBuilder, id)
          (inMemTab.relation.cachedPlan :: Nil) -> WrapperInMemoryTableScanExec(id, inMemTab)
        }
      case _ => (plan.children ++ plan.subqueries) -> plan
    }

    val metrics = plan.metrics.toSeq.map { case (key, metric) =>
      new SQLMetricInfo(metric.name.getOrElse(key), metric.id, metric.metricType)
    }

    // dump the file scan metadata (e.g file path) to event log
    val metadata = plan match {
      case fileScan: FileSourceScanExec => fileScan.metadata
      case _ => Map[String, String]()
    }

    val simpleString = decoratedPlanForStringGeneration.simpleString(SQLConf.get.maxToStringFields)

    new SparkPlanInfo(decoratedPlanForStringGeneration.nodeName,
      simpleString, children.map(child => fromSparkPlan(child,
        inMemoryRelationEncountered, idGenerator)), metadata, metrics)
  }

  private case class WrapperInMemoryTableScanExec(repeatId: Int, imr: InMemoryTableScanExec)
    extends LeafExecNode {
    private val prefix = s"InMemoryTableScan(Repeat Identifier: $repeatId) "

    override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException

    override def output: Seq[Attribute] = imr.output
    override def simpleString(maxFields: Int): String = s"$prefix ${imr.simpleString(maxFields)}"

    override def nodeName: String = s"${imr.nodeName}(Repeat Identifier: $repeatId)"
  }
}
