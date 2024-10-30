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

package org.apache.spark.sql.execution.columnar

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Common trait for all InMemoryTableScans implementations to facilitate pattern matching.
 */
trait InMemoryTableScanLike extends LeafExecNode {

  /**
   * Returns whether the cache buffer is loaded
   */
  def isMaterialized: Boolean

  /**
   * Returns the actual cached RDD without filters and serialization of row/columnar.
   */
  def baseCacheRDD(): RDD[CachedBatch]

  /**
   * Returns the runtime statistics after materialization.
   */
  def runtimeStatistics: Statistics
}

case class InMemoryTableScanExec(
    attributes: Seq[Attribute],
    predicates: Seq[Expression],
    @transient relation: InMemoryRelation)
  extends InMemoryTableScanLike {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override val nodeName: String = {
    relation.cacheBuilder.tableName match {
      case Some(_) =>
        "Scan " + relation.cacheBuilder.cachedName
      case _ =>
        super.nodeName
    }
  }

  override def simpleStringWithNodeId(): String = {
    val columnarInfo = if (relation.cacheBuilder.supportsColumnarInput || supportsColumnar) {
      s" (columnarIn=${relation.cacheBuilder.supportsColumnarInput}, columnarOut=$supportsColumnar)"
    } else {
      ""
    }
    super.simpleStringWithNodeId() + columnarInfo
  }

  override def innerChildren: Seq[QueryPlan[_]] = Seq(relation) ++ super.innerChildren

  override def doCanonicalize(): SparkPlan =
    copy(attributes = attributes.map(QueryPlan.normalizeExpressions(_, relation.output)),
      predicates = predicates.map(QueryPlan.normalizeExpressions(_, relation.output)),
      relation = relation.canonicalized.asInstanceOf[InMemoryRelation])

  override def vectorTypes: Option[Seq[String]] =
    relation.cacheBuilder.serializer.vectorTypes(attributes, conf)

  override def supportsRowBased: Boolean = true

  /**
   * If true, get data from ColumnVector in ColumnarBatch, which are generally faster.
   * If false, get data from UnsafeRow build from CachedBatch
   */
  override val supportsColumnar: Boolean = {
    conf.cacheVectorizedReaderEnabled  &&
        !WholeStageCodegenExec.isTooManyFields(conf, relation.schema) &&
        relation.cacheBuilder.serializer.supportsColumnarOutput(relation.schema)
  }

  override def output: Seq[Attribute] = attributes

  private def cachedPlan = relation.cachedPlan match {
    case adaptive: AdaptiveSparkPlanExec if adaptive.isFinalPlan => adaptive.executedPlan
    case other => other
  }

  private def updateAttribute(expr: Expression): Expression = {
    // attributes can be pruned so using relation's output.
    // E.g., relation.output is [id, item] but this scan's output can be [item] only.
    val attrMap = AttributeMap(cachedPlan.output.zip(relation.output))
    expr.transform {
      case attr: Attribute => attrMap.getOrElse(attr, attr)
    }
  }

  // The cached version does not change the outputPartitioning of the original SparkPlan.
  // But the cached version could alias output, so we need to replace output.
  override def outputPartitioning: Partitioning = {
    cachedPlan.outputPartitioning match {
      case e: Expression => updateAttribute(e).asInstanceOf[Partitioning]
      case other => other
    }
  }

  // The cached version does not change the outputOrdering of the original SparkPlan.
  // But the cached version could alias output, so we need to replace output.
  override def outputOrdering: Seq[SortOrder] =
    cachedPlan.outputOrdering.map(updateAttribute(_).asInstanceOf[SortOrder])

  lazy val enableAccumulatorsForTest: Boolean = conf.inMemoryTableScanStatisticsEnabled

  // Accumulators used for testing purposes
  lazy val readPartitions = sparkContext.longAccumulator
  lazy val readBatches = sparkContext.longAccumulator

  private val inMemoryPartitionPruningEnabled = conf.inMemoryPartitionPruning

  private def filteredCachedBatches(): RDD[CachedBatch] = {
    val buffers = relation.cacheBuilder.cachedColumnBuffers

    if (inMemoryPartitionPruningEnabled) {
      val filterFunc = relation.cacheBuilder.serializer.buildFilter(predicates, relation.output)
      buffers.mapPartitionsWithIndexInternal(filterFunc)
    } else {
      buffers
    }
  }

  protected override def doExecute(): RDD[InternalRow] = {
    // Resulting RDD is cached and reused by SparkPlan.executeRDD
    if (enableAccumulatorsForTest) {
      readPartitions.setValue(0)
      readBatches.setValue(0)
    }

    val numOutputRows = longMetric("numOutputRows")
    // Using these variables here to avoid serialization of entire objects (if referenced
    // directly) within the map Partitions closure.
    val relOutput = relation.output
    val serializer = relation.cacheBuilder.serializer

    // update SQL metrics
    val withMetrics =
      filteredCachedBatches().mapPartitionsInternal { iter =>
        if (enableAccumulatorsForTest && iter.hasNext) {
          readPartitions.add(1)
        }
        iter.map { batch =>
          if (enableAccumulatorsForTest) {
            readBatches.add(1)
          }
          numOutputRows += batch.numRows
          batch
        }
      }
    serializer.convertCachedBatchToInternalRow(withMetrics, relOutput, attributes, conf)
  }

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    // Resulting RDD is cached and reused by SparkPlan.executeColumnarRDD
    val numOutputRows = longMetric("numOutputRows")
    val buffers = filteredCachedBatches()
    relation.cacheBuilder.serializer.convertCachedBatchToColumnarBatch(
      buffers,
      relation.output,
      attributes,
      conf).map { cb =>
      numOutputRows += cb.numRows()
      cb
    }
  }

  override def isMaterialized: Boolean = relation.cacheBuilder.isCachedColumnBuffersLoaded

  /**
   * This method is only used by AQE which executes the actually cached RDD that without filter and
   * serialization of row/columnar.
   */
  override def baseCacheRDD(): RDD[CachedBatch] = {
    relation.cacheBuilder.cachedColumnBuffers
  }

  /**
   * Returns the runtime statistics after shuffle materialization.
   */
  override def runtimeStatistics: Statistics = relation.computeStats()
}
