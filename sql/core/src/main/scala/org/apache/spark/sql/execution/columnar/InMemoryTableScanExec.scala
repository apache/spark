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
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.columnar.CachedBatch
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.vectorized.ColumnarBatch

case class InMemoryTableScanExec(
    attributes: Seq[Attribute],
    predicates: Seq[Expression],
    @transient relation: InMemoryRelation)
  extends LeafExecNode {

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

  override def innerChildren: Seq[QueryPlan[_]] = Seq(relation) ++ super.innerChildren

  override def doCanonicalize(): SparkPlan =
    copy(attributes = attributes.map(QueryPlan.normalizeExpressions(_, relation.output)),
      predicates = predicates.map(QueryPlan.normalizeExpressions(_, relation.output)),
      relation = relation.canonicalized.asInstanceOf[InMemoryRelation])

  override def vectorTypes: Option[Seq[String]] =
    relation.cacheBuilder.serializer.vectorTypes(attributes, conf)

  /**
   * If true, get data from ColumnVector in ColumnarBatch, which are generally faster.
   * If false, get data from UnsafeRow build from CachedBatch
   */
  override val supportsColumnar: Boolean = {
    conf.cacheVectorizedReaderEnabled  &&
        !WholeStageCodegenExec.isTooManyFields(conf, relation.schema) &&
        relation.cacheBuilder.serializer.supportsColumnarOutput(relation.schema)
  }

  private lazy val columnarInputRDD: RDD[ColumnarBatch] = {
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

  private lazy val inputRDD: RDD[InternalRow] = {
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

  override def output: Seq[Attribute] = attributes

  private def updateAttribute(expr: Expression): Expression = {
    // attributes can be pruned so using relation's output.
    // E.g., relation.output is [id, item] but this scan's output can be [item] only.
    val attrMap = AttributeMap(relation.cachedPlan.output.zip(relation.output))
    expr.transform {
      case attr: Attribute => attrMap.getOrElse(attr, attr)
    }
  }

  // The cached version does not change the outputPartitioning of the original SparkPlan.
  // But the cached version could alias output, so we need to replace output.
  override def outputPartitioning: Partitioning = {
    relation.cachedPlan.outputPartitioning match {
      case e: Expression => updateAttribute(e).asInstanceOf[Partitioning]
      case other => other
    }
  }

  // The cached version does not change the outputOrdering of the original SparkPlan.
  // But the cached version could alias output, so we need to replace output.
  override def outputOrdering: Seq[SortOrder] =
    relation.cachedPlan.outputOrdering.map(updateAttribute(_).asInstanceOf[SortOrder])

  lazy val enableAccumulatorsForTest: Boolean = sqlContext.conf.inMemoryTableScanStatisticsEnabled

  // Accumulators used for testing purposes
  lazy val readPartitions = sparkContext.longAccumulator
  lazy val readBatches = sparkContext.longAccumulator

  private val inMemoryPartitionPruningEnabled = sqlContext.conf.inMemoryPartitionPruning

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
    inputRDD
  }

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    columnarInputRDD
  }
}
