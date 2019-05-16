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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, NamedRelation}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.reader.{Statistics => V2Statistics, _}
import org.apache.spark.sql.sources.v2.reader.streaming.{Offset, SparkDataStream}
import org.apache.spark.sql.sources.v2.writer._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A logical plan representing a data source v2 table.
 *
 * @param table   The table that this relation represents.
 * @param options The options for this table operation. It's used to create fresh [[ScanBuilder]]
 *                and [[WriteBuilder]].
 */
case class DataSourceV2Relation(
    table: Table,
    output: Seq[AttributeReference],
    options: CaseInsensitiveStringMap)
  extends LeafNode with MultiInstanceRelation with NamedRelation {

  import DataSourceV2Implicits._

  override def name: String = table.name()

  override def skipSchemaResolution: Boolean = table.supports(TableCapability.ACCEPT_ANY_SCHEMA)

  override def simpleString(maxFields: Int): String = {
    s"RelationV2${truncatedString(output, "[", ", ", "]", maxFields)} $name"
  }

  def newScanBuilder(): ScanBuilder = {
    table.asReadable.newScanBuilder(options)
  }

  override def computeStats(): Statistics = {
    val scan = newScanBuilder().build()
    scan match {
      case r: SupportsReportStatistics =>
        val statistics = r.estimateStatistics()
        DataSourceV2Relation.transformV2Stats(statistics, None, conf.defaultSizeInBytes)
      case _ =>
        Statistics(sizeInBytes = conf.defaultSizeInBytes)
    }
  }

  override def newInstance(): DataSourceV2Relation = {
    copy(output = output.map(_.newInstance()))
  }

  override def refresh(): Unit = table match {
    case table: FileTable => table.fileIndex.refresh()
    case _ => // Do nothing.
  }
}

/**
 * A specialization of [[DataSourceV2Relation]] with the streaming bit set to true.
 *
 * Note that, this plan has a mutable reader, so Spark won't apply operator push-down for this plan,
 * to avoid making the plan mutable. We should consolidate this plan and [[DataSourceV2Relation]]
 * after we figure out how to apply operator push-down for streaming data sources.
 */
case class StreamingDataSourceV2Relation(
    output: Seq[Attribute],
    scan: Scan,
    stream: SparkDataStream,
    startOffset: Option[Offset] = None,
    endOffset: Option[Offset] = None)
  extends LeafNode with MultiInstanceRelation {

  override def isStreaming: Boolean = true

  override def newInstance(): LogicalPlan = copy(output = output.map(_.newInstance()))

  override def computeStats(): Statistics = scan match {
    case r: SupportsReportStatistics =>
      val statistics = r.estimateStatistics()
      DataSourceV2Relation.transformV2Stats(statistics, None, conf.defaultSizeInBytes)
    case _ =>
      Statistics(sizeInBytes = conf.defaultSizeInBytes)
  }
}

object DataSourceV2Relation {
  def create(table: Table, options: CaseInsensitiveStringMap): DataSourceV2Relation = {
    val output = table.schema().toAttributes
    DataSourceV2Relation(table, output, options)
  }

  /**
   * This is used to transform data source v2 statistics to logical.Statistics.
   */
  def transformV2Stats(
      v2Statistics: V2Statistics,
      defaultRowCount: Option[BigInt],
      defaultSizeInBytes: Long): Statistics = {
    val numRows: Option[BigInt] = if (v2Statistics.numRows().isPresent) {
      Some(v2Statistics.numRows().getAsLong)
    } else {
      defaultRowCount
    }
    Statistics(
      sizeInBytes = v2Statistics.sizeInBytes().orElse(defaultSizeInBytes),
      rowCount = numRows)
  }
}
