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

import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, NamedRelation}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.writer._
import org.apache.spark.sql.types.StructType

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
    options: Map[String, String])
  extends LeafNode with MultiInstanceRelation with NamedRelation {

  override def name: String = table.name()

  override def simpleString(maxFields: Int): String = {
    s"RelationV2${truncatedString(output, "[", ", ", "]", maxFields)} $name"
  }

  def newScanBuilder(): ScanBuilder = table match {
    case s: SupportsBatchRead =>
      val dsOptions = new DataSourceOptions(options.asJava)
      s.newScanBuilder(dsOptions)
    case _ => throw new AnalysisException(s"Table is not readable: ${table.name()}")
  }



  def newWriteBuilder(schema: StructType): WriteBuilder = table match {
    case s: SupportsBatchWrite =>
      val dsOptions = new DataSourceOptions(options.asJava)
      s.newWriteBuilder(dsOptions)
        .withQueryId(UUID.randomUUID().toString)
        .withInputDataSchema(schema)
    case _ => throw new AnalysisException(s"Table is not writable: ${table.name()}")
  }

  override def computeStats(): Statistics = {
    val scan = newScanBuilder().build()
    scan match {
      case r: SupportsReportStatistics =>
        val statistics = r.estimateStatistics()
        Statistics(sizeInBytes = statistics.sizeInBytes().orElse(conf.defaultSizeInBytes))
      case _ =>
        Statistics(sizeInBytes = conf.defaultSizeInBytes)
    }
  }

  override def newInstance(): DataSourceV2Relation = {
    copy(output = output.map(_.newInstance()))
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
    output: Seq[AttributeReference],
    source: DataSourceV2,
    options: Map[String, String],
    readSupport: ReadSupport,
    scanConfigBuilder: ScanConfigBuilder)
  extends LeafNode with MultiInstanceRelation with DataSourceV2StringFormat {

  override def isStreaming: Boolean = true

  override def simpleString(maxFields: Int): String = {
    "Streaming RelationV2 " + metadataString(maxFields)
  }

  override def pushedFilters: Seq[Expression] = Nil

  override def newInstance(): LogicalPlan = copy(output = output.map(_.newInstance()))

  // TODO: unify the equal/hashCode implementation for all data source v2 query plans.
  override def equals(other: Any): Boolean = other match {
    case other: StreamingDataSourceV2Relation =>
      output == other.output && readSupport.getClass == other.readSupport.getClass &&
        options == other.options
    case _ => false
  }

  override def hashCode(): Int = {
    Seq(output, source, options).hashCode()
  }

  override def computeStats(): Statistics = readSupport match {
    case r: OldSupportsReportStatistics =>
      val statistics = r.estimateStatistics(scanConfigBuilder.build())
      Statistics(sizeInBytes = statistics.sizeInBytes().orElse(conf.defaultSizeInBytes))
    case _ =>
      Statistics(sizeInBytes = conf.defaultSizeInBytes)
  }
}

object DataSourceV2Relation {
  def create(table: Table, options: Map[String, String]): DataSourceV2Relation = {
    val output = table.schema().toAttributes
    DataSourceV2Relation(table, output, options)
  }
}
