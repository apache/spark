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

import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, NamedRelation}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.reader.{Scan, SupportsReportStatistics}
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousInputStream, InputStream, MicroBatchInputStream, Offset}
import org.apache.spark.sql.sources.v2.writer.BatchWriteSupport
import org.apache.spark.sql.types.StructType

/**
 * A logical plan representing a data source v2 scan.
 *
 * @param source An instance of a [[DataSourceV2]] implementation.
 * @param options The options for this scan. Used to create fresh [[BatchWriteSupport]].
 * @param userSpecifiedSchema The user-specified schema for this scan.
 */
case class DataSourceV2Relation(
    source: DataSourceV2,
    table: SupportsBatchRead,
    output: Seq[AttributeReference],
    options: Map[String, String],
    tableIdent: Option[TableIdentifier] = None,
    userSpecifiedSchema: Option[StructType] = None)
  extends LeafNode with MultiInstanceRelation with NamedRelation with DataSourceV2StringFormat {

  import DataSourceV2Relation._

  override def name: String = {
    tableIdent.map(_.unquotedString).getOrElse(s"${source.name}:unknown")
  }

  override def pushedFilters: Seq[Expression] = Seq.empty

  override def simpleString: String = "RelationV2 " + metadataString

  def newWriteSupport(): BatchWriteSupport = source.createWriteSupport(options, schema)

  override def computeStats(): Statistics = {
    val dsOptions = new DataSourceOptions(options.asJava)
    val config = table.newScanConfigBuilder(dsOptions).build()
    table.createBatchScan(config, dsOptions) match {
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
    output: Seq[Attribute],
    source: DataSourceV2,
    options: Map[String, String],
    stream: InputStream,
    startOffset: Option[Offset] = None,
    endOffset: Option[Offset] = None)
  extends LeafNode with MultiInstanceRelation with DataSourceV2StringFormat {

  override def isStreaming: Boolean = true

  override def simpleString: String = "Streaming RelationV2 " + metadataString

  override def pushedFilters: Seq[Expression] = Nil

  override def newInstance(): LogicalPlan = copy(output = output.map(_.newInstance()))

  // TODO: unify the equal/hashCode implementation for all data source v2 query plans.
  override def equals(other: Any): Boolean = other match {
    case other: StreamingDataSourceV2Relation =>
      output == other.output && source.getClass == other.source.getClass &&
        options == other.options && startOffset == other.startOffset && endOffset == other.endOffset
    case _ => false
  }

  override def hashCode(): Int = {
    Seq(output, source, options).hashCode()
  }

  def createScan(): Scan = (startOffset, endOffset) match {
    case (Some(start), Some(end)) =>
      stream.asInstanceOf[MicroBatchInputStream].createMicroBatchScan(start, end)
    case (Some(start), None) =>
      stream.asInstanceOf[ContinuousInputStream].createContinuousScan(start)
    case _ =>
      throw new IllegalStateException("[BUG] wrong offsets in StreamingDataSourceV2Relation.")
  }

  override def computeStats(): Statistics = createScan() match {
    case r: SupportsReportStatistics =>
      val statistics = r.estimateStatistics()
      Statistics(sizeInBytes = statistics.sizeInBytes().orElse(conf.defaultSizeInBytes))
    case _ => Statistics(sizeInBytes = conf.defaultSizeInBytes)
  }
}

object DataSourceV2Relation {
  private implicit class SourceHelpers(source: DataSourceV2) {

    def asFormat: Format = source match {
      case f: Format => f
      case _ =>
        throw new AnalysisException(s"Data source is not readable: $name")
    }

    def asWriteSupportProvider: BatchWriteSupportProvider = {
      source match {
        case provider: BatchWriteSupportProvider =>
          provider
        case _ =>
          throw new AnalysisException(s"Data source is not writable: $name")
      }
    }

    def name: String = {
      source match {
        case registered: DataSourceRegister =>
          registered.shortName()
        case _ =>
          source.getClass.getSimpleName
      }
    }

    def getTable(
        options: Map[String, String],
        userSpecifiedSchema: Option[StructType]): Table = {
      val v2Options = new DataSourceOptions(options.asJava)
      userSpecifiedSchema match {
        case Some(s) =>
          asFormat.getTable(v2Options, s)
        case _ =>
          asFormat.getTable(v2Options)
      }
    }

    def createWriteSupport(
        options: Map[String, String],
        schema: StructType): BatchWriteSupport = {
      asWriteSupportProvider.createBatchWriteSupport(
        UUID.randomUUID().toString,
        schema,
        SaveMode.Append,
        new DataSourceOptions(options.asJava)).get
    }
  }

  def create(
      source: DataSourceV2,
      options: Map[String, String],
      tableIdent: Option[TableIdentifier] = None,
      userSpecifiedSchema: Option[StructType] = None): Option[DataSourceV2Relation] = {
    val table = source.getTable(options, userSpecifiedSchema)
    val output = table.schema().toAttributes
    val ident = tableIdent.orElse(tableFromOptions(options))
    table match {
      case batch: SupportsBatchRead =>
        Some(DataSourceV2Relation(
          source, batch, output, options, ident, userSpecifiedSchema))
      case _ =>
        None
    }
  }

  private def tableFromOptions(options: Map[String, String]): Option[TableIdentifier] = {
    options
      .get(DataSourceOptions.TABLE_KEY)
      .map(TableIdentifier(_, options.get(DataSourceOptions.DATABASE_KEY)))
  }
}
