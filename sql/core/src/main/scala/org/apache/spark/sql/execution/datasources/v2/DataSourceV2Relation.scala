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

import scala.collection.JavaConverters._

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, ReadSupportWithSchema}
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, SupportsReportStatistics}
import org.apache.spark.sql.types.StructType

/**
 * A logical plan representing a data source v2 scan.
 *
 * @param source An instance of a [[DataSourceV2]] implementation.
 * @param options The options for this scan. Used to create fresh [[DataSourceReader]].
 * @param userSpecifiedSchema The user-specified schema for this scan. Used to create fresh
 *                            [[DataSourceReader]].
 * @param optimizedReader An optimized [[DataSourceReader]] which is produced by the optimizer rule
 *                        [[PushDownOperatorsToDataSource]]. It is a temporary value and is excluded
 *                        in the equality definition of this class. It is to avoid re-applying
 *                        operators pushdown and re-creating [[DataSourceReader]] when we copy
 *                        the relation during query plan transformation.
 * @param pushedFilters The filters that are pushed down to the data source.
 */
case class DataSourceV2Relation(
    output: Seq[AttributeReference],
    source: DataSourceV2,
    options: Map[String, String],
    userSpecifiedSchema: Option[StructType],
    optimizedReader: Option[DataSourceReader] = None,
    pushedFilters: Seq[Expression] = Nil)
  extends LeafNode with MultiInstanceRelation with DataSourceV2StringFormat {

  import DataSourceV2Relation._

  def createFreshReader: DataSourceReader = source.createReader(options, userSpecifiedSchema)

  def reader: DataSourceReader = optimizedReader.getOrElse(createFreshReader)

  override def simpleString: String = "RelationV2 " + metadataString

  override def equals(other: Any): Boolean = other match {
    case other: DataSourceV2Relation =>
      output == other.output && source.getClass == other.source.getClass &&
        options == other.options && userSpecifiedSchema == other.userSpecifiedSchema &&
        pushedFilters == other.pushedFilters
    case _ => false
  }

  override def hashCode(): Int = {
    Seq(output, source.getClass, options, userSpecifiedSchema, pushedFilters).hashCode()
  }

  // `LogicalPlanStats` caches the computed statistics, so we are fine here even the
  // `optimizedReader` is None. We won't create `DataSourceReader` many times.
  override def computeStats(): Statistics = reader match {
    case r: SupportsReportStatistics =>
      Statistics(sizeInBytes = r.getStatistics.sizeInBytes().orElse(conf.defaultSizeInBytes))
    case _ =>
      Statistics(sizeInBytes = conf.defaultSizeInBytes)
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
    reader: DataSourceReader)
  extends LeafNode with MultiInstanceRelation with DataSourceV2StringFormat {

  override def isStreaming: Boolean = true

  override def simpleString: String = "Streaming RelationV2 " + metadataString

  override def pushedFilters: Seq[Expression] = Nil

  override def newInstance(): LogicalPlan = copy(output = output.map(_.newInstance()))

  // TODO: unify the equal/hashCode implementation for all data source v2 query plans.
  override def equals(other: Any): Boolean = other match {
    case other: StreamingDataSourceV2Relation =>
      output == other.output && reader.getClass == other.reader.getClass && options == other.options
    case _ => false
  }

  override def hashCode(): Int = {
    Seq(output, source, options).hashCode()
  }

  override def computeStats(): Statistics = reader match {
    case r: SupportsReportStatistics =>
      Statistics(sizeInBytes = r.getStatistics.sizeInBytes().orElse(conf.defaultSizeInBytes))
    case _ =>
      Statistics(sizeInBytes = conf.defaultSizeInBytes)
  }
}

object DataSourceV2Relation {

  private implicit class SourceHelpers(source: DataSourceV2) {

    private def asReadSupport: ReadSupport = source match {
      case support: ReadSupport =>
        support
      case _: ReadSupportWithSchema =>
        // this method is only called if there is no user-supplied schema. if there is no
        // user-supplied schema and ReadSupport was not implemented, throw a helpful exception.
        throw new AnalysisException(s"Data source requires a user-supplied schema: $name")
      case _ =>
        throw new AnalysisException(s"Data source is not readable: $name")
    }

    private def asReadSupportWithSchema: ReadSupportWithSchema = source match {
      case support: ReadSupportWithSchema =>
        support
      case _: ReadSupport =>
        throw new AnalysisException(
          s"Data source does not support user-supplied schema: $name")
      case _ =>
        throw new AnalysisException(s"Data source is not readable: $name")
    }


    private def name: String = source match {
      case registered: DataSourceRegister =>
        registered.shortName()
      case _ =>
        source.getClass.getSimpleName
    }

    def createReader(
        options: Map[String, String],
        userSpecifiedSchema: Option[StructType]): DataSourceReader = {
      val v2Options = new DataSourceOptions(options.asJava)
      userSpecifiedSchema match {
        case Some(s) =>
          asReadSupportWithSchema.createReader(s, v2Options)
        case _ =>
          asReadSupport.createReader(v2Options)
      }
    }
  }

  def create(
      source: DataSourceV2,
      options: Map[String, String],
      userSpecifiedSchema: Option[StructType]): DataSourceV2Relation = {
    val reader = source.createReader(options, userSpecifiedSchema)
    DataSourceV2Relation(
      reader.readSchema().toAttributes, source, options, userSpecifiedSchema)
  }
}
