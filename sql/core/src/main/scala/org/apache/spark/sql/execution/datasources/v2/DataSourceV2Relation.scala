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
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, ReadSupportWithSchema}
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, SupportsPushDownCatalystFilters, SupportsPushDownFilters, SupportsPushDownRequiredColumns, SupportsReportStatistics}
import org.apache.spark.sql.types.StructType

case class DataSourceV2Relation(
    source: DataSourceV2,
    output: Seq[AttributeReference],
    options: Map[String, String],
    userSpecifiedSchema: Option[StructType] = None)
  extends LeafNode with MultiInstanceRelation with DataSourceV2StringFormat {

  import DataSourceV2Relation._

  override def pushedFilters: Seq[Expression] = Seq.empty

  override def simpleString: String = "RelationV2 " + metadataString

  lazy val v2Options: DataSourceOptions = makeV2Options(options)

  def newReader: DataSourceReader = userSpecifiedSchema match {
    case Some(userSchema) =>
      source.asReadSupportWithSchema.createReader(userSchema, v2Options)
    case None =>
      source.asReadSupport.createReader(v2Options)
  }

  override def computeStats(): Statistics = newReader match {
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
    def asReadSupport: ReadSupport = {
      source match {
        case support: ReadSupport =>
          support
        case _: ReadSupportWithSchema =>
          // this method is only called if there is no user-supplied schema. if there is no
          // user-supplied schema and ReadSupport was not implemented, throw a helpful exception.
          throw new AnalysisException(s"Data source requires a user-supplied schema: $name")
        case _ =>
          throw new AnalysisException(s"Data source is not readable: $name")
      }
    }

    def asReadSupportWithSchema: ReadSupportWithSchema = {
      source match {
        case support: ReadSupportWithSchema =>
          support
        case _: ReadSupport =>
          throw new AnalysisException(
            s"Data source does not support user-supplied schema: $name")
        case _ =>
          throw new AnalysisException(s"Data source is not readable: $name")
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
  }

  private def makeV2Options(options: Map[String, String]): DataSourceOptions = {
    new DataSourceOptions(options.asJava)
  }

  private def schema(
      source: DataSourceV2,
      v2Options: DataSourceOptions,
      userSchema: Option[StructType]): StructType = {
    val reader = userSchema match {
      case Some(s) =>
        source.asReadSupportWithSchema.createReader(s, v2Options)
      case _ =>
        source.asReadSupport.createReader(v2Options)
    }
    reader.readSchema()
  }

  def create(
      source: DataSourceV2,
      options: Map[String, String],
      userSpecifiedSchema: Option[StructType] = None): DataSourceV2Relation = {
    val output = schema(source, makeV2Options(options), userSpecifiedSchema).toAttributes
    DataSourceV2Relation(source, output, options, userSpecifiedSchema)
  }

  def pushRequiredColumns(
      relation: DataSourceV2Relation,
      reader: DataSourceReader,
      struct: StructType): Seq[AttributeReference] = {
    reader match {
      case projectionSupport: SupportsPushDownRequiredColumns =>
        projectionSupport.pruneColumns(struct)
        // return the output columns from the relation that were projected
        val attrMap = relation.output.map(a => a.name -> a).toMap
        projectionSupport.readSchema().map(f => attrMap(f.name))
      case _ =>
        relation.output
    }
  }

  def pushFilters(
      reader: DataSourceReader,
      filters: Seq[Expression]): (Seq[Expression], Seq[Expression]) = {
    reader match {
      case r: SupportsPushDownCatalystFilters =>
        val postScanFilters = r.pushCatalystFilters(filters.toArray)
        val pushedFilters = r.pushedCatalystFilters()
        (postScanFilters, pushedFilters)

      case r: SupportsPushDownFilters =>
        // A map from translated data source filters to original catalyst filter expressions.
        val translatedFilterToExpr = scala.collection.mutable.HashMap.empty[Filter, Expression]
        // Catalyst filter expression that can't be translated to data source filters.
        val untranslatableExprs = scala.collection.mutable.ArrayBuffer.empty[Expression]

        for (filterExpr <- filters) {
          val translated = DataSourceStrategy.translateFilter(filterExpr)
          if (translated.isDefined) {
            translatedFilterToExpr(translated.get) = filterExpr
          } else {
            untranslatableExprs += filterExpr
          }
        }

        // Data source filters that need to be evaluated again after scanning. which means
        // the data source cannot guarantee the rows returned can pass these filters.
        // As a result we must return it so Spark can plan an extra filter operator.
        val postScanFilters =
        r.pushFilters(translatedFilterToExpr.keys.toArray).map(translatedFilterToExpr)
        // The filters which are marked as pushed to this data source
        val pushedFilters = r.pushedFilters().map(translatedFilterToExpr)

        (untranslatableExprs ++ postScanFilters, pushedFilters)

      case _ => (filters, Nil)
    }
  }
}
