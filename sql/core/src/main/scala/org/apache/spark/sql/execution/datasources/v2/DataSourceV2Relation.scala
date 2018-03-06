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
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, ReadSupportWithSchema}
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, SupportsPushDownCatalystFilters, SupportsPushDownFilters, SupportsPushDownRequiredColumns, SupportsReportStatistics}
import org.apache.spark.sql.types.StructType

case class DataSourceV2Relation(
    source: DataSourceV2,
    options: Map[String, String],
    projection: Seq[AttributeReference],
    filters: Option[Seq[Expression]] = None,
    userSpecifiedSchema: Option[StructType] = None) extends LeafNode with MultiInstanceRelation {

  import DataSourceV2Relation._

  override def simpleString: String = {
    s"DataSourceV2Relation(source=${source.name}, " +
      s"schema=[${output.map(a => s"$a ${a.dataType.simpleString}").mkString(", ")}], " +
      s"filters=[${pushedFilters.mkString(", ")}], options=$options)"
  }

  override lazy val schema: StructType = reader.readSchema()

  override lazy val output: Seq[AttributeReference] = {
    // use the projection attributes to avoid assigning new ids. fields that are not projected
    // will be assigned new ids, which is okay because they are not projected.
    val attrMap = projection.map(a => a.name -> a).toMap
    schema.map(f => attrMap.getOrElse(f.name,
      AttributeReference(f.name, f.dataType, f.nullable, f.metadata)()))
  }

  private lazy val v2Options: DataSourceOptions = makeV2Options(options)

  lazy val (
      reader: DataSourceReader,
      unsupportedFilters: Seq[Expression],
      pushedFilters: Seq[Expression]) = {
    val newReader = userSpecifiedSchema match {
      case Some(s) =>
        source.asReadSupportWithSchema.createReader(s, v2Options)
      case _ =>
        source.asReadSupport.createReader(v2Options)
    }

    DataSourceV2Relation.pushRequiredColumns(newReader, projection.toStructType)

    val (remainingFilters, pushedFilters) = filters match {
      case Some(filterSeq) =>
        DataSourceV2Relation.pushFilters(newReader, filterSeq)
      case _ =>
        (Nil, Nil)
    }

    (newReader, remainingFilters, pushedFilters)
  }

  override def doCanonicalize(): LogicalPlan = {
    val c = super.doCanonicalize().asInstanceOf[DataSourceV2Relation]

    // override output with canonicalized output to avoid attempting to configure a reader
    val canonicalOutput: Seq[AttributeReference] = this.output
        .map(a => QueryPlan.normalizeExprId(a, projection))

    new DataSourceV2Relation(c.source, c.options, c.projection) {
      override lazy val output: Seq[AttributeReference] = canonicalOutput
    }
  }

  override def computeStats(): Statistics = reader match {
    case r: SupportsReportStatistics =>
      Statistics(sizeInBytes = r.getStatistics.sizeInBytes().orElse(conf.defaultSizeInBytes))
    case _ =>
      Statistics(sizeInBytes = conf.defaultSizeInBytes)
  }

  override def newInstance(): DataSourceV2Relation = {
    // projection is used to maintain id assignment.
    // if projection is not set, use output so the copy is not equal to the original
    copy(projection = projection.map(_.newInstance()))
  }
}

/**
 * A specialization of DataSourceV2Relation with the streaming bit set to true. Otherwise identical
 * to the non-streaming relation.
 */
case class StreamingDataSourceV2Relation(
    output: Seq[AttributeReference],
    reader: DataSourceReader)
    extends LeafNode with DataSourceReaderHolder with MultiInstanceRelation {
  override def isStreaming: Boolean = true

  override def canEqual(other: Any): Boolean = other.isInstanceOf[StreamingDataSourceV2Relation]

  override def newInstance(): LogicalPlan = copy(output = output.map(_.newInstance()))

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
      filters: Option[Seq[Expression]] = None,
      userSpecifiedSchema: Option[StructType] = None): DataSourceV2Relation = {
    val projection = schema(source, makeV2Options(options), userSpecifiedSchema).toAttributes
    DataSourceV2Relation(source, options, projection, filters, userSpecifiedSchema)
  }

  private def pushRequiredColumns(reader: DataSourceReader, struct: StructType): Unit = {
    reader match {
      case projectionSupport: SupportsPushDownRequiredColumns =>
        projectionSupport.pruneColumns(struct)
      case _ =>
    }
  }

  private def pushFilters(
      reader: DataSourceReader,
      filters: Seq[Expression]): (Seq[Expression], Seq[Expression]) = {
    reader match {
      case catalystFilterSupport: SupportsPushDownCatalystFilters =>
        (
            catalystFilterSupport.pushCatalystFilters(filters.toArray),
            catalystFilterSupport.pushedCatalystFilters()
        )

      case filterSupport: SupportsPushDownFilters =>
        // A map from original Catalyst expressions to corresponding translated data source
        // filters. If a predicate is not in this map, it means it cannot be pushed down.
        val translatedMap: Map[Expression, Filter] = filters.flatMap { p =>
          DataSourceStrategy.translateFilter(p).map(f => p -> f)
        }.toMap

        // Catalyst predicate expressions that cannot be converted to data source filters.
        val nonConvertiblePredicates = filters.filterNot(translatedMap.contains)

        // Data source filters that cannot be pushed down. An unhandled filter means
        // the data source cannot guarantee the rows returned can pass the filter.
        // As a result we must return it so Spark can plan an extra filter operator.
        val unhandledFilters = filterSupport.pushFilters(translatedMap.values.toArray).toSet
        val (unhandledPredicates, pushedPredicates) = translatedMap.partition { case (_, f) =>
          unhandledFilters.contains(f)
        }

        (nonConvertiblePredicates ++ unhandledPredicates.keys, pushedPredicates.keys.toSeq)

      case _ => (filters, Nil)
    }
  }
}
