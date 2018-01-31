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

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, Statistics}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, ReadSupportWithSchema}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.types.StructType

/**
 * A logical plan representing a data source relation, which will be planned to a data scan
 * operator finally.
 *
 * @param output The output of this relation.
 * @param source The instance of a data source v2 implementation.
 * @param options The options specified for this scan, used to create the `DataSourceReader`.
 * @param userSpecifiedSchema The user specified schema, used to create the `DataSourceReader`.
 * @param filters The predicates which are pushed and handled by this data source.
 * @param existingReader A mutable reader carrying some temporary stats during optimization and
 *                       planning. It's always None before optimization, and does not take part in
 *                       the equality of this plan, which means this plan is still immutable.
 */
case class DataSourceV2Relation(
    output: Seq[AttributeReference],
    source: DataSourceV2,
    options: DataSourceOptions,
    userSpecifiedSchema: Option[StructType],
    filters: Set[Expression],
    existingReader: Option[DataSourceReader]) extends LeafNode with DataSourceV2QueryPlan {

  override def references: AttributeSet = AttributeSet.empty

  override def sourceClass: Class[_ <: DataSourceV2] = source.getClass

  override def canEqual(other: Any): Boolean = other.isInstanceOf[DataSourceV2Relation]

  def reader: DataSourceReader = existingReader.getOrElse {
    (source, userSpecifiedSchema) match {
      case (ds: ReadSupportWithSchema, Some(schema)) =>
        ds.createReader(schema, options)

      case (ds: ReadSupport, None) =>
        ds.createReader(options)

      case (ds: ReadSupport, Some(schema)) =>
        val reader = ds.createReader(options)
        // Sanity check, this should be guaranteed by `DataFrameReader.load`
        assert(reader.readSchema() == schema)
        reader

      case _ => throw new IllegalStateException()
    }
  }

  override def computeStats(): Statistics = reader match {
    case r: SupportsReportStatistics =>
      Statistics(sizeInBytes = r.getStatistics.sizeInBytes().orElse(conf.defaultSizeInBytes))
    case _ =>
      Statistics(sizeInBytes = conf.defaultSizeInBytes)
  }

  override def simpleString: String = s"Relation $metadataString"
}

/**
 * A specialization of DataSourceV2Relation with the streaming bit set to true. Otherwise identical
 * to the non-streaming relation.
 */
case class StreamingDataSourceV2Relation(
    output: Seq[AttributeReference],
    reader: DataSourceReader) extends LeafNode {
  override def isStreaming: Boolean = true
}

object DataSourceV2Relation {
  def apply(
      source: DataSourceV2,
      schema: StructType,
      options: DataSourceOptions,
      userSpecifiedSchema: Option[StructType]): DataSourceV2Relation = {
    DataSourceV2Relation(
      schema.toAttributes, source, options, userSpecifiedSchema, Set.empty, None)
  }
}
