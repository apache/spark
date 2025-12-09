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

package org.apache.spark.sql.metricview.serde

import scala.util.{Failure, Success, Try}

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonInclude, JsonProperty}
import com.fasterxml.jackson.annotation.JsonInclude.Include

import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.metricview.serde.ColumnType.ColumnType
import org.apache.spark.sql.metricview.serde.SourceType.SourceType

private[sql] sealed abstract class MetricViewSerdeException(
    message: String,
    cause: Option[Throwable] = None)
  extends Exception(message, cause.orNull)

private[sql] case class MetricViewValidationException(
    message: String,
    cause: Option[Throwable] = None)
  extends MetricViewSerdeException(message, cause)

private[sql] case class MetricViewFromProtoException(
    message: String,
    cause: Option[Throwable] = None)
  extends MetricViewSerdeException(message, cause)

private[sql] case class MetricViewYAMLParsingException(
    message: String,
    cause: Option[Throwable] = None)
  extends MetricViewSerdeException(message, cause)

// Expression types in a Metric View
private[sql] sealed trait Expression {
  def expr: String
}

// Dimension expression representing a scalar value
private[sql] case class DimensionExpression(expr: String) extends Expression

// Measure expression representing an aggregated value
private[sql] case class MeasureExpression(expr: String) extends Expression

private[sql] object SourceType extends Enumeration {
  type SourceType = Value
  val ASSET, SQL = Value

  def fromString(sourceType: String): SourceType = {
    values.find(_.toString.equalsIgnoreCase(sourceType)).getOrElse {
      throw MetricViewFromProtoException(
        s"Unsupported source type: $sourceType"
      )
    }
  }
}

// Representation of a source in the Metric View
private[sql] sealed trait Source {
  def sourceType: SourceType
}

// Asset source, representing a catalog table, view, or Metric View, etc.
private[sql] case class AssetSource(name: String) extends Source {
  val sourceType: SourceType = SourceType.ASSET

  override def toString: String = this.name
}

// SQL source, representing a SQL query
private[sql] case class SQLSource(sql: String) extends Source {
  val sourceType: SourceType = SourceType.SQL

  override def toString: String = this.sql
}

private[sql] object Source {
  def apply(sourceText: String): Source = {
    if (sourceText.isEmpty) {
      throw MetricViewValidationException("Source cannot be empty")
    }
    Try(CatalystSqlParser.parseTableIdentifier(sourceText)) match {
      case Success(_) => AssetSource(sourceText)
      case Failure(_) =>
        Try(CatalystSqlParser.parseQuery(sourceText)) match {
          case Success(_) => SQLSource(sourceText)
          case Failure(queryEx) =>
            throw MetricViewValidationException(
              s"Invalid source: $sourceText",
              Some(queryEx)
            )
        }
    }
  }
}

private[sql] case class Column(
    name: String,
    expression: Expression,
    ordinal: Int) {
  def columnType: ColumnType = expression match {
    case _: DimensionExpression => ColumnType.Dimension
    case _: MeasureExpression => ColumnType.Measure
    case _ =>
      throw MetricViewValidationException(
        s"Unsupported expression type: ${expression.getClass.getName}"
      )
  }

  def getColumnMetadata: ColumnMetadata = {
    val expr = expression.expr
    if (expr.length > Constants.MAXIMUM_PROPERTY_SIZE) {
      throw MetricViewValidationException(
        s"Expression length ${expr.length} exceeds maximum allowed size " +
        s"${Constants.MAXIMUM_PROPERTY_SIZE} for column '$name'"
      )
    }
    ColumnMetadata(columnType.toString, expr)
  }
}

private[sql] object ColumnType extends Enumeration {
  type ColumnType = Value
  val Dimension: ColumnType = Value("dimension")
  val Measure: ColumnType = Value("measure")

  // Method to match case-insensitively and return the correct value
  def fromString(columnType: String): ColumnType = {
    values.find(_.toString.equalsIgnoreCase(columnType)).getOrElse {
      throw MetricViewFromProtoException(
        s"Unsupported column type: $columnType"
      )
    }
  }
}

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_ABSENT)
private[sql] case class ColumnMetadata(
    @JsonProperty(value = Constants.COLUMN_TYPE_PROPERTY_KEY, required = true)
    columnType: String, // "type" -> "metric_view.type"
    @JsonProperty(value = Constants.COLUMN_EXPR_PROPERTY_KEY, required = true)
    expr: String // "expr" -> "metric_view.expr"
)

// Only parse the "version" field and ignore all others
@JsonIgnoreProperties(ignoreUnknown = true)
private[sql] case class YAMLVersion(version: String) {
}

private[sql] case class MetricView(
    version: String,
    from: Source,
    where: Option[String] = None,
    select: Seq[Column])
