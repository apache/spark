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

import java.util.Locale

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Cast, DateFormatClass, DayOfMonth, EqualTo, Expression, GetStructField, GreaterThan, GreaterThanOrEqual, Hour, IntegerLiteral, IsNull, LessThan, LessThanOrEqual, Literal, Month, StringLiteral, Substring, TruncDate, TruncTimestamp, Year}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.catalyst.types.DataTypeUtils.{fromAttributes, toAttributes}
import org.apache.spark.sql.catalyst.util.{quoteIfNeeded, CaseInsensitiveMap, GeneratedColumn}
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connector.expressions.IdentityTransform
import org.apache.spark.sql.types.{DataType, DateType, StringType, StructField, StructType, TimestampType}

/**
 * Derives partition filters from data filters when a DataSource V2 table is partitioned by a
 * generated column. For example, if a table has a partition column
 * `date GENERATED ALWAYS AS (CAST(eventTime AS DATE))`, a data filter `eventTime < '2021-01-01'`
 * can be used to derive the partition filter `date <= DATE '2021-01-01'`, which lets the connector
 * prune partitions without scanning all of them.
 *
 * Used by [[V2ScanRelationPushDown]]. The derived filters are only used to help the data source
 * prune data; because each derived filter is implied by the original data filter it is derived
 * from, applying it never removes valid rows.
 */
object GeneratedColumnPartitionFilters extends Logging {

  private val DATE_FORMAT_YEAR_MONTH = "yyyy-MM"
  private val DATE_FORMAT_YEAR_MONTH_DAY = "yyyy-MM-dd"
  private val DATE_FORMAT_YEAR_MONTH_DAY_HOUR = "yyyy-MM-dd-HH"

  /**
   * Extracts the base (source) column referenced by a data filter. Handles both top level columns
   * and nested fields, returning the field path and the column data type.
   */
  private object ExtractBaseColumn {
    def unapply(e: Expression): Option[(Seq[String], DataType)] = e match {
      case a: AttributeReference =>
        Some((Seq(a.name), a.dataType))
      case g: GetStructField => g.child match {
        case ExtractBaseColumn(nameParts, _) =>
          Some((nameParts :+ g.extractFieldName, g.dataType))
        case _ => None
      }
      case _ => None
    }
  }

  private def createFieldPath(nameParts: Seq[String]): String = {
    nameParts.map(quoteIfNeeded).mkString(".")
  }

  /**
   * Returns the schema of the table's top level, identity-partitioned columns, preserving field
   * metadata (which carries the generation expression). Only such columns can be generated
   * partition columns for this optimization; the schema is empty if the table has none.
   */
  private[v2] def identityPartitionSchema(relation: DataSourceV2Relation): StructType = {
    val partitionColumnNames = relation.table.partitioning().collect {
      case t: IdentityTransform if t.ref.fieldNames().length == 1 => t.ref.fieldNames().head
    }.toSet
    StructType(relation.output.collect {
      case a if partitionColumnNames.contains(a.name) =>
        StructField(a.name, a.dataType, a.nullable, a.metadata)
    })
  }

  /**
   * The main entry point used by [[V2ScanRelationPushDown]]. Given a relation and its data filters,
   * returns additional partition filters derived from the relation's generated partition columns.
   * The returned filters are analyzed (resolved and type coerced) against the relation output and
   * reference only the partition columns, so they are safe to add to the list of filters that are
   * pushed down to the data source.
   */
  def generatePartitionFilters(
      spark: SparkSession,
      relation: DataSourceV2Relation,
      dataFilters: Seq[Expression]): Seq[Expression] = {
    if (dataFilters.isEmpty) {
      return Nil
    }

    val output = relation.output
    val partitionSchema = identityPartitionSchema(relation)
    if (partitionSchema.isEmpty) {
      return Nil
    }
    // Cheap guard: skip the (more expensive) analysis unless a partition column is generated.
    if (!partitionSchema.exists(GeneratedColumn.isGeneratedColumn)) {
      return Nil
    }

    val fullSchema = fromAttributes(output)

    val rawExpressions = getOptimizablePartitionExpressions(spark, fullSchema, partitionSchema)
    if (rawExpressions.isEmpty) {
      return Nil
    }
    val optimizablePartitionExpressions =
      if (spark.sessionState.conf.caseSensitiveAnalysis) {
        rawExpressions
      } else {
        CaseInsensitiveMap(rawExpressions)
      }

    // Put the column on the left and the literal on the right.
    def preprocess(filter: Expression): Expression = filter match {
      case LessThan(lit: Literal, e: Expression) => GreaterThan(e, lit)
      case LessThanOrEqual(lit: Literal, e: Expression) => GreaterThanOrEqual(e, lit)
      case EqualTo(lit: Literal, e: Expression) => EqualTo(e, lit)
      case GreaterThan(lit: Literal, e: Expression) => LessThan(e, lit)
      case GreaterThanOrEqual(lit: Literal, e: Expression) => LessThanOrEqual(e, lit)
      case e => e
    }

    def toPartitionFilter(
        nameParts: Seq[String],
        func: OptimizablePartitionExpression => Option[Expression]): Seq[Expression] = {
      optimizablePartitionExpressions.get(createFieldPath(nameParts)).toSeq.flatMap { exprs =>
        exprs.flatMap(expr => func(expr))
      }
    }

    val partitionFilters = dataFilters.flatMap { filter =>
      preprocess(filter) match {
        case LessThan(ExtractBaseColumn(nameParts, _), lit: Literal) =>
          toPartitionFilter(nameParts, _.lessThan(lit))
        case LessThanOrEqual(ExtractBaseColumn(nameParts, _), lit: Literal) =>
          toPartitionFilter(nameParts, _.lessThanOrEqual(lit))
        case EqualTo(ExtractBaseColumn(nameParts, _), lit: Literal) =>
          toPartitionFilter(nameParts, _.equalTo(lit))
        case GreaterThan(ExtractBaseColumn(nameParts, _), lit: Literal) =>
          toPartitionFilter(nameParts, _.greaterThan(lit))
        case GreaterThanOrEqual(ExtractBaseColumn(nameParts, _), lit: Literal) =>
          toPartitionFilter(nameParts, _.greaterThanOrEqual(lit))
        case IsNull(ExtractBaseColumn(nameParts, _)) =>
          toPartitionFilter(nameParts, _.isNull())
        case _ => Nil
      }
    }

    resolveAndCoerce(spark, partitionFilters, output)
  }

  /**
   * Analyzes the derived partition filters against the relation output so that partition column
   * references are resolved and the expressions are type coerced. The derived filters reference
   * only partition columns that exist in `output`, so they are always expected to resolve; a
   * failure indicates a bug and is surfaced as an internal error.
   */
  private def resolveAndCoerce(
      spark: SparkSession,
      filters: Seq[Expression],
      output: Seq[AttributeReference]): Seq[Expression] = {
    if (filters.isEmpty) {
      return Nil
    }
    val plan = Project(filters.map(f => Alias(f, "gcpf")()), LocalRelation(output))
    spark.sessionState.analyzer.execute(plan) match {
      case Project(projectList, _) =>
        projectList.map {
          case Alias(child, _) if child.resolved => child
          case other =>
            throw SparkException.internalError(
              s"Failed to resolve derived generated column partition filter: $other")
        }
      case other =>
        throw SparkException.internalError(
          s"Expected a Project when resolving derived partition filters but got $other")
    }
  }

  /**
   * Builds a map from a base column path to the [[OptimizablePartitionExpression]]s of the
   * partition columns generated from that base column. The generation expressions are parsed and
   * analyzed against `schema` to normalize their shape (e.g. inserting casts) before matching.
   */
  private[v2] def getOptimizablePartitionExpressions(
      spark: SparkSession,
      schema: StructType,
      partitionSchema: StructType): Map[String, Seq[OptimizablePartitionExpression]] = {
    val partitionGenerationExprs = partitionSchema.flatMap { col =>
      GeneratedColumn.getGenerationExpression(col).map { exprStr =>
        Alias(spark.sessionState.sqlParser.parseExpression(exprStr), col.name)()
      }
    }
    if (partitionGenerationExprs.isEmpty) {
      return Map.empty
    }

    val resolver = spark.sessionState.analyzer.resolver
    val nameNormalizer: String => String =
      if (spark.sessionState.conf.caseSensitiveAnalysis) identity else _.toLowerCase(Locale.ROOT)

    def createExpr(nameParts: Seq[String])(func: => OptimizablePartitionExpression):
        Option[(String, OptimizablePartitionExpression)] = {
      if (schema.findNestedField(nameParts, resolver = resolver).isDefined) {
        Some(nameNormalizer(createFieldPath(nameParts)) -> func)
      } else {
        None
      }
    }

    // Analyze the parsed generation expressions against the table schema so that column references
    // are resolved and implicit casts / type coercion are applied. This normalizes each expression
    // into the canonical, coerced shape that the pattern match below relies on.
    val analyzed = spark.sessionState.analyzer.execute(
      Project(partitionGenerationExprs, LocalRelation(toAttributes(schema))))

    val extractedPartitionExprs = analyzed match {
      case Project(exprs, _) =>
        exprs.flatMap {
          case Alias(expr, partColName) =>
            expr match {
              case Cast(ExtractBaseColumn(name, TimestampType), DateType, _, _) =>
                createExpr(name)(DatePartitionExpr(partColName))
              case Cast(ExtractBaseColumn(name, DateType), DateType, _, _) =>
                createExpr(name)(DatePartitionExpr(partColName))
              case Year(ExtractBaseColumn(name, DateType)) =>
                createExpr(name)(YearPartitionExpr(partColName))
              case Year(Cast(ExtractBaseColumn(name, TimestampType), DateType, _, _)) =>
                createExpr(name)(YearPartitionExpr(partColName))
              case Year(Cast(ExtractBaseColumn(name, DateType), DateType, _, _)) =>
                createExpr(name)(YearPartitionExpr(partColName))
              case Month(Cast(ExtractBaseColumn(name, TimestampType), DateType, _, _)) =>
                createExpr(name)(MonthPartitionExpr(partColName))
              case DateFormatClass(
                  Cast(ExtractBaseColumn(name, DateType), TimestampType, _, _),
                  StringLiteral(format), _) =>
                format match {
                  case DATE_FORMAT_YEAR_MONTH =>
                    createExpr(name)(DateFormatPartitionExpr(partColName, DATE_FORMAT_YEAR_MONTH))
                  case _ => None
                }
              case DateFormatClass(
                  ExtractBaseColumn(name, TimestampType), StringLiteral(format), _) =>
                format match {
                  case DATE_FORMAT_YEAR_MONTH =>
                    createExpr(name)(DateFormatPartitionExpr(partColName, DATE_FORMAT_YEAR_MONTH))
                  case DATE_FORMAT_YEAR_MONTH_DAY =>
                    createExpr(name)(
                      DateFormatPartitionExpr(partColName, DATE_FORMAT_YEAR_MONTH_DAY))
                  case DATE_FORMAT_YEAR_MONTH_DAY_HOUR =>
                    createExpr(name)(
                      DateFormatPartitionExpr(partColName, DATE_FORMAT_YEAR_MONTH_DAY_HOUR))
                  case _ => None
                }
              case DayOfMonth(Cast(ExtractBaseColumn(name, TimestampType), DateType, _, _)) =>
                createExpr(name)(DayPartitionExpr(partColName))
              case Hour(ExtractBaseColumn(name, TimestampType), _) =>
                createExpr(name)(HourPartitionExpr(partColName))
              case Substring(
                  ExtractBaseColumn(name, StringType), IntegerLiteral(pos), IntegerLiteral(len)) =>
                createExpr(name)(SubstringPartitionExpr(partColName, pos, len))
              case TruncTimestamp(
                  StringLiteral(format), ExtractBaseColumn(name, TimestampType), _) =>
                createExpr(name)(TimestampTruncPartitionExpr(format, partColName))
              case TruncTimestamp(
                  StringLiteral(format),
                  Cast(ExtractBaseColumn(name, DateType), TimestampType, _, _), _) =>
                createExpr(name)(TimestampTruncPartitionExpr(format, partColName))
              case ExtractBaseColumn(name, _) =>
                createExpr(name)(IdentityPartitionExpr(partColName))
              case TruncDate(ExtractBaseColumn(name, DateType), StringLiteral(format)) =>
                createExpr(name)(TruncDatePartitionExpr(partColName, format))
              case TruncDate(
                  Cast(ExtractBaseColumn(name, TimestampType | StringType), DateType, _, _),
                  StringLiteral(format)) =>
                createExpr(name)(TruncDatePartitionExpr(partColName, format))
              case _ => None
            }
          case other =>
            // Should not happen since we wrap every generation expression in an Alias above.
            throw SparkException.internalError(
              s"Expected an Alias when analyzing generation expressions but got $other")
        }
      case other =>
        // Should not happen since analyzing a Project always returns a Project.
        throw SparkException.internalError(
          s"Expected a Project when analyzing generation expressions but got $other")
    }

    extractedPartitionExprs.groupBy(_._1).map { case (name, group) =>
      val mergedExprs = mergePartitionExpressionsIfPossible(group.map(_._2))
      if (log.isDebugEnabled) {
        logDebug(s"Optimizable partition expressions for column $name:")
        mergedExprs.foreach(expr => logDebug(expr.toString))
      }
      name -> mergedExprs
    }
  }

  /**
   * Merges separate YEAR/MONTH/DAY/HOUR partition expressions on the same base column into a single
   * composite expression, which allows deriving more selective partition filters.
   */
  private def mergePartitionExpressionsIfPossible(
      exprs: Seq[OptimizablePartitionExpression]): Seq[OptimizablePartitionExpression] = {
    def isRedundantPartitionExpr(f: OptimizablePartitionExpression): Boolean = {
      f.isInstanceOf[YearPartitionExpr] ||
        f.isInstanceOf[MonthPartitionExpr] ||
        f.isInstanceOf[DayPartitionExpr] ||
        f.isInstanceOf[HourPartitionExpr]
    }

    val year = exprs.collect { case y: YearPartitionExpr => y }.headOption
    val month = exprs.collect { case m: MonthPartitionExpr => m }.headOption
    val day = exprs.collect { case d: DayPartitionExpr => d }.headOption
    val hour = exprs.collect { case h: HourPartitionExpr => h }.headOption
    (year ++ month ++ day ++ hour).toSeq match {
      case Seq(y: YearPartitionExpr, m: MonthPartitionExpr, d: DayPartitionExpr,
          h: HourPartitionExpr) =>
        exprs.filterNot(isRedundantPartitionExpr) :+
          YearMonthDayHourPartitionExpr(y.yearPart, m.monthPart, d.dayPart, h.hourPart)
      case Seq(y: YearPartitionExpr, m: MonthPartitionExpr, d: DayPartitionExpr) =>
        exprs.filterNot(isRedundantPartitionExpr) :+
          YearMonthDayPartitionExpr(y.yearPart, m.monthPart, d.dayPart)
      case Seq(y: YearPartitionExpr, m: MonthPartitionExpr) =>
        exprs.filterNot(isRedundantPartitionExpr) :+
          YearMonthPartitionExpr(y.yearPart, m.monthPart)
      case _ =>
        exprs
    }
  }
}
