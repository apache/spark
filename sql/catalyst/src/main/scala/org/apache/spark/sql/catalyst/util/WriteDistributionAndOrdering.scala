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

package org.apache.spark.sql.catalyst.util

import java.time.ZoneOffset

import scala.util.Try
import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.expressions.{Literal => CatalystLiteral}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog, TableCatalogCapability, WriteDistributionMode}
import org.apache.spark.sql.connector.expressions.{Expression, GeneralScalarExpression, GetArrayItem, IdentityTransform, Literal, NamedReference, SortOrder, Transform, VariantGet}
import org.apache.spark.sql.connector.expressions.filter.PartitionPredicate
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.connector.{ExpressionWithToString, ToStringSQLBuilder}
import org.apache.spark.sql.types.{FloatType, StructType, TimestampLTZNanosType, TimestampType, TimeType}
import org.apache.spark.unsafe.types.TimestampNanosVal
import org.apache.spark.util.ArrayImplicits._

/**
 * Utility methods for the create-time write distribution and ordering requested by
 * `CREATE`/`REPLACE TABLE ... DISTRIBUTED BY PARTITION / [LOCALLY] ORDERED BY`.
 */
object WriteDistributionAndOrdering {

  /** Names the write clauses in an error's operation, as in `CREATE TABLE ... <CLAUSES>`. */
  val CLAUSES = "DISTRIBUTED BY/ORDERED BY/UNORDERED"

  /**
   * True when a CREATE/REPLACE TABLE statement asked for a write distribution or ordering.
   * `UNORDERED` counts as a request.
   */
  def isRequested(
      writeDistributionMode: WriteDistributionMode,
      writeOrdering: Seq[SortOrder]): Boolean = {
    writeDistributionMode != null || writeOrdering.nonEmpty
  }

  /**
   * True when the transforms define a partitioning that `DISTRIBUTED BY PARTITION` can distribute
   * by. Bucketing counts; a `cluster_by` transform, from CLUSTER BY or reported by a connector,
   * does not. Matches by name only, since a connector's `cluster_by` may take any arguments.
   */
  def hasPartitioning(partitioning: Seq[Transform]): Boolean = {
    partitioning.exists(_.name != "cluster_by")
  }

  /**
   * Rejects a create-time write distribution or ordering that the catalog has not advertised
   * support for, before anything is created or dropped.
   */
  def validateCatalogForWriteDistributionAndOrdering(
      catalog: TableCatalog,
      ident: Identifier,
      operation: String,
      writeDistributionMode: WriteDistributionMode,
      writeOrdering: Seq[SortOrder]): Unit = {
    if (isRequested(writeDistributionMode, writeOrdering) &&
        !catalog.capabilities().contains(
          TableCatalogCapability.SUPPORTS_CREATE_TABLE_WITH_WRITE_DISTRIBUTION_AND_ORDERING)) {
      throw QueryCompilationErrors.unsupportedTableOperationError(
        catalog, ident, s"$operation ... $CLAUSES")
    }
  }

  /** Renders a requested sort key as SQL, for DESCRIBE. */
  def describeSortOrder(sortOrder: SortOrder): String = sortOrderToSQL(sortOrder, quoteIfNeeded)

  /**
   * Renders the declared write distribution and ordering as `DISTRIBUTED BY PARTITION`,
   * `[LOCALLY] ORDERED BY` or `UNORDERED` clauses for SHOW CREATE TABLE. Returns None when the
   * pair has no clause form, such as `HASH` without partitioning, or when `replay`, which parses
   * [[replayStatement]] for the clauses and returns the pair it declares, does not give back the
   * same pair with every referenced column in `schema`. Names are quoted only where needed, and on
   * a second try all of them, which reserved keywords under
   * `spark.sql.ansi.enforceReservedKeywords` need.
   */
  def writeClausesSQL(
      writeDistributionMode: WriteDistributionMode,
      writeOrdering: Seq[SortOrder],
      partitioning: Seq[Transform],
      schema: StructType,
      replay: String => Option[(WriteDistributionMode, Seq[SortOrder])]): Option[String] = {
    lazy val partitioned = hasPartitioning(partitioning)
    Seq[String => String](quoteIfNeeded, quoteIdentifier).iterator.flatMap { quote =>
      val orderBy = if (writeOrdering.nonEmpty) {
        Some(writeOrdering.map(sortOrderToSQL(_, quote)).mkString("ORDERED BY (", ", ", ")"))
      } else {
        None
      }
      (writeDistributionMode, orderBy) match {
        case (WriteDistributionMode.HASH, Some(o)) if partitioned =>
          Some(s"DISTRIBUTED BY PARTITION $o")
        case (WriteDistributionMode.HASH, None) if partitioned => Some("DISTRIBUTED BY PARTITION")
        case (WriteDistributionMode.RANGE, Some(o)) => Some(o)
        case (WriteDistributionMode.NONE, Some(o)) => Some(s"LOCALLY $o")
        case (WriteDistributionMode.NONE, None) => Some("UNORDERED")
        case _ => None
      }
    }.find { clauses =>
      replay(clauses).exists { case (mode, ordering) =>
        mode == writeDistributionMode && sameSortOrders(ordering, writeOrdering) &&
          referencesExist(schema, ordering)
      }
    }
  }

  /**
   * A minimal statement whose parse depends only on `clauses`. The partitioning is there only so
   * that the parser accepts `DISTRIBUTED BY PARTITION`.
   */
  def replayStatement(clauses: String): String =
    s"CREATE TABLE t USING foo PARTITIONED BY (p) $clauses"

  // Checks the parsed ordering exactly, which is at least as strict as CheckAnalysis on the
  // replayed statement.
  private def referencesExist(schema: StructType, writeOrdering: Seq[SortOrder]): Boolean = {
    writeOrdering.flatMap(_.expression().references()).forall { ref =>
      ref.fieldNames().nonEmpty &&
        Try(schema.findNestedField(ref.fieldNames().toImmutableArraySeq)).toOption.flatten.isDefined
    }
  }

  // True when two orderings declare the same keys: the same column names, transform names and
  // argument structure, literal values and types, directions and null orderings.
  private def sameSortOrders(left: Seq[SortOrder], right: Seq[SortOrder]): Boolean = {
    left.length == right.length && left.zip(right).forall { case (l, r) =>
      l.direction() == r.direction() && l.nullOrdering() == r.nullOrdering() &&
        keyOf(sortKey(l.expression())) == keyOf(sortKey(r.expression()))
    }
  }

  // A plain column is a key's reference itself or, only at the top of a key, `identity(col)`.
  private def sortKey(e: Expression): Expression = e match {
    case t: IdentityTransform => t.ref
    case other => other
  }

  private def keyOf(e: Expression): Any = e match {
    case r: NamedReference => r.fieldNames().toSeq
    case l: Literal[_] => Try(CatalystLiteral.create(l.value, l.dataType)).toOption
    case t: Transform => (t.name, t.arguments().toSeq.map(keyOf))
    case other => other
  }

  private def sortOrderToSQL(sortOrder: SortOrder, quote: String => String): String = {
    val key = toSQL(sortKey(sortOrder.expression()), quote)
    s"$key ${sortOrder.direction()} ${sortOrder.nullOrdering()}"
  }

  // `describe` prints a literal's internal value, e.g. `0` for DATE '1970-01-01', so literals are
  // rendered through Catalyst to keep their type, with the exceptions in `literalToSQL`. A literal
  // Catalyst cannot represent falls back to `describe`. PARTITIONED BY in SHOW CREATE TABLE and the
  // `Part N` rows of DESCRIBE render transforms with `describe`, so the same transform can print
  // differently there.
  private def toSQL(e: Expression, quote: String => String): String = e match {
    case null => "null"
    case r: NamedReference => r.fieldNames.map(quote).mkString(".")
    case l: Literal[_] =>
      try literalToSQL(CatalystLiteral.create(l.value, l.dataType)) catch {
        case NonFatal(_) => describeLiteral(l)
      }
    case o: SortOrder => sortOrderToSQL(o, quote)
    case t: Transform =>
      t.arguments().map(toSQL(_, quote)).mkString(s"${quote(t.name)}(", ", ", ")")
    case _: ExpressionWithToString =>
      Try(new DescribingSQLBuilder(quote).build(e)).getOrElse(childrenToSQL(e, quote))
    case other => Try(other.describe).getOrElse(childrenToSQL(other, quote))
  }

  private def childrenToSQL(e: Expression, quote: String => String): String = {
    val name = e match {
      case g: GeneralScalarExpression => g.name
      case _ => e.getClass.getSimpleName
    }
    e.children().map(toSQL(_, quote)).mkString(s"$name(", ", ", ")")
  }

  // `ToStringSQLBuilder` renders a few expressions through their children's `describe`, and for an
  // expression it does not know builds an error message with `describe`, which recurses without
  // end for a `GeneralScalarExpression` name it does not know. This builder renders every child
  // itself, and an expression it does not know from its children.
  private class DescribingSQLBuilder(quote: String => String) extends ToStringSQLBuilder {
    override protected def visitLiteral(literal: Literal[_]): String = toSQL(literal, quote)

    override protected def visitNamedReference(ref: NamedReference): String = toSQL(ref, quote)

    override protected def visitGetArrayItem(item: GetArrayItem): String = {
      s"${build(item.childArray)}[${build(item.ordinal)}]"
    }

    override protected def visitVariantGet(variantGet: VariantGet): String = {
      val funcName = if (variantGet.failOnError()) "variant_get" else "try_variant_get"
      val tz = Option(variantGet.timeZoneId()).map(z => s", tz=$z").getOrElse("")
      s"$funcName(${build(variantGet.child())}, '${variantGet.path()}', " +
        s"${variantGet.targetType().catalogString}$tz)"
    }

    override protected def visitPartitionPredicate(predicate: PartitionPredicate): String = {
      childrenToSQL(predicate, quote)
    }

    override protected def visitUnexpectedExpr(expr: Expression): String = {
      childrenToSQL(expr, quote)
    }
  }

  // `LiteralValue.describe` also rejects a value that does not match its type.
  private def describeLiteral(l: Literal[_]): String = {
    try l.describe catch {
      case NonFatal(_) => String.valueOf(l.value)
    }
  }

  // Catalyst renders a FLOAT as a CAST, which the parser does not accept here, so a finite FLOAT
  // is rendered as `<v>F`. A TIMESTAMP and a nanosecond TIMESTAMP_LTZ are rendered in UTC with an
  // explicit offset, so that they do not depend on the session time zone or timestamp type. The
  // parser takes the precision of a TIME from its fraction digits, so a TIME with more than
  // microsecond precision keeps its trailing zeros.
  private def literalToSQL(l: CatalystLiteral): String = (l.value, l.dataType) match {
    case (f: Float, FloatType) if java.lang.Float.isFinite(f) => s"${f}F"
    case (micros: Long, TimestampType) =>
      s"TIMESTAMP_LTZ '${utcFormatter.format(micros)}Z'"
    case (nanos: TimestampNanosVal, t: TimestampLTZNanosType) =>
      val utc = padFraction(utcFormatter.formatNanos(nanos, t.precision), t.precision)
      s"TIMESTAMP_LTZ '${utc}Z'"
    case (nanos: Long, t: TimeType) if t.precision > TimeType.MICROS_PRECISION =>
      s"TIME '${padFraction(new FractionTimeFormatter().format(nanos), t.precision)}'"
    case _ => l.sql
  }

  private def utcFormatter = TimestampFormatter.getFractionFormatter(ZoneOffset.UTC)

  private def padFraction(s: String, precision: Int): String = {
    val digits = s.indexOf('.') match {
      case -1 => 0
      case dot => s.length - dot - 1
    }
    (if (digits == 0) s"$s." else s) + "0" * (precision - digits)
  }
}
