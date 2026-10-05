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
import org.apache.spark.sql.connector.expressions.{Cast, Expression, GeneralScalarExpression, IdentityTransform, Literal, NamedReference, SortOrder, Transform}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{FloatType, StructType, TimestampType}
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
   * pair has no clause form, such as `HASH` without partitioning, or when `replay`, which parses a
   * statement carrying the clauses and returns the pair it declares, does not give back the same
   * pair. Names are quoted only where needed, and on a second try all of them, which reserved
   * keywords under `spark.sql.ansi.enforceReservedKeywords` need.
   */
  def writeClausesSQL(
      writeDistributionMode: WriteDistributionMode,
      writeOrdering: Seq[SortOrder],
      partitioning: Seq[Transform],
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
        mode == writeDistributionMode && sameSortOrders(ordering, writeOrdering)
      }
    }
  }

  /**
   * True when every column a sort key references is in `schema`, matched as `CheckAnalysis`
   * matches the ordering of a CREATE/REPLACE TABLE statement.
   */
  def referencesExist(schema: StructType, writeOrdering: Seq[SortOrder]): Boolean = {
    writeOrdering.flatMap(_.expression().references()).forall { ref =>
      ref.fieldNames().nonEmpty &&
        Try(schema.findNestedField(ref.fieldNames().toImmutableArraySeq)).toOption.flatten.isDefined
    }
  }

  /**
   * True when two orderings declare the same keys: the same column names, transform names and
   * argument structure, literal values and types, directions and null orderings.
   */
  def sameSortOrders(left: Seq[SortOrder], right: Seq[SortOrder]): Boolean = {
    left.length == right.length && left.zip(right).forall { case (l, r) =>
      l.direction() == r.direction() && l.nullOrdering() == r.nullOrdering() &&
        keyOf(l.expression()) == keyOf(r.expression())
    }
  }

  private def keyOf(e: Expression): Any = e match {
    case r: NamedReference => r.fieldNames().toSeq
    case l: Literal[_] => Try(CatalystLiteral.create(l.value, l.dataType)).toOption
    case t: IdentityTransform => keyOf(t.ref)
    case t: Transform => (t.name, t.arguments().toSeq.map(keyOf))
    case other => other
  }

  private def sortOrderToSQL(sortOrder: SortOrder, quote: String => String): String = {
    s"${toSQL(sortOrder.expression(), quote)} ${sortOrder.direction()} ${sortOrder.nullOrdering()}"
  }

  // `describe` prints a literal's internal value, e.g. `0` for DATE '1970-01-01', so literals are
  // rendered through Catalyst to keep their type. Catalyst renders a FLOAT as a CAST, which the
  // parser does not accept here, so a finite FLOAT is rendered as `<v>F` instead, and a TIMESTAMP
  // in UTC with an explicit offset so that it does not depend on the session time zone or
  // timestamp type. A literal Catalyst cannot represent falls back to `describe`. Other connector
  // expressions are rendered from their children: `describe` on one that
  // `V2ExpressionSQLBuilder` does not know recurses without end. PARTITIONED BY in SHOW CREATE
  // TABLE and the `Part N` rows of DESCRIBE render transforms with `describe`, so the same
  // transform can print differently there.
  private def toSQL(e: Expression, quote: String => String): String = e match {
    case r: NamedReference => r.fieldNames.map(quote).mkString(".")
    case l: Literal[_] =>
      try literalToSQL(CatalystLiteral.create(l.value, l.dataType)) catch {
        case NonFatal(_) => describeLiteral(l)
      }
    case t: IdentityTransform => toSQL(t.ref, quote)
    case t: Transform =>
      t.arguments().map(toSQL(_, quote)).mkString(s"${quote(t.name)}(", ", ", ")")
    case c: Cast => s"CAST(${toSQL(c.expression(), quote)} AS ${c.dataType().sql})"
    case g: GeneralScalarExpression =>
      g.children().map(toSQL(_, quote)).mkString(s"${g.name}(", ", ", ")")
    case other =>
      other.children().map(toSQL(_, quote)).mkString(s"${other.getClass.getSimpleName}(", ", ", ")")
  }

  // `LiteralValue.describe` also rejects a value that does not match its type.
  private def describeLiteral(l: Literal[_]): String = {
    try l.describe catch {
      case NonFatal(_) => String.valueOf(l.value)
    }
  }

  private def literalToSQL(l: CatalystLiteral): String = (l.value, l.dataType) match {
    case (f: Float, FloatType) if java.lang.Float.isFinite(f) => s"${f}F"
    case (micros: Long, TimestampType) =>
      val utc = TimestampFormatter.getFractionFormatter(ZoneOffset.UTC).format(micros)
      s"TIMESTAMP_LTZ '${utc}Z'"
    case _ => l.sql
  }
}
