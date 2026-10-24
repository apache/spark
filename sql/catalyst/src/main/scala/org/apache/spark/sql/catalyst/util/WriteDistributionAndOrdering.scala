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

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.{Literal => CatalystLiteral}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.CreateTable
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog, TableCatalogCapability, WriteDistributionMode}
import org.apache.spark.sql.connector.expressions.{Expression, GeneralScalarExpression, GetArrayItem, IdentityTransform, Literal, NamedReference, SortOrder, Transform, VariantGet}
import org.apache.spark.sql.connector.expressions.filter.PartitionPredicate
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.connector.{ExpressionWithToString, ToStringSQLBuilder}
import org.apache.spark.sql.types.{DataType, FloatType, StructField, StructType, TimestampLTZNanosType, TimestampType}
import org.apache.spark.unsafe.types.TimestampNanosVal
import org.apache.spark.util.ArrayImplicits._

/**
 * Utility methods for the create-time write distribution and ordering requested by
 * `CREATE`/`REPLACE TABLE ... DISTRIBUTED BY PARTITION / [LOCALLY] ORDERED BY`.
 */
object WriteDistributionAndOrdering {

  // Runs a call into a connector's expression or literal, or into the parser on text built from
  // them. `NonFatal` does not cover a `LinkageError`, from a connector class whose dependency is
  // missing, or a `StackOverflowError`, from a deeply nested expression, and neither should fail
  // DESCRIBE or SHOW CREATE TABLE.
  private def attempt[T](body: => T): Try[T] = {
    try Success(body) catch {
      case NonFatal(e) => Failure(e)
      case e @ (_: LinkageError | _: StackOverflowError) => Failure(e)
    }
  }

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

  /** True when `catalog` accepts a create-time write distribution and ordering. */
  def supportsWriteDistributionAndOrdering(catalog: TableCatalog): Boolean = {
    catalog.capabilities().contains(
      TableCatalogCapability.SUPPORTS_CREATE_TABLE_WITH_WRITE_DISTRIBUTION_AND_ORDERING)
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
        !supportsWriteDistributionAndOrdering(catalog)) {
      throw QueryCompilationErrors.unsupportedTableOperationError(
        catalog, ident, s"$operation ... $CLAUSES")
    }
  }

  /**
   * Renders a requested sort key as SQL, for DESCRIBE. When a connector expression in it fails to
   * render, the sort order is rendered with its own `describe`, and when that fails too, with
   * whichever of the class name of its key, its direction and its null ordering can be read, or
   * with its own class name when none can.
   */
  def describeSortOrder(sortOrder: SortOrder): String = {
    attempt(sortOrderToSQL(sortOrder, quoteIfNeeded)).orElse(attempt(sortOrder.describe))
      .getOrElse(describeUnrenderable(sortOrder))
  }

  private def describeUnrenderable(sortOrder: SortOrder): String = {
    val parts = Seq(
      attempt(sortOrder.expression().getClass.getName),
      attempt(sortOrder.direction().toString),
      attempt(sortOrder.nullOrdering().toString)).flatMap(_.toOption)
    if (parts.nonEmpty) parts.mkString(" ") else Option(sortOrder).fold("null")(_.getClass.getName)
  }

  /**
   * Renders the declared write distribution and ordering as `DISTRIBUTED BY PARTITION`,
   * `[LOCALLY] ORDERED BY` or `UNORDERED` clauses for SHOW CREATE TABLE. Returns None when the
   * pair has no clause form, such as `HASH` without partitioning, when a connector expression in
   * the ordering fails to render, or when `replay`, such as [[replayWith]], does not give back the
   * same pair with every referenced column in `schema`. Names are quoted only where needed, and on
   * a second try all of them, which a name that is a reserved keyword under
   * `spark.sql.ansi.enforceReservedKeywords` needs.
   */
  def writeClausesSQL(
      writeDistributionMode: WriteDistributionMode,
      writeOrdering: Seq[SortOrder],
      partitioning: Seq[Transform],
      schema: StructType,
      replay: String => Option[(WriteDistributionMode, Seq[SortOrder])]): Option[String] = {
    lazy val partitioned = hasPartitioning(partitioning)
    Seq[String => String](quoteIfNeeded, quoteIdentifier).iterator.flatMap { quote =>
      attempt {
        val orderBy = if (writeOrdering.nonEmpty) {
          Some(writeOrdering.map(sortOrderToSQL(_, quote)).mkString("ORDERED BY (", ", ", ")"))
        } else {
          None
        }
        (writeDistributionMode, orderBy) match {
          case (WriteDistributionMode.HASH, Some(o)) if partitioned =>
            Some(s"DISTRIBUTED BY PARTITION $o")
          case (WriteDistributionMode.HASH, None) if partitioned =>
            Some("DISTRIBUTED BY PARTITION")
          case (WriteDistributionMode.RANGE, Some(o)) => Some(o)
          case (WriteDistributionMode.NONE, Some(o)) => Some(s"LOCALLY $o")
          case (WriteDistributionMode.NONE, None) => Some("UNORDERED")
          case _ => None
        }
      }.toOption.flatten
    }.find { clauses =>
      attempt(replay(clauses).exists { case (mode, ordering) =>
        mode == writeDistributionMode && sameSortOrders(ordering, writeOrdering) &&
          unknownReferences(schema, ordering).isEmpty
      }).getOrElse(false)
    }
  }

  /**
   * Parses `clauses` with `parser` in a minimal statement whose parse depends only on them, and
   * returns the write distribution and ordering it declares, or None when it does not parse. The
   * statement's partitioning is there only so that the parser accepts `DISTRIBUTED BY PARTITION`.
   */
  def replayWith(parser: ParserInterface)(
      clauses: String): Option[(WriteDistributionMode, Seq[SortOrder])] = {
    attempt(parser.parsePlan(s"CREATE TABLE t USING foo PARTITIONED BY (p) $clauses")).toOption
      .flatMap {
        case c: CreateTable => Some((c.writeDistributionMode, c.writeOrdering))
        case _ => None
      }
  }

  /**
   * The columns that the keys of a CREATE/REPLACE TABLE write ordering reference but `schema`
   * does not have, once each and in declaration order. A path through a field that is not a
   * struct is not in the schema either.
   */
  def unknownReferences(schema: StructType, writeOrdering: Seq[SortOrder]): Seq[Seq[String]] = {
    writeOrdering
      .flatMap(_.expression().references().map(_.fieldNames().toImmutableArraySeq))
      .distinct
      .filter(column => findColumn(schema, column).isEmpty)
  }

  /**
   * Finds the column at path `column` in `schema`, matching names with `resolver`, as its path
   * in the schema's spelling and its field. A path through a field that is not a struct is not
   * in the schema either, although `findNestedField` throws for it. `PreprocessTableCreation`
   * normalizes with the session resolver and CheckAnalysis rejects with an exact match, so both
   * look a column up here.
   */
  def findColumn(
      schema: StructType,
      column: Seq[String],
      resolver: Resolver = _ == _): Option[(Seq[String], StructField)] = {
    Try(schema.findNestedField(column, resolver = resolver)).toOption.flatten
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
    case IdentityTransform(ref) => ref
    case other => other
  }

  private def keyOf(e: Expression): Any = e match {
    case r: NamedReference => r.fieldNames().toSeq
    case l: Literal[_] => attempt(CatalystLiteral.create(l.value, l.dataType)).toOption
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
      attempt(literalToSQL(CatalystLiteral.create(l.value, l.dataType)))
        .getOrElse(describeLiteral(l))
    case o: SortOrder => sortOrderToSQL(o, quote)
    case t: Transform =>
      t.arguments().map(toSQL(_, quote)).mkString(s"${quote(t.name)}(", ", ", ")")
    case _: ExpressionWithToString => new DescribingSQLBuilder(quote).build(e)
    case other => attempt(other.describe).getOrElse(childrenToSQL(other, quote))
  }

  private def childrenToSQL(e: Expression, quote: String => String): String = {
    val name = e match {
      case g: GeneralScalarExpression => g.name
      case _ => e.getClass.getSimpleName
    }
    e.children().map(toSQL(_, quote)).mkString(s"$name(", ", ", ")")
  }

  // `ToStringSQLBuilder` renders literals and references with their `toString`, which does not
  // keep a literal's type and, for a connector's own reference, may not quote a name. It renders
  // the children of `GetArrayItem` and `VariantGet` through their `toString` too. This builder
  // renders all of them with `toSQL`, and a transform, which `ToStringSQLBuilder` does not know,
  // as `toSQL` does. A node it cannot render, including a `PartitionPredicate` whose `describe` is
  // the default `toString` that recurses in `ToStringSQLBuilder`, is rendered from its children,
  // so that a parent keeps its own rendering.
  private class DescribingSQLBuilder(quote: String => String) extends ToStringSQLBuilder {
    override def build(expr: Expression): String = {
      if (expr == null) "null" else attempt(super.build(expr)).getOrElse(childrenToSQL(expr, quote))
    }

    override protected def visitLiteral(literal: Literal[_]): String = toSQL(literal, quote)

    override protected def visitNamedReference(ref: NamedReference): String = toSQL(ref, quote)

    override protected def visitGetArrayItem(item: GetArrayItem): String = {
      s"${build(item.childArray)}[${build(item.ordinal)}]"
    }

    override protected def visitVariantGet(variantGet: VariantGet): String = {
      val funcName = if (variantGet.failOnError()) "variant_get" else "try_variant_get"
      val tz = Option(variantGet.timeZoneId()).map(z => s", tz=$z").getOrElse("")
      s"$funcName(${build(variantGet.child())}, " +
        s"'${Option(variantGet.path()).map(escapeSingleQuotedString).orNull}', " +
        s"${variantGet.targetType().catalogString}$tz)"
    }

    override protected def visitPartitionPredicate(predicate: PartitionPredicate): String = {
      childrenToSQL(predicate, quote)
    }

    override protected def visitCast(
        expr: String, exprDataType: DataType, dataType: DataType): String = {
      s"CAST($expr AS ${dataType.sql})"
    }

    // `toSQL` renders an `ExpressionWithToString` with this builder, so one that reaches this
    // method has to be rendered from its children to avoid recursing; any other node is a
    // connector's own expression, which `toSQL` renders with its `describe`.
    override protected def visitUnexpectedExpr(expr: Expression): String = expr match {
      case t: Transform => toSQL(t, quote)
      case _: ExpressionWithToString => childrenToSQL(expr, quote)
      case _ => toSQL(expr, quote)
    }
  }

  // `LiteralValue.describe` also rejects a value that does not match its type.
  private def describeLiteral(l: Literal[_]): String = {
    attempt(l.describe).getOrElse(String.valueOf(l.value))
  }

  // Catalyst renders a FLOAT as a CAST, which the parser does not accept here, so a finite FLOAT
  // is rendered as `<v>F`. A TIMESTAMP and a nanosecond TIMESTAMP_LTZ are rendered in UTC with an
  // explicit offset, so that they do not depend on the session time zone or timestamp type.
  private def literalToSQL(l: CatalystLiteral): String = (l.value, l.dataType) match {
    case (f: Float, FloatType) if java.lang.Float.isFinite(f) => s"${f}F"
    case (micros: Long, TimestampType) =>
      s"TIMESTAMP_LTZ '${utcFormatter.format(micros)}Z'"
    case (nanos: TimestampNanosVal, t: TimestampLTZNanosType) =>
      val utc = utcFormatter.formatNanos(nanos, t.precision)
      s"TIMESTAMP_LTZ '${CatalystLiteral.padToNanosPrecision(utc, t.precision)}Z'"
    case _ => l.sql
  }

  private def utcFormatter = TimestampFormatter.getFractionFormatter(ZoneOffset.UTC)
}
