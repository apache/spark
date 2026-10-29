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
package org.apache.spark.sql.execution.datasources.v2.columnar

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.catalyst.util.quoteIfNeeded
import org.apache.spark.sql.connector.expressions.{Expression, LiteralValue, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * The values that a column can have in the rows that match the filters of a query: null if
 * `nullAllowed`, NaN if `nanAllowed`, for a float or double column, and the other values in one
 * of the `ranges`, or any of them without ranges. A data source can skip the data whose statistics
 * show that it has no value in the domain of a column, but never a row whose values are in their
 * domains: Spark evaluates the filters after reading anyway.
 *
 * @param path the index of the column in the read schema, and then the index of the field in each
 *             struct, for a field of a struct
 * @param name the names of the column and of its fields
 * @param ranges sorted ranges, which neither overlap nor touch, of the values other than null and
 *               NaN, in the internal representation of Spark. None if the column can have any of
 *               them, and empty if it can have none.
 */
case class ColumnDomain(
    path: Seq[Int],
    name: Seq[String],
    dataType: DataType,
    nullAllowed: Boolean,
    nanAllowed: Boolean,
    ranges: Option[Seq[ValueRange]]) {

  private def ordering: Ordering[Any] = PhysicalDataType.ordering(dataType)

  /** Whether the domain has the value, in the internal representation of Spark. */
  def contains(value: Any): Boolean = value match {
    case null => nullAllowed
    case f: Float if f.isNaN => nanAllowed
    case d: Double if d.isNaN => nanAllowed
    case _ => ranges.forall(_.exists(_.contains(value, ordering)))
  }

  override def toString: String = {
    def literal(value: Any): String = Literal(value, dataType).sql
    val values = ranges match {
      case None => Seq("ANY")
      case Some(ranges) => ranges.map {
        case ValueRange(Some(low), true, Some(high), true) if ordering.equiv(low, high) =>
          literal(low)
        case range =>
          (if (range.lowInclusive) "[" else "(") + range.low.map(literal).getOrElse("-inf") +
            ", " + range.high.map(literal).getOrElse("+inf") +
            (if (range.highInclusive) "]" else ")")
      }
    }
    val special = (if (nullAllowed) Seq("NULL") else Nil) ++ (if (nanAllowed) Seq("NaN") else Nil)
    val all = special ++ values
    s"${name.map(quoteIfNeeded).mkString(".")}: ${if (all.isEmpty) "NONE" else all.mkString(", ")}"
  }
}

/**
 * A range of values in the internal representation of Spark: the values above `low`, or equal to
 * it if `lowInclusive`, and below `high`, or equal to it if `highInclusive`. A bound that is None
 * is unbounded, and is not inclusive.
 */
case class ValueRange(low: Option[Any], lowInclusive: Boolean, high: Option[Any],
    highInclusive: Boolean) {

  def contains(value: Any, ordering: Ordering[Any]): Boolean = {
    low.forall { l =>
      val c = ordering.compare(value, l)
      c > 0 || (c == 0 && lowInclusive)
    } && high.forall { h =>
      val c = ordering.compare(value, h)
      c < 0 || (c == 0 && highInclusive)
    }
  }
}

/**
 * The domains that a query passes to a data source, and the predicates that each of them comes
 * from.
 *
 * @param domains the domains, at most one for each column, sorted by path
 * @param columns the paths of the columns that each predicate constrains
 */
case class ColumnDomains(domains: Seq[ColumnDomain], columns: Seq[Set[Seq[Int]]]) {

  /**
   * Returns the predicates that constrain only the columns whose domains are used, given whether
   * the data source uses each domain.
   */
  def pushedPredicates(predicates: Array[Predicate], used: Seq[Boolean]): Array[Predicate] = {
    val usedPaths = domains.zip(used).collect { case (domain, true) => domain.path }.toSet
    predicates.zip(columns).collect {
      case (predicate, paths) if paths.nonEmpty && paths.subsetOf(usedPaths) => predicate
    }
  }
}

/**
 * Derives the domains of the columns that the predicates of a query constrain, the ones that Data
 * Source V2 pushes to a scan, combined with AND. A domain is a superset of the values that a column
 * has in the rows for which all the predicates are true: Spark derives it from the comparisons of
 * a column with literals, IN, IS NULL, IS NOT NULL and STARTS_WITH, under AND, OR and NOT, with the
 * semantics of Spark SQL. Comparisons with null are null, NaN is equal to NaN and greater than any
 * other value, -0.0 is equal to 0.0, and strings and binary values are compared by their bytes, as
 * unsigned bytes. A predicate that cannot be expressed this way does not constrain any column. Only
 * the columns and the fields of structs of the types that a data source can compare get a domain:
 * not the strings of a non-binary collation, nor arrays, maps and structs.
 */
object ColumnDomains {

  // A column that a predicate constrains, with its type.
  private case class Column(path: Seq[Int], name: Seq[String], dataType: DataType) {
    val ordering: Ordering[Any] = PhysicalDataType.ordering(dataType)
    val isFloating: Boolean = dataType == FloatType || dataType == DoubleType
  }

  // The values that a column can have: see ColumnDomain.
  private case class Values(nullAllowed: Boolean, nanAllowed: Boolean,
      ranges: Option[Seq[ValueRange]])

  private val NoValue = Values(nullAllowed = false, nanAllowed = false, Some(Nil))
  private val OnlyNull = Values(nullAllowed = true, nanAllowed = false, Some(Nil))

  // The columns that the predicates constrain, with their values: a row can only make the
  // predicates true if each of these columns has one of the values. None when no row can.
  private type Constraint = Option[Map[Column, Values]]
  private val All: Constraint = Some(Map.empty)
  private val NoRow: Constraint = None

  /** Derives the domains of the predicates for a scan of the schema. */
  def derive(predicates: Seq[Predicate], schema: StructType): ColumnDomains = {
    val constraints = predicates.map(constraint(_, negated = false, schema))
    val columns = constraints.map(_.map(_.keySet.map(_.path)).getOrElse(Set.empty[Seq[Int]]))
    val domains = constraints.foldLeft(All)(and) match {
      case Some(values) =>
        values.toSeq.map { case (column, v) =>
          ColumnDomain(column.path, column.name, column.dataType, v.nullAllowed, v.nanAllowed,
            v.ranges)
        }.sortBy(_.path)(Ordering.Implicits.seqOrdering[Seq, Int])
      // A predicate that is always false, which Spark usually removes before it scans the data.
      case None => Nil
    }
    ColumnDomains(domains, columns)
  }

  // The rows for which the predicate is true, or false if negated: NOT moves to the leaves.
  private def constraint(predicate: Predicate, negated: Boolean, schema: StructType): Constraint = {
    def child(i: Int): Option[Predicate] = predicate.children().lift(i).collect {
      case p: Predicate => p
    }
    def both(combine: (Constraint, Constraint) => Constraint, negated: Boolean): Constraint = {
      (child(0), child(1)) match {
        case (Some(left), Some(right)) =>
          combine(constraint(left, negated, schema), constraint(right, negated, schema))
        case _ => All
      }
    }
    predicate.name() match {
      case "AND" => if (negated) both(or, negated = true) else both(and, negated = false)
      case "OR" => if (negated) both(and, negated = true) else both(or, negated = false)
      case "NOT" => child(0).map(constraint(_, !negated, schema)).getOrElse(All)
      case "ALWAYS_TRUE" => if (negated) NoRow else All
      case "ALWAYS_FALSE" => if (negated) All else NoRow
      case _ => leaf(predicate, negated, schema).getOrElse(All)
    }
  }

  private def leaf(
      predicate: Predicate,
      negated: Boolean,
      schema: StructType): Option[Constraint] = {
    val children = predicate.children()
    predicate.name() match {
      case "IS_NULL" | "IS_NOT_NULL" if children.length == 1 =>
        column(children(0), schema).map { col =>
          val isNull = (predicate.name() == "IS_NULL") != negated
          single(col, if (isNull) OnlyNull else anyValue(col))
        }
      case op @ ("=" | "<>" | "<=>" | "<" | "<=" | ">" | ">=") if children.length == 2 =>
        val operands = (column(children(0), schema), column(children(1), schema)) match {
          case (Some(col), _) => literal(children(1), col).map((col, _, op))
          case (_, Some(col)) => literal(children(0), col).map((col, _, flip(op)))
          case _ => None
        }
        operands.map { case (col, value, op) => compare(col, value, op, negated) }
      case "IN" if children.nonEmpty =>
        column(children(0), schema).flatMap { col =>
          val values = children.tail.map(literal(_, col))
          if (values.forall(_.isDefined)) Some(in(col, values.map(_.get).toSeq, negated)) else None
        }
      case "STARTS_WITH" if children.length == 2 =>
        column(children(0), schema).filter(_.dataType.isInstanceOf[StringType]).flatMap { col =>
          literal(children(1), col).map(prefix => startsWith(col, prefix, negated))
        }
      case _ => None
    }
  }

  private def flip(op: String): String = op match {
    case "<" => ">"
    case "<=" => ">="
    case ">" => "<"
    case ">=" => "<="
    case other => other
  }

  private def compare(column: Column, value: Any, op: String, negated: Boolean): Constraint = {
    // NOT (x <=> v) is true for null, unless v is null.
    val nullAllowed = op == "<=>" && negated && value != null
    if (value == null) {
      // A comparison with null is null, and so is its negation, but x <=> NULL is x IS NULL.
      if (op == "<=>") single(column, if (negated) anyValue(column) else OnlyNull)
      else single(column, NoValue)
    } else if (isNaN(value)) {
      // NaN is equal to NaN and greater than any other value: (NaN, other values).
      val (nan, others) = (op, negated) match {
        case ("=" | "<=>" | ">=", false) | ("<>" | "<", true) => (true, Some(Nil))
        case ("=" | "<=>" | ">=", true) | ("<>" | "<", false) => (false, None)
        case ("<=", false) | (">", true) => (true, None)
        case ("<=", true) | (">", false) => (false, Some(Nil))
        case _ => throw SparkException.internalError(s"Unexpected comparison $op.")
      }
      single(column, Values(nullAllowed, nan, others))
    } else {
      val v = normalize(value)
      // NaN is greater than v, and not equal to it.
      val (nan, ranges) = (op, negated) match {
        case ("=" | "<=>", false) | ("<>", true) => (false, Seq(point(v)))
        case ("=" | "<=>", true) | ("<>", false) =>
          (true, Seq(below(v, inclusive = false), above(v, inclusive = false)))
        case ("<", false) | (">=", true) => (false, Seq(below(v, inclusive = false)))
        case ("<=", false) | (">", true) => (false, Seq(below(v, inclusive = true)))
        case (">", false) | ("<=", true) => (true, Seq(above(v, inclusive = false)))
        case (">=", false) | ("<", true) => (true, Seq(above(v, inclusive = true)))
        case _ => throw SparkException.internalError(s"Unexpected comparison $op.")
      }
      single(column, Values(nullAllowed, nan, Some(ranges)))
    }
  }

  private def in(column: Column, values: Seq[Any], negated: Boolean): Constraint = {
    val nonNull = values.filter(_ != null)
    val hasNaN = nonNull.exists(isNaN)
    val points = nonNull.filterNot(isNaN).map(normalize).sorted(column.ordering)
      .foldLeft(List.empty[Any]) {
        case (last :: rest, v) if column.ordering.equiv(last, v) => last :: rest
        case (acc, v) => v :: acc
      }.reverse
    if (!negated) {
      single(column, Values(nullAllowed = false, hasNaN, Some(points.map(point))))
    } else if (nonNull.length < values.length) {
      // NOT (x IN (..., NULL)) is never true: it is false or null.
      single(column, NoValue)
    } else if (points.isEmpty) {
      single(column, Values(nullAllowed = false, !hasNaN, None))
    } else {
      // The values between the points.
      val bounds = None +: points.map(Some(_)) :+ None
      val gaps = bounds.zip(bounds.tail).map { case (low, high) =>
        ValueRange(low, lowInclusive = false, high = high, highInclusive = false)
      }
      single(column, Values(nullAllowed = false, !hasNaN, Some(gaps)))
    }
  }

  private def startsWith(column: Column, prefix: Any, negated: Boolean): Constraint = {
    (prefix, negated) match {
      case (null, _) => single(column, NoValue)
      // The strings that do not start with the prefix are not a range. A value whose bytes are not
      // valid UTF-8 could even be in the range of the strings that do.
      case (_, true) => single(column, anyValue(column))
      case (prefix: UTF8String, false) if prefix.numBytes() == 0 => single(column, anyValue(column))
      case (prefix: UTF8String, false) =>
        val range = ValueRange(Some(prefix), lowInclusive = true, high = nextPrefix(prefix),
          highInclusive = false)
        single(column, Values(nullAllowed = false, nanAllowed = false, Some(Seq(range))))
      case _ => All
    }
  }

  /**
   * Returns the smallest string that is greater than all the strings that start with the prefix,
   * in the order of their UTF-8 bytes, which is the order of their code points, or None if there
   * is none: the prefix with its last code point incremented, after dropping the trailing
   * U+10FFFF, the largest code point.
   */
  private[columnar] def nextPrefix(prefix: UTF8String): Option[UTF8String] = {
    val codePoints = prefix.toString.codePoints().toArray.toBuffer
    while (codePoints.nonEmpty && codePoints.last == Character.MAX_CODE_POINT) {
      codePoints.remove(codePoints.length - 1)
    }
    if (codePoints.isEmpty) {
      None
    } else {
      val last = codePoints.last + 1
      // Skips the surrogates, which are not code points of characters.
      codePoints(codePoints.length - 1) =
        if (last >= Character.MIN_SURROGATE && last <= Character.MAX_SURROGATE) {
          Character.MAX_SURROGATE + 1
        } else {
          last
        }
      Some(UTF8String.fromString(new String(codePoints.toArray, 0, codePoints.length)))
    }
  }

  private def single(column: Column, values: Values): Constraint = {
    // Only floating-point columns have NaN.
    val normalized = values.copy(nanAllowed = values.nanAllowed && column.isFloating)
    if (isAll(column, normalized)) All else Some(Map(column -> normalized))
  }

  private def point(v: Any): ValueRange =
    ValueRange(Some(v), lowInclusive = true, high = Some(v), highInclusive = true)

  private def below(v: Any, inclusive: Boolean): ValueRange =
    ValueRange(None, lowInclusive = false, high = Some(v), highInclusive = inclusive)

  private def above(v: Any, inclusive: Boolean): ValueRange =
    ValueRange(Some(v), lowInclusive = inclusive, high = None, highInclusive = false)

  private def anyValue(column: Column): Values = {
    Values(nullAllowed = false, nanAllowed = column.isFloating, None)
  }

  private def isAll(column: Column, values: Values): Boolean = {
    values.nullAllowed && values.ranges.isEmpty && (values.nanAllowed || !column.isFloating)
  }

  private def isEmpty(values: Values): Boolean = {
    !values.nullAllowed && !values.nanAllowed && values.ranges.exists(_.isEmpty)
  }

  // Whether no row can make the predicates true.
  private def matchesNoRow(constraint: Constraint): Boolean = {
    constraint.forall(_.values.exists(isEmpty))
  }

  private def and(left: Constraint, right: Constraint): Constraint = (left, right) match {
    case (Some(l), Some(r)) =>
      Some((l.keySet ++ r.keySet).map { column =>
        column -> ((l.get(column), r.get(column)) match {
          case (Some(a), Some(b)) => intersect(column, a, b)
          case (a, b) => a.orElse(b).get
        })
      }.toMap)
    case _ => NoRow
  }

  // A column that only one side constrains can have any value in the rows of the other side, so
  // only the columns that both sides constrain are constrained.
  private def or(left: Constraint, right: Constraint): Constraint = {
    if (matchesNoRow(left)) {
      right
    } else if (matchesNoRow(right)) {
      left
    } else {
      val (l, r) = (left.get, right.get)
      Some(l.keySet.intersect(r.keySet).map(column => column -> union(column, l(column), r(column)))
        .filterNot { case (column, values) => isAll(column, values) }.toMap)
    }
  }

  private def intersect(column: Column, a: Values, b: Values): Values = Values(
    a.nullAllowed && b.nullAllowed,
    a.nanAllowed && b.nanAllowed,
    (a.ranges, b.ranges) match {
      case (Some(x), Some(y)) => Some(intersectRanges(x, y, column.ordering))
      case (x, y) => x.orElse(y)
    })

  private def union(column: Column, a: Values, b: Values): Values = Values(
    a.nullAllowed || b.nullAllowed,
    a.nanAllowed || b.nanAllowed,
    (a.ranges, b.ranges) match {
      case (Some(x), Some(y)) =>
        unionRanges(x, y, column.ordering) match {
          // All the values.
          case Seq(ValueRange(None, _, None, _)) => None
          case ranges => Some(ranges)
        }
      case _ => None
    })

  // A negative value when `a` starts before `b`.
  private def compareLows(a: ValueRange, b: ValueRange, ordering: Ordering[Any]): Int = {
    (a.low, b.low) match {
      case (None, None) => 0
      case (None, _) => -1
      case (_, None) => 1
      case (Some(x), Some(y)) =>
        val c = ordering.compare(x, y)
        if (c != 0 || a.lowInclusive == b.lowInclusive) c else if (a.lowInclusive) -1 else 1
    }
  }

  // A negative value when `a` ends before `b`.
  private def compareHighs(a: ValueRange, b: ValueRange, ordering: Ordering[Any]): Int = {
    (a.high, b.high) match {
      case (None, None) => 0
      case (None, _) => 1
      case (_, None) => -1
      case (Some(x), Some(y)) =>
        val c = ordering.compare(x, y)
        if (c != 0 || a.highInclusive == b.highInclusive) c else if (a.highInclusive) 1 else -1
    }
  }

  private def isEmptyRange(range: ValueRange, ordering: Ordering[Any]): Boolean = {
    (range.low, range.high) match {
      case (Some(low), Some(high)) =>
        val c = ordering.compare(low, high)
        c > 0 || (c == 0 && !(range.lowInclusive && range.highInclusive))
      case _ => false
    }
  }

  // Intersects two lists of sorted ranges that neither overlap nor touch, by sweeping them.
  private def intersectRanges(
      a: Seq[ValueRange],
      b: Seq[ValueRange],
      ordering: Ordering[Any]): Seq[ValueRange] = {
    val result = ArrayBuffer.empty[ValueRange]
    var i = 0
    var j = 0
    while (i < a.length && j < b.length) {
      val (x, y) = (a(i), b(j))
      val low = if (compareLows(x, y, ordering) >= 0) x else y
      val high = if (compareHighs(x, y, ordering) <= 0) x else y
      val range = ValueRange(low.low, low.lowInclusive, high.high, high.highInclusive)
      if (!isEmptyRange(range, ordering)) {
        result += range
      }
      // Moves past the range that ends first.
      if (compareHighs(x, y, ordering) <= 0) i += 1 else j += 1
    }
    result.toSeq
  }

  // Merges two lists of sorted ranges, and the ranges that overlap or touch, such as [1, 2) and
  // [2, 3].
  private def unionRanges(
      a: Seq[ValueRange],
      b: Seq[ValueRange],
      ordering: Ordering[Any]): Seq[ValueRange] = {
    def touches(first: ValueRange, next: ValueRange): Boolean = (first.high, next.low) match {
      case (Some(high), Some(low)) =>
        val c = ordering.compare(low, high)
        c < 0 || (c == 0 && (first.highInclusive || next.lowInclusive))
      case _ => true
    }
    (a ++ b).sortWith(compareLows(_, _, ordering) < 0).foldLeft(List.empty[ValueRange]) {
      case (last :: rest, range) if touches(last, range) =>
        val high = if (compareHighs(last, range, ordering) >= 0) last else range
        last.copy(high = high.high, highInclusive = high.highInclusive) :: rest
      case (merged, range) => range :: merged
    }.reverse
  }

  // The column of a reference, if it has a type that a data source can compare.
  private def column(expression: Expression, schema: StructType): Option[Column] = {
    expression match {
      case reference: NamedReference =>
        val names = reference.fieldNames().toSeq
        names.foldLeft(Option((Seq.empty[Int], schema: DataType))) {
          case (Some((path, struct: StructType)), name) =>
            val index = struct.fieldNames.indexOf(name)
            if (index < 0) None else Some((path :+ index, struct.fields(index).dataType))
          case _ => None
        }.collect {
          case (path, dataType) if names.nonEmpty && isComparable(dataType) =>
            Column(path, names, dataType)
        }
      case _ => None
    }
  }

  private def isComparable(dataType: DataType): Boolean = dataType match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType |
         _: DecimalType | BinaryType | DateType | TimestampType | TimestampNTZType => true
    // A data source compares strings by their UTF-8 bytes.
    case s: StringType => s.isUTF8BinaryCollation
    case _ => false
  }

  /**
   * The value of a literal of the type of the column, or null, as Some. None for another
   * expression, a literal of another type, or a string that is not valid UTF-8, which an Arrow
   * array of strings cannot hold.
   */
  private def literal(expression: Expression, column: Column): Option[Any] = expression match {
    case LiteralValue(null, _) => Some(null)
    case LiteralValue(value: UTF8String, dataType) if dataType == column.dataType =>
      if (value.isValid) Some(value) else None
    case LiteralValue(value, dataType) if dataType == column.dataType => Some(value)
    case _ => None
  }

  private def isNaN(value: Any): Boolean = value match {
    case f: Float => f.isNaN
    case d: Double => d.isNaN
    case _ => false
  }

  // -0.0 is equal to 0.0, and a data source may order it before 0.0.
  private def normalize(value: Any): Any = value match {
    case f: Float if f == 0.0f => 0.0f
    case d: Double if d == 0.0d => 0.0d
    case other => other
  }
}
