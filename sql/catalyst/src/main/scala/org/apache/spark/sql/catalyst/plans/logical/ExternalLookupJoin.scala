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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSeq, AttributeSet,
  BinaryComparison, Cast, EqualNullSafe, EqualTo, Expression, ExprUtils,
  ExternalUserDefinedFunction, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual,
  NamedExpression, PythonUDF, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftAnti, LeftOuter, LeftSemi,
  QueryPlan}
import org.apache.spark.sql.catalyst.trees.TreePattern.{EXTERNAL_LOOKUP_HINT, EXTERNAL_LOOKUP_JOIN,
  TreePattern}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.read.{ExternalLookupReaderFactory, Scan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, CharType, DataType,
  DateType, DecimalType, IntegerType, LongType, ShortType, StringType, TimestampNTZType,
  TimestampType, VarcharType}

/**
 * Marks the plan below it as the lookup side that the `EXTERNAL_LOOKUP` hint asks an external
 * lookup join to read. The external lookup join rules consume it when they form the join.
 *
 * Join strategy hints are [[ResolvedHint]] nodes with a [[JoinStrategyHint]]. This hint is not,
 * because that representation cannot keep two of its guarantees:
 *  - A strategy hint that conflicts with it must fail the query. [[HintInfo]] holds one strategy,
 *    and `ResolveJoinStrategyHints` merges a strategy hint on an already hinted relation into the
 *    existing [[ResolvedHint]] with `HintInfo.merge`, which keeps one strategy and only warns.
 *  - It selects how the lookup side is read, not a physical join. `EliminateResolvedHint` moves
 *    every [[ResolvedHint]] it can attach into `Join.hint`, for physical join selection.
 */
case class ExternalLookupHint(child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  final override val nodePatterns: Seq[TreePattern] = Seq(EXTERNAL_LOOKUP_HINT)

  // As for a ResolvedHint, so that a hint that attaches to no join leaves the cached data that a
  // plan finds as it is without the hint. Cache substitution protects the lookup side of a lookup
  // join by identity, not by its canonical form.
  override def doCanonicalize(): LogicalPlan = child.canonicalized

  override protected def withNewChildInternal(newChild: LogicalPlan): ExternalLookupHint =
    copy(child = newChild)
}

/**
 * A lookup join retained through ordinary expression and operator optimization.
 *
 * @param requiredLookupOutput Lookup-side attributes still required above this join. They are
 *   part of this node's references, so column pruning keeps them on the lookup side.
 *   `ColumnPruning` narrows them to what the parent reads, and `PrepareExternalLookupJoin` prunes
 *   the lookup scan to them.
 */
case class ExternalLookupJoin(
    left: LogicalPlan,
    right: LogicalPlan,
    joinType: JoinType,
    condition: Option[Expression],
    requiredLookupOutput: Seq[Attribute]) extends BinaryNode {

  final override val nodePatterns: Seq[TreePattern] = Seq(EXTERNAL_LOOKUP_JOIN)

  override def output: Seq[Attribute] = joinType match {
    case Inner => left.output ++ right.output
    case LeftOuter => left.output ++ right.output.map(_.withNullability(true))
    case LeftSemi | LeftAnti => left.output
    case other => throw SparkException.internalError(s"Unexpected external lookup join type $other")
  }

  override def maxRows: Option[Long] = joinType match {
    case LeftSemi | LeftAnti => left.maxRows
    case _ => None
  }

  override protected def withNewChildrenInternal(
      newLeft: LogicalPlan,
      newRight: LogicalPlan): ExternalLookupJoin = copy(left = newLeft, right = newRight)
}

/**
 * The join-condition and lookup-side shapes that an external lookup join can run. Analysis,
 * lookup planning and the optimizer rules for lookup joins share them.
 */
object ExternalLookupJoinShapes {
  /**
   * A lookup candidate: a comparison of a lookup column with a value, written with the lookup
   * column on the left. `predicateName` is the name of the DSv2 `Predicate` that is offered to the
   * connector for it: the SQL operator of the comparison, as `V2ExpressionBuilder` names it.
   */
  case class LookupComparison(
      lookupField: Attribute,
      parameter: Expression,
      predicateName: String,
      nullSafe: Boolean)

  /**
   * `conjunct` of the condition of a lookup join as a lookup candidate: `=`, `<=>`, `<`, `<=`, `>`
   * or `>=` between a bare lookup column and a value, in either operand order. When the lookup
   * column is written on the right, the comparison is rebuilt with it on the left, so
   * `s.ts < t.event_time` is offered as `event_time > ?`.
   *
   * The value is the other operand, which Spark evaluates for each stream row to fill the
   * comparison's parameter. It may read only stream columns, and it may read none, as a literal
   * does. A value that reads lookup columns, as in `t.a = t.b`, cannot be a parameter.
   *
   * The lookup column must be of a type on which [[ExternalLookupKeyTypes]] lets Spark offer the
   * comparison, and not a CHAR or VARCHAR column. A comparison on another column is an ordinary
   * conjunct.
   *
   * @param streamOutput The output of the join's left side, the stream side.
   * @param lookupOutput The output of the join's right side, the lookup side.
   */
  def lookupComparison(
      conjunct: Expression,
      streamOutput: AttributeSet,
      lookupOutput: AttributeSet): Option[LookupComparison] = {
    def isLookupComparison(lookupField: Attribute, parameter: Expression): Boolean =
      lookupOutput.contains(lookupField) && parameter.references.subsetOf(streamOutput)
    conjunct match {
      case comparison: BinaryComparison if isLookupOperator(comparison) =>
        val nullSafe = comparison.isInstanceOf[EqualNullSafe]
        Seq(comparison, withOperandsSwapped(comparison)).collectFirst {
          case candidate @ BinaryComparison(lookupField: Attribute, parameter)
              if isLookupComparison(lookupField, parameter) &&
                isOfferedOn(comparison, lookupField, parameter) =>
            LookupComparison(lookupField, parameter, candidate.sqlOperator, nullSafe)
        }
      case _ => None
    }
  }

  private def isLookupOperator(comparison: BinaryComparison): Boolean = comparison match {
    case _: EqualTo | _: EqualNullSafe | _: LessThan | _: LessThanOrEqual | _: GreaterThan |
        _: GreaterThanOrEqual => true
    case _ => false
  }

  /**
   * Whether Spark offers the comparison on this lookup column, with this parameter.
   *
   * Spark evaluates every parameter on every stream row, before the other join conditions. That
   * is safe for an equality, as a hash join evaluates its keys on every row too. A range is a
   * join condition, which a join evaluates only after its keys match, so its parameter must be
   * one that cannot fail, as `cannotFail` decides.
   */
  private def isOfferedOn(
      comparison: BinaryComparison,
      lookupField: Attribute,
      parameter: Expression): Boolean = comparison match {
    case _: EqualTo | _: EqualNullSafe => ExternalLookupKeyTypes.supportsEquality(lookupField)
    case _ =>
      ExternalLookupKeyTypes.supportsRange(lookupField.dataType) && cannotFail(parameter)
  }

  /**
   * Whether a range parameter cannot fail: ExprUtils.canEvaluateUnconditionally accepts it, or it
   * is a widening cast of such a value between types on which Spark offers a range. Analysis adds
   * such a cast when the stream side has a narrower type than the lookup column, as an INT column
   * compared with a BIGINT key, and `Cast.canUpCast` ensures that it cannot fail.
   */
  private def cannotFail(parameter: Expression): Boolean = parameter match {
    case cast: Cast =>
      Cast.canUpCast(cast.child.dataType, cast.dataType) &&
        ExternalLookupKeyTypes.supportsRange(cast.child.dataType) &&
        ExternalLookupKeyTypes.supportsRange(cast.dataType) &&
        ExprUtils.canEvaluateUnconditionally(cast.child)
    case _ => ExprUtils.canEvaluateUnconditionally(parameter)
  }

  /** The same comparison with its operands swapped, so that `a < b` becomes `b > a`. */
  private def withOperandsSwapped(comparison: BinaryComparison): BinaryComparison =
    comparison match {
      case EqualTo(left, right) => EqualTo(right, left)
      case EqualNullSafe(left, right) => EqualNullSafe(right, left)
      case LessThan(left, right) => GreaterThan(right, left)
      case LessThanOrEqual(left, right) => GreaterThanOrEqual(right, left)
      case GreaterThan(left, right) => LessThan(right, left)
      case GreaterThanOrEqual(left, right) => LessThanOrEqual(right, left)
    }

  /**
   * Whether a lookup-side projection or filter can evaluate `expression` on the lookup results,
   * inside the join. Python and external UDFs and subqueries are planned as separate operators,
   * which a lookup-side stage cannot contain. A non-deterministic expression is not allowed
   * either: the join evaluates a stage on each returned row, so one stored row could get
   * different values for different stream rows, which a read of the table never does.
   */
  def canEvaluateInLookupStage(expression: Expression): Boolean =
    expression.deterministic && !expression.exists {
      case _: PythonUDF | _: ExternalUserDefinedFunction | _: SubqueryExpression => true
      case _ => false
    }
}

/** One optimized lookup-side operation, retained in evaluation order. */
sealed trait ExternalLookupTransform {
  def input: Seq[Attribute]
  def output: Seq[Attribute]
}

case class ExternalLookupProject(
    projectList: Seq[NamedExpression],
    input: Seq[Attribute]) extends ExternalLookupTransform {
  override def output: Seq[Attribute] = projectList.map(_.toAttribute)
}

case class ExternalLookupFilter(
    condition: Expression,
    input: Seq[Attribute]) extends ExternalLookupTransform {
  override def output: Seq[Attribute] = input
}

/**
 * A lookup join after its optimized lookup side has been converted to an external operation.
 *
 * @param lookupKeys The table column that each selected lookup predicate compares with the
 *   parameter at the same position. Spark checks each row that the connector returns against
 *   them, so the read schema must hold these columns.
 * @param lookupKeyPredicates The name of each selected lookup predicate, such as `=` or `<`,
 *   with the lookup column on the left. It tells how Spark checks the key.
 */
case class PreparedExternalLookupJoin(
    child: LogicalPlan,
    tableName: String,
    @transient scan: Scan,
    readerFactory: ExternalLookupReaderFactory,
    maxParameterRowsPerRequest: Int,
    lookupInput: Seq[Attribute],
    lookupOutput: Seq[Attribute],
    lookupTransforms: Seq[ExternalLookupTransform],
    lookupParameters: Seq[Expression],
    nullSafeParameters: Seq[Boolean],
    lookupKeys: Seq[Attribute],
    lookupKeyPredicates: Seq[String],
    joinType: JoinType,
    residualCondition: Option[Expression]) extends UnaryNode {

  override def output: Seq[Attribute] = joinType match {
    case Inner => child.output ++ lookupOutput
    case LeftOuter => child.output ++ lookupOutput.map(_.withNullability(true))
    case LeftSemi | LeftAnti => child.output
    case other => throw SparkException.internalError(s"Unexpected external lookup join type $other")
  }

  override def references: AttributeSet =
    AttributeSet(lookupParameters.flatMap(_.references)) ++
      residualCondition.map(_.references.intersect(child.outputSet))
        .getOrElse(AttributeSet.empty)

  override protected def withNewChildInternal(newChild: LogicalPlan): PreparedExternalLookupJoin =
    copy(child = newChild)

  // The lookup-side attributes are not in the child's output, so the default canonicalization
  // gives each top-level one the next id, whichever column it is: two joins that keep different
  // lookup columns of one type would canonicalize alike. Each is normalized by its place in the
  // child's output and the read schema instead.
  override protected def doCanonicalize(): LogicalPlan = {
    val attributes: AttributeSeq = child.output ++ lookupInput
    def normalize[T <: Expression](expression: T): T =
      QueryPlan.normalizeExpressions(expression, attributes)
    copy(
      child = child.canonicalized,
      lookupInput = lookupInput.map(normalize),
      lookupOutput = lookupOutput.map(normalize),
      lookupParameters = lookupParameters.map(normalize),
      lookupKeys = lookupKeys.map(normalize),
      residualCondition = residualCondition.map(normalize))
  }

  // Connector descriptions may contain secrets, so the scan and the reader factory are not shown.
  // The physical node renders the scan's redacted description, and nothing of the reader factory.
  override protected def stringArgs: Iterator[Any] =
    Iterator(joinType, lookupParameters, residualCondition, child)

  // The product of the stream side's size and spark.sql.defaultSizeInBytes, as the lookup side is
  // read by lookups rather than scanned. It can differ from the estimate before preparation, when
  // the scan of the lookup side reports statistics. The stream side's size alone would ignore the
  // lookup columns and the rows that a stream row can match.
  override def stats: Statistics = statsCache.getOrElse {
    statsCache = Some(Statistics(sizeInBytes =
      Seq(child.stats.sizeInBytes, BigInt(conf.defaultSizeInBytes)).filter(_ > 0L).product))
    statsCache.get
  }
}

/**
 * The types of lookup columns on which Spark offers a comparison to the connector as a lookup
 * candidate. The connector evaluates a selected candidate in its store, so Spark offers only types
 * whose values a store compares as Spark does. A comparison on any other type stays in Spark's
 * residual condition.
 */
object ExternalLookupKeyTypes {
  /**
   * Whether Spark offers `=` or `<=>` on the lookup column `column`, by its declared type:
   * integral types, BOOLEAN, DECIMAL, DATE, TIMESTAMP, TIMESTAMP_NTZ, BINARY, and STRING and
   * VARCHAR whose collation compares bytes, such as UTF8_BINARY. FLOAT and DOUBLE are not
   * offered, because stores treat NaN and -0.0 in different ways, nor CHAR, which Spark pads on
   * read, nor strings with another collation, nor complex types, nor any other type. VARCHAR is
   * not offered under `spark.sql.charVarchar.standardSemantics.enabled` either, because Spark
   * then trims trailing blanks on read, so a store that compares the stored value could miss a
   * row.
   *
   * Analysis reads a CHAR or VARCHAR column as a STRING and keeps its declared type in the
   * metadata, so the column's data type alone does not tell.
   */
  def supportsEquality(column: Attribute): Boolean =
    supportsEquality(CharVarcharUtils.getRawType(column.metadata).getOrElse(column.dataType))

  private def supportsEquality(dataType: DataType): Boolean = dataType match {
    case ByteType | ShortType | IntegerType | LongType | BooleanType | DateType | TimestampType |
        TimestampNTZType | BinaryType | _: DecimalType => true
    case _: CharType => false
    case varchar: VarcharType =>
      !SQLConf.get.charVarcharStandardSemantics && varchar.supportsBinaryEquality
    case stringType: StringType => stringType.supportsBinaryEquality
    case _ => false
  }

  /**
   * Whether Spark offers `<`, `<=`, `>` or `>=` on a lookup column of this type: integral types,
   * DECIMAL, DATE, TIMESTAMP and TIMESTAMP_NTZ. Strings and BINARY are not offered, because Spark
   * orders them by code point and by unsigned byte, while stores often order them differently,
   * nor BOOLEAN, FLOAT, DOUBLE, complex types or any other type.
   */
  def supportsRange(dataType: DataType): Boolean = dataType match {
    case ByteType | ShortType | IntegerType | LongType | DateType | TimestampType |
        TimestampNTZType | _: DecimalType => true
    case _ => false
  }
}
