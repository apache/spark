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
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeSeq, AttributeSet,
  BinaryComparison, CurrentBatchTimestamp, EqualNullSafe, EqualTo, Expression, ExprId, ExprUtils,
  ExternalUserDefinedFunction, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual,
  Literal, NamedExpression, PythonUDF, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftAnti, LeftOuter, LeftSemi,
  QueryPlan}
import org.apache.spark.sql.catalyst.trees.TreePattern.{CURRENT_LIKE, EXTERNAL_LOOKUP_HINT,
  EXTERNAL_LOOKUP_JOIN, TreePattern}
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

  // As an ordinary join's: a semi or anti join returns at most its stream rows, and the other join
  // types get the product of both sides' sizes.
  override def stats: Statistics = joinType match {
    case LeftSemi | LeftAnti => left.stats
    case _ => super.stats
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
   * column on the left.
   */
  case class LookupComparison(
      lookupField: Attribute,
      parameter: Expression,
      comparison: LookupKeyComparison)

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
   * The lookup column must be one on which [[ExternalLookupKeyTypes]] lets Spark offer the
   * comparison. A comparison on another column is an ordinary conjunct.
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
      case written: BinaryComparison =>
        LookupKeyComparison.of(written).flatMap { comparison =>
          Seq(
            (written.left, written.right, comparison),
            (written.right, written.left, comparison.swapped)).collectFirst {
            case (lookupField: Attribute, parameter, comparison)
                if isLookupComparison(lookupField, parameter) &&
                  isOfferedOn(comparison, lookupField, parameter) =>
              LookupComparison(lookupField, parameter, comparison)
          }
        }
      case _ => None
    }
  }

  /**
   * Whether Spark offers the comparison on this lookup column, with this parameter.
   *
   * Spark evaluates the parameters on each stream row that passes the stream condition, before
   * the other join conditions. That is safe for an equality, as a hash join evaluates its keys on
   * every row too. A range is a join condition, which a join evaluates only after its keys match,
   * so its parameter must be one that cannot fail, as `cannotFail` decides.
   */
  private def isOfferedOn(
      comparison: LookupKeyComparison,
      lookupField: Attribute,
      parameter: Expression): Boolean =
    if (comparison.isRange) {
      ExternalLookupKeyTypes.supportsRange(lookupField.dataType) && cannotFail(parameter)
    } else {
      ExternalLookupKeyTypes.supportsEquality(lookupField)
    }

  /**
   * Whether a range parameter cannot fail, as ExprUtils.canEvaluateUnconditionally decides, cast
   * or not. It judges a cast in the cast's own eval mode, and accepts the numeric up-casts that
   * analysis adds when the stream side has a narrower type than the lookup column, as an INT
   * column compared with a BIGINT key.
   */
  private def cannotFail(parameter: Expression): Boolean =
    ExprUtils.canEvaluateUnconditionally(parameter)

  /**
   * Whether a lookup-side projection or filter can evaluate `expression` on the lookup results,
   * inside the join. Python and external UDFs and subqueries are planned as separate operators,
   * which a lookup-side stage cannot contain. A non-deterministic expression is not allowed
   * either: the join evaluates a stage on each returned row, so one stored row could get
   * different values for different stream rows, which a read of the table never does. The current
   * time of a micro-batch is allowed, as on a static side: MicroBatchExecution writes
   * `current_timestamp()` and `current_date()` as a `CurrentBatchTimestamp`, which is constant
   * within the batch, and [[withBatchTimestampValues]] replaces it with its value.
   */
  def canEvaluateInLookupStage(expression: Expression): Boolean =
    expression.transform {
      case batchTimestamp: CurrentBatchTimestamp => Literal(null, batchTimestamp.dataType)
    }.deterministic && !expression.exists {
      case _: PythonUDF | _: ExternalUserDefinedFunction | _: SubqueryExpression => true
      case _ => false
    }

  /**
   * Whether a request holds one parameter row: one of the selected comparisons is a range whose
   * parameter reads a stream column. One stored row can then match several parameter rows of a
   * request, and the source would have to return it once for each of them, with each ordinal.
   * Spark cannot detect a missing one. A range whose parameter reads no stream column has the
   * same value in every parameter row, so a stored row that satisfies it matches only the
   * parameter rows that the other comparisons select.
   */
  def oneParameterRowPerRequest(
      parameters: Seq[Expression],
      comparisons: Seq[LookupKeyComparison]): Boolean =
    parameters.zip(comparisons).exists { case (parameter, comparison) =>
      comparison.isRange && parameter.references.nonEmpty
    }

  /**
   * `plan` with the current time of the micro-batch replaced by its value. IncrementalExecution
   * replaces it after optimization, which does not reach the lookup-side stages of a prepared
   * lookup join.
   */
  def withBatchTimestampValues(plan: LogicalPlan): LogicalPlan =
    plan.transformAllExpressionsWithPruning(_.containsPattern(CURRENT_LIKE)) {
      case batchTimestamp: CurrentBatchTimestamp => batchTimestamp.toLiteral
    }
}

/**
 * The comparison that a lookup predicate makes between a lookup column, on the left, and its
 * parameter. `predicateName` is the name of the DSv2 `Predicate` that is offered for it: the SQL
 * operator, as `V2ExpressionBuilder` names it.
 */
sealed abstract class LookupKeyComparison(val predicateName: String) extends Serializable {
  /** The comparison of `key` with `parameter`, as Spark evaluates it. */
  def apply(key: Expression, parameter: Expression): Expression

  /** The same comparison with the operands swapped, so that `<` becomes `>`. */
  def swapped: LookupKeyComparison

  /** Whether one lookup column value can satisfy the comparison with several parameter values. */
  def isRange: Boolean = true

  /** Whether a NULL parameter can match, as with `<=>`. */
  def nullSafe: Boolean = false
}

object LookupKeyComparison {
  case object Equal extends LookupKeyComparison("=") {
    override def apply(key: Expression, parameter: Expression): Expression =
      EqualTo(key, parameter)
    override def swapped: LookupKeyComparison = this
    override def isRange: Boolean = false
  }

  case object NullSafeEqual extends LookupKeyComparison("<=>") {
    override def apply(key: Expression, parameter: Expression): Expression =
      EqualNullSafe(key, parameter)
    override def swapped: LookupKeyComparison = this
    override def isRange: Boolean = false
    override def nullSafe: Boolean = true
  }

  case object Less extends LookupKeyComparison("<") {
    override def apply(key: Expression, parameter: Expression): Expression =
      LessThan(key, parameter)
    override def swapped: LookupKeyComparison = Greater
  }

  case object LessOrEqual extends LookupKeyComparison("<=") {
    override def apply(key: Expression, parameter: Expression): Expression =
      LessThanOrEqual(key, parameter)
    override def swapped: LookupKeyComparison = GreaterOrEqual
  }

  case object Greater extends LookupKeyComparison(">") {
    override def apply(key: Expression, parameter: Expression): Expression =
      GreaterThan(key, parameter)
    override def swapped: LookupKeyComparison = Less
  }

  case object GreaterOrEqual extends LookupKeyComparison(">=") {
    override def apply(key: Expression, parameter: Expression): Expression =
      GreaterThanOrEqual(key, parameter)
    override def swapped: LookupKeyComparison = LessOrEqual
  }

  /** The comparison that `comparison` makes, if it is one that Spark can offer. */
  def of(comparison: BinaryComparison): Option[LookupKeyComparison] = comparison match {
    case _: EqualTo => Some(Equal)
    case _: EqualNullSafe => Some(NullSafeEqual)
    case _: LessThan => Some(Less)
    case _: LessThanOrEqual => Some(LessOrEqual)
    case _: GreaterThan => Some(Greater)
    case _: GreaterThanOrEqual => Some(GreaterOrEqual)
    case _ => None
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

object ExternalLookupTransform {
  /**
   * The lookup-side attributes of a lookup join, which canonicalization normalizes by their place
   * here, after the stream side's: the read schema, then the attributes that each projection of
   * `stages` makes.
   */
  def attributes(
      lookupInput: Seq[Attribute],
      stages: Seq[ExternalLookupTransform]): Seq[Attribute] =
    lookupInput ++ stages.collect { case project: ExternalLookupProject => project.output }.flatten

  /**
   * `stage` with each attribute normalized by its place in `attributes`, and each alias given the
   * id of its attribute's place, so that the stages after it read the attributes that it makes.
   */
  def canonicalized(
      stage: ExternalLookupTransform,
      attributes: AttributeSeq): ExternalLookupTransform = {
    def normalize[T <: Expression](expression: T): T =
      QueryPlan.normalizeExpressions(expression, attributes)
    stage match {
      case ExternalLookupProject(projectList, input) =>
        ExternalLookupProject(projectList.map {
          case alias: Alias =>
            Alias(normalize(alias.child), "")(ExprId(attributes.indexOf(alias.exprId)))
          case other => normalize(other)
        }, input.map(normalize))
      case ExternalLookupFilter(condition, input) =>
        ExternalLookupFilter(normalize(condition), input.map(normalize))
    }
  }
}

/**
 * A lookup join after its optimized lookup side has been converted to an external operation.
 *
 * @param lookupKeys The table column that each selected lookup predicate compares with the
 *   parameter at the same position. Spark checks each row that the connector returns against
 *   them, so the read schema must hold these columns.
 * @param lookupKeyComparisons The comparison that each selected lookup predicate makes, such as
 *   `=` or `<`, with the lookup column on the left. It tells how Spark checks the key.
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
    lookupKeys: Seq[Attribute],
    lookupKeyComparisons: Seq[LookupKeyComparison],
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
  // child's output, the read schema and the outputs of the lookup-side stages instead.
  override protected def doCanonicalize(): LogicalPlan = {
    val attributes: AttributeSeq =
      child.output ++ ExternalLookupTransform.attributes(lookupInput, lookupTransforms)
    def normalize[T <: Expression](expression: T): T =
      QueryPlan.normalizeExpressions(expression, attributes)
    copy(
      child = child.canonicalized,
      lookupInput = lookupInput.map(normalize),
      lookupOutput = lookupOutput.map(normalize),
      lookupTransforms = lookupTransforms.map(ExternalLookupTransform.canonicalized(_, attributes)),
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
  // the scan of the lookup side reports statistics, or when column pruning has projected the
  // lookup side. The stream side's size alone would ignore the lookup columns and the rows that a
  // stream row can match. A semi or anti join returns at most its stream rows.
  override def stats: Statistics = joinType match {
    case LeftSemi | LeftAnti => child.stats
    case _ => statsCache.getOrElse {
      statsCache = Some(Statistics(sizeInBytes =
        Seq(child.stats.sizeInBytes, BigInt(conf.defaultSizeInBytes)).filter(_ > 0L).product))
      statsCache.get
    }
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
   * not offered under `spark.sql.charVarchar.standardSemantics.enabled` either. Spark then reads a
   * value within the declared length unchanged, but drops the trailing blanks past that length
   * from a longer value, and fails if it is still too long. So a store that compares such a longer
   * value as stored could miss a row that Spark's comparison matches.
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
