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
package org.apache.spark.sql.catalyst.expressions

import java.util.UUID

import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, UnresolvedException}
import org.apache.spark.sql.catalyst.expressions.codegen.{Block, CodegenContext, ExprCode, FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.catalyst.optimizer.{ComputeCurrentTime, ReplaceExpressions, SpecialDatetimeValues}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.trees.TreePattern.{ANALYSIS_AWARE_EXPRESSION, CURRENT_LIKE, TreePattern, VARIABLE_REFERENCE}
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, DateTimeUtils, V2ExpressionBuilder}
import org.apache.spark.sql.connector.catalog.constraints.Constraint
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction
import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{AbstractDataType, BooleanType, CharType, DataType, StringType, TimestampLTZNanosType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

trait TableConstraint extends Expression with Unevaluable {
  /** Convert to a data source v2 constraint */
  def toV2Constraint: Constraint

  /** Returns the user-provided name of the constraint */
  def userProvidedName: String

  /** Returns the name of the table containing this constraint */
  def tableName: String

  /** Returns the user-provided characteristics of the constraint (e.g., ENFORCED, RELY) */
  def userProvidedCharacteristic: ConstraintCharacteristic

  /** Creates a new constraint with the user-provided name
   *
   * @param name Constraint name
   * @return New TableConstraint instance
   */
  def withUserProvidedName(name: String): TableConstraint

  /**
   * Creates a new constraint with the given table name
   *
   * @param tableName Name of the table containing this constraint
   * @return New TableConstraint instance
   */
  def withTableName(tableName: String): TableConstraint

  /** Creates a new constraint with the user-provided characteristic
   *
   * @param c Constraint characteristic (ENFORCED, RELY)
   * @return New TableConstraint instance
   */
  def withUserProvidedCharacteristic(c: ConstraintCharacteristic): TableConstraint

  // Generate a constraint name based on the table name if the name is not specified
  protected def generateName(tableName: String): String

  /**
   * Gets the constraint name. If no name is provided by the user (null or empty),
   * generates a name based on the table name using generateName.
   *
   * @return The constraint name (either user-provided or generated)
   */
  final def name: String = {
    if (userProvidedName == null || userProvidedName.isEmpty) {
      generateName(tableName)
    } else {
      userProvidedName
    }
  }

  // This method generates a random identifier that has a similar format to Git commit hashes,
  // which provide a good balance between uniqueness and readability when used as constraint
  // identifiers.
  final protected def randomSuffix: String = {
    UUID.randomUUID().toString.replace("-", "").take(7)
  }

  protected def failIfEnforced(c: ConstraintCharacteristic, constraintType: String): Unit = {
    if (c.enforced.contains(true)) {
      val origin = CurrentOrigin.get
      throw new ParseException(
        command = origin.sqlText,
        start = origin,
        errorClass = "UNSUPPORTED_CONSTRAINT_CHARACTERISTIC",
        messageParameters = Map(
          "characteristic" -> "ENFORCED",
          "constraintType" -> constraintType)
      )
    }
  }

  override def nullable: Boolean = throw new UnresolvedException("nullable")

  override def dataType: DataType = throw new UnresolvedException("dataType")
}

case class ConstraintCharacteristic(enforced: Option[Boolean], rely: Option[Boolean])

object ConstraintCharacteristic {
  val empty: ConstraintCharacteristic = ConstraintCharacteristic(None, None)
}

// scalastyle:off line.size.limit
case class CheckConstraint(
    child: Expression,
    condition: String,
    override val userProvidedName: String = null,
    override val tableName: String = null,
    override val userProvidedCharacteristic: ConstraintCharacteristic = ConstraintCharacteristic.empty,
    keepPredicate: Boolean = false,
    omitPredicate: Boolean = false)
  extends UnaryExpression
  with TableConstraint
  with ImplicitCastInputTypes
  with AnalysisAwareExpression[CheckConstraint] {
// scalastyle:on line.size.limit

  final override val nodePatterns: Seq[TreePattern] = Seq(ANALYSIS_AWARE_EXPRESSION)

  override def inputTypes: Seq[AbstractDataType] = Seq(BooleanType)

  override def stringArgs: Iterator[Any] =
    Iterator(child, condition, userProvidedName, tableName, userProvidedCharacteristic)

  // A write parses `condition` again. It cannot bind a parameter marker that the legacy parameter
  // substitution leaves there, resolves an IDENTIFIER clause in its own session, and does not
  // resolve a column qualified by the table name the way ALTER TABLE does. None of them can be
  // told apart after analysis, so the parser sets `keepPredicate` for them, and for any name with
  // more than one part, since it cannot tell a table qualifier from a struct column. Such a
  // condition, or one that `resolvesDifferentlyOnWrite`, keeps the predicate built from the
  // optimized plan, as before.
  override def markAsAnalyzed(): CheckConstraint = copy(omitPredicate =
    !keepPredicate && !CheckConstraint.resolvesDifferentlyOnWrite(child) &&
      CheckConstraint.readsCurrentContext(child))

  // ALTER TABLE builds the constraint from the optimized plan, where the current time, date,
  // catalog, user and the like are replaced by their values at the time of the ALTER TABLE. A
  // condition that `markAsAnalyzed` finds reading one is stored without a predicate, so that
  // writes check `predicateSql`.
  // The predicate is not built from the analyzed condition, as the default value of a column is,
  // because `V2ExpressionBuilder` folds `CAST('today' AS DATE)` there as an ordinary cast. CREATE
  // and REPLACE TABLE convert their constraints before analysis finishes, while `omitPredicate` is
  // still false.
  def toV2Constraint: Constraint = {
    val predicate = if (omitPredicate) {
      null
    } else {
      new V2ExpressionBuilder(child, true).buildPredicate().orNull
    }
    val enforced = userProvidedCharacteristic.enforced.getOrElse(true)
    val rely = userProvidedCharacteristic.rely.getOrElse(false)
    Constraint
      .check(name)
      .predicateSql(condition)
      .predicate(predicate)
      .rely(rely)
      .enforced(enforced)
      .validationStatus(Constraint.ValidationStatus.VALID)
      .build()
  }

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)

  override protected def generateName(tableName: String): String = {
    s"${tableName}_chk_$randomSuffix"
  }

  override def sql: String = s"CONSTRAINT $userProvidedName CHECK ($condition)"

  override def withUserProvidedName(name: String): TableConstraint = copy(userProvidedName = name)

  override def withTableName(tableName: String): TableConstraint = copy(tableName = tableName)

  override def withUserProvidedCharacteristic(c: ConstraintCharacteristic): TableConstraint = {
    if (c.enforced.contains(false)) {
      val origin = CurrentOrigin.get
      throw new ParseException(
        command = origin.sqlText,
        start = origin,
        errorClass = "UNSUPPORTED_CONSTRAINT_CHARACTERISTIC",
        messageParameters = Map(
          "characteristic" -> "NOT ENFORCED",
          "constraintType" -> "CHECK")
      )
    }
    copy(userProvidedCharacteristic = c)
  }
}

object CheckConstraint {
  /**
   * Whether `e`, with its `RuntimeReplaceable`s replaced, reads the current time, date, catalog,
   * user and the like, which the optimizer replaces with their values. The CURRENT_LIKE pattern
   * stands in for `ReplaceCurrentLike`, which needs a `CatalogManager`, and for the functions
   * `ComputeCurrentTime` replaces. `ComputeCurrentTime` is still applied for the casts from TIME
   * to a timestamp it rewrites, which carry no CURRENT_LIKE bit.
   */
  private def readsCurrentContext(e: Expression): Boolean = {
    val replaced = ReplaceExpressions.replace(e)
    replaced.containsPattern(CURRENT_LIKE) ||
      (ComputeCurrentTime.applyForExpression(replaced) ne replaced) ||
      replaced.exists {
        case c: Cast => castReadsCurrentDate(c)
        case _ => false
      }
  }

  /**
   * Whether `cast` reads the current date: `SpecialDatetimeValues` replaces it with the value of
   * 'now', 'today', 'tomorrow' or 'yesterday', or it casts a string with only a time part to a
   * timestamp with local time zone, which completes it with the current date. Only a cast of a
   * constant string at the top level counts, not one into an array, map or struct of timestamps,
   * from a VARIANT, or of a string that only the optimizer makes constant.
   */
  private def castReadsCurrentDate(cast: Cast): Boolean = cast match {
    case c if SpecialDatetimeValues.readsCurrentDate(c) => true
    case Cast(s, TimestampType | _: TimestampLTZNanosType, _, _)
        if s.foldable && s.dataType.isInstanceOf[StringType] =>
      try {
        val v = s.eval()
        v != null && DateTimeUtils.isTimeOnlyTimestamp(v.asInstanceOf[UTF8String])
      } catch {
        case NonFatal(_) => false
      }
    case _ => false
  }

  /**
   * Whether a write that parses the condition resolves `e` differently: `e` reads a variable or
   * calls a user-defined function, which the write looks up in its own session, or reads a column
   * whose type is or contains CHAR, which the write reads without the CHAR type that pads the
   * literals compared with it.
   */
  private def resolvesDifferentlyOnWrite(e: Expression): Boolean = {
    e.containsPattern(VARIABLE_REFERENCE) ||
      e.references.exists { a =>
        val rawType = CharVarcharUtils.getRawType(a.metadata)
        rawType.exists(_.existsRecursively(_.isInstanceOf[CharType]))
      } ||
      e.exists {
        case _: UserDefinedExpression => true
        case i: StaticInvoke => i.scalarFunction.isDefined
        case Invoke(Literal(_: ScalarFunction[_], _), _, _, _, _, _, _, _) => true
        case _ => false
      }
  }
}

// scalastyle:off line.size.limit
case class PrimaryKeyConstraint(
    columns: Seq[String],
    override val userProvidedName: String = null,
    override val tableName: String = null,
    override val userProvidedCharacteristic: ConstraintCharacteristic = ConstraintCharacteristic.empty)
  extends LeafExpression with TableConstraint {
// scalastyle:on line.size.limit

  override def toV2Constraint: Constraint = {
    val enforced = userProvidedCharacteristic.enforced.getOrElse(false)
    val rely = userProvidedCharacteristic.rely.getOrElse(false)
    Constraint
      .primaryKey(name, columns.map(FieldReference.column).toArray)
      .rely(rely)
      .enforced(enforced)
      .validationStatus(Constraint.ValidationStatus.UNVALIDATED)
      .build()
  }

  override protected def generateName(tableName: String): String = s"${tableName}_pk"

  override def withUserProvidedName(name: String): TableConstraint = copy(userProvidedName = name)

  override def withTableName(tableName: String): TableConstraint = copy(tableName = tableName)

  override def withUserProvidedCharacteristic(c: ConstraintCharacteristic): TableConstraint = {
    failIfEnforced(c, "PRIMARY KEY")
    copy(userProvidedCharacteristic = c)
  }
}

// scalastyle:off line.size.limit
case class UniqueConstraint(
    columns: Seq[String],
    override val userProvidedName: String = null,
    override val tableName: String = null,
    override val userProvidedCharacteristic: ConstraintCharacteristic = ConstraintCharacteristic.empty)
  extends LeafExpression with TableConstraint {
// scalastyle:on line.size.limit

  override def toV2Constraint: Constraint = {
    val enforced = userProvidedCharacteristic.enforced.getOrElse(false)
    val rely = userProvidedCharacteristic.rely.getOrElse(false)
    Constraint
      .unique(name, columns.map(FieldReference.column).toArray)
      .rely(rely)
      .enforced(enforced)
      .validationStatus(Constraint.ValidationStatus.UNVALIDATED)
      .build()
  }

  override protected def generateName(tableName: String): String = {
    s"${tableName}_uniq_$randomSuffix"
  }

  override def withUserProvidedName(name: String): TableConstraint = copy(userProvidedName = name)

  override def withTableName(tableName: String): TableConstraint = copy(tableName = tableName)

  override def withUserProvidedCharacteristic(c: ConstraintCharacteristic): TableConstraint = {
    failIfEnforced(c, "UNIQUE")
    copy(userProvidedCharacteristic = c)
  }
}

// scalastyle:off line.size.limit
case class ForeignKeyConstraint(
    childColumns: Seq[String] = Seq.empty,
    parentTableId: Seq[String] = Seq.empty,
    parentColumns: Seq[String] = Seq.empty,
    override val userProvidedName: String = null,
    override val tableName: String = null,
    override val userProvidedCharacteristic: ConstraintCharacteristic = ConstraintCharacteristic.empty)
  extends LeafExpression with TableConstraint {
// scalastyle:on line.size.limit

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override def toV2Constraint: Constraint = {
    val enforced = userProvidedCharacteristic.enforced.getOrElse(false)
    val rely = userProvidedCharacteristic.rely.getOrElse(false)
    Constraint
      .foreignKey(name,
        childColumns.map(FieldReference.column).toArray,
        parentTableId.asIdentifier,
        parentColumns.map(FieldReference.column).toArray)
      .rely(rely)
      .enforced(enforced)
      .validationStatus(Constraint.ValidationStatus.UNVALIDATED)
      .build()
  }

  override protected def generateName(tableName: String): String =
    s"${tableName}_${parentTableId.last}_fk_$randomSuffix"

  override def withUserProvidedName(name: String): TableConstraint = copy(userProvidedName = name)

  override def withTableName(tableName: String): TableConstraint = copy(tableName = tableName)

  override def withUserProvidedCharacteristic(c: ConstraintCharacteristic): TableConstraint = {
    failIfEnforced(c, "FOREIGN KEY")
    copy(userProvidedCharacteristic = c)
  }
}

/**
 * An expression that validates a check constraint on a column.
 * If the evaluation result is false, it throws a [[SparkRuntimeException]] indicating constraint
 * violation. Otherwise, it returns true to indicate that the constraint is satisfied, even if the
 * expression is null.
 *
 * @param child The fully resolved expression to be evaluated to check the constraint.
 * @param columnExtractors Extractors for each referenced column. Used to generate readable errors.
 * @param constraintName The name of the constraint.
 * @param predicateSql The SQL representation of the constraint.
 */
case class CheckInvariant(
    child: Expression,
    columnExtractors: Seq[(String, Expression)],
    constraintName: String,
    predicateSql: String)
  extends Expression with NonSQLExpression {

  override def children: Seq[Expression] = child +: columnExtractors.map(_._2)
  override def dataType: DataType = BooleanType
  override def nullable: Boolean = false

  override def checkInputDataTypes(): TypeCheckResult = {
    if (child.dataType != BooleanType) {
      TypeCheckResult.TypeCheckFailure(
        s"CHECK constraint must evaluate to a boolean expression, but got ${child.dataType}")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def eval(input: InternalRow): Any = {
    val result = child.eval(input)
    if (result == false) {
      val values = columnExtractors.map {
        case (column, extractor) => column -> extractor.eval(input)
      }.toMap
      throw QueryExecutionErrors.checkViolation(constraintName, predicateSql, values)
    }
    true
  }

  /**
   * Generate the code to extract values for the columns referenced in a violated CHECK constraint.
   * We build parallel lists of full column names and their extracted values in the row which
   * violates the constraint, to be passed to the constraint violation error
   * in [[generateExpressionValidationCode()]].
   *
   * Note that this code is a bit expensive, so it shouldn't be run until we already
   * know the constraint has been violated.
   */
  private def generateColumnValuesCode(
    colList: String, valList: String, ctx: CodegenContext): Block = {
    val start =
      code"""
            |java.util.List<String> $colList = new java.util.ArrayList<String>();
            |java.util.List<Object> $valList = new java.util.ArrayList<Object>();
            |""".stripMargin
    columnExtractors.map {
      case (name, extractor) =>
        val colValue = extractor.genCode(ctx)
        code"""
              |$colList.add("$name");
              |${colValue.code}
              |if (${colValue.isNull}) {
              |  $valList.add(null);
              |} else {
              |  $valList.add(${colValue.value});
              |}
              |""".stripMargin
    }.fold(start)(_ + _)
  }

  private def generateExpressionValidationCode(ctx: CodegenContext): Block = {
    val elementValue = child.genCode(ctx)
    val colListName = ctx.freshName("colList")
    val valListName = ctx.freshName("valList")
    val ret = code"""${elementValue.code}
          |
          |if (!${elementValue.isNull} && ${elementValue.value} == false) {
          |  ${generateColumnValuesCode(colListName, valListName, ctx)}
          |  throw org.apache.spark.sql.errors.QueryExecutionErrors.checkViolationJava(
          |     "$constraintName", "$predicateSql", $colListName, $valListName);
          |}
     """.stripMargin
    ret
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val code = generateExpressionValidationCode(ctx)
    ev.copy(code = code, isNull = FalseLiteral, value = TrueLiteral)
  }

  override protected def withNewChildrenInternal(
    newChildren: IndexedSeq[Expression]): Expression = {
    copy(
      child = newChildren.head,
      columnExtractors = columnExtractors.map(_._1).zip(newChildren.tail)
    )
  }
}
