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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{AnalysisErrorAt, StoredExpressionFunctions, TypeCheckResult, UnresolvedException}
import org.apache.spark.sql.catalyst.expressions.codegen.{Block, CodegenContext, ExprCode, FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.optimizer.{ReplaceExpressions, SpecialDatetimeValues}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.trees.TreePattern.{CURRENT_LIKE, VARIABLE_REFERENCE}
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, DateTimeUtils, StoredExpressionText, V2ExpressionBuilder}
import org.apache.spark.sql.connector.catalog.constraints.Constraint
import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{AbstractDataType, BooleanType, CharType, DataType, TimestampFamily}

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
    omitPredicate: Option[Boolean] = None)
  extends UnaryExpression
  with TableConstraint
  with ImplicitCastInputTypes {
// scalastyle:on line.size.limit

  override def inputTypes: Seq[AbstractDataType] = Seq(BooleanType)

  /**
   * Fails the analysis if the condition calls a user-defined function. A write can read `condition`
   * again and resolve the name of the function in its own session, where the name can refer to
   * another function or to none, so the constraint would not enforce what this DDL declares.
   */
  def failIfCallsUserDefinedFunction(): Unit = {
    StoredExpressionFunctions.findUserDefinedFunctionCall(child).foreach {
      case (call, functionName) =>
        call.failAnalysis(
          errorClass = "USER_DEFINED_FUNCTION_IN_CHECK_CONSTRAINT",
          messageParameters = Map("checkCondition" -> condition, "functionName" -> functionName))
    }
  }

  /** Fails the analysis if the condition contains a subquery, which `Check` does not allow. */
  def failIfContainsSubquery(): Unit = {
    if (SubqueryExpression.hasSubquery(child)) {
      child.failAnalysis(
        errorClass = "SUBQUERY_IN_CHECK_CONSTRAINT",
        messageParameters = Map("checkCondition" -> condition))
    }
  }

  // The decision is printed only once made, so that the plan string of a constraint without one
  // stays as it was.
  override def stringArgs: Iterator[Any] =
    Iterator(child, condition, userProvidedName, tableName, userProvidedCharacteristic) ++
      omitPredicate.iterator.map(omit => s"omitPredicate=$omit")

  /**
   * Decides from the analyzed condition whether to store the constraint without a predicate
   * (`omitPredicate`), unless the parser has already decided to keep it.
   * `AddCheckConstraint.storeAnalyzedQuery` calls this at the end of the analysis of an ALTER
   * TABLE. CREATE and REPLACE TABLE never decide, and store the predicate if there is one.
   *
   * A write parses `condition` again, with its own settings. It cannot bind a parameter marker
   * that the legacy parameter substitution leaves there, resolves an IDENTIFIER clause in its own
   * session, and does not resolve a column qualified by the table name the way ALTER TABLE does.
   * It also resolves a name such as `current_date` or `user` to the function or to a column,
   * depending on its case sensitivity, its reserved keywords and the columns of the table at that
   * time. None of them can be told apart after analysis, so the parser keeps the predicate for
   * them, and for any name with more than one part, since it cannot tell a table qualifier from a
   * struct column. Such a condition, one that `resolvesDifferentlyOnWrite`, and one that a write
   * with the default parser settings reads differently (`readsAsOnWrite`) keep the predicate built
   * from the optimized plan, as before. So does one that calls a user-defined function, which the
   * analysis rejects next and which is not evaluated here, and every condition under
   * `spark.sql.legacy.freezeCurrentValuesInCheckConstraint`.
   */
  def withPredicateDecision(): CheckConstraint = if (omitPredicate.isDefined) this else {
    // Computed only after the check for a user-defined function, since finding the reads of the
    // current context evaluates constant strings.
    lazy val replaced = ReplaceExpressions.replace(child)
    lazy val readsCurrentContext = CheckConstraint.nodesReadingCurrentContext(replaced)
    copy(omitPredicate = Some(
      !SQLConf.get.getConf(SQLConf.FREEZE_CURRENT_VALUES_IN_CHECK_CONSTRAINT) &&
        StoredExpressionFunctions.findUserDefinedFunctionCall(child).isEmpty &&
        !CheckConstraint.resolvesDifferentlyOnWrite(child, replaced, readsCurrentContext) &&
        readsCurrentContext(replaced) &&
        CheckConstraint.readsAsOnWrite(condition)))
  }

  // ALTER TABLE calls this on the optimized plan, where the current time, date, catalog, user and
  // the like are replaced by their values at the time of the ALTER TABLE. A condition that
  // `withPredicateDecision` finds reading one is stored without a predicate, so that writes check
  // `predicateSql`. ALTER TABLE does not build the predicate from the analyzed condition instead,
  // because `V2ExpressionBuilder` folds `CAST('today' AS DATE)` there as an ordinary cast.
  // CREATE and REPLACE TABLE call this on the analyzed condition (`ResolveTableSpec`), where that
  // folding still happens (SPARK-60451). Only `AddCheckConstraint` decides, so they always store
  // the predicate if there is one.
  def toV2Constraint: Constraint = {
    val predicate = if (omitPredicate.contains(true)) {
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
   * Which nodes of `replaced`, an expression whose `RuntimeReplaceable`s are replaced, read the
   * current time, date, catalog, user and the like, which the optimizer replaces with their
   * values, found in one bottom-up pass. A node reads one if a node of its subtree is one of:
   *  - a node with the CURRENT_LIKE pattern, which stands in for `ReplaceCurrentLike`, which needs
   *    a `CatalogManager`, and for the functions `ComputeCurrentTime` replaces;
   *  - a cast from TIME to a timestamp, which `ComputeCurrentTime` rewrites without that pattern;
   *  - a cast that `castReadsCurrentDate`.
   * The result answers by identity, so only for the nodes of `replaced` itself.
   */
  private def nodesReadingCurrentContext(replaced: Expression): Expression => Boolean = {
    val reading = java.util.Collections.newSetFromMap(
      new java.util.IdentityHashMap[Expression, java.lang.Boolean]())
    def visit(e: Expression): Boolean = {
      // Every child is visited, so that each node that reads one is recorded.
      val childReads = e.children.map(visit).contains(true)
      val reads = childReads || e.containsPattern(CURRENT_LIKE) || (e match {
        case c: Cast =>
          Cast.isTimeToTimestampNTZ(c.child.dataType, c.dataType) ||
            Cast.isTimeToTimestampLTZ(c.child.dataType, c.dataType) || castReadsCurrentDate(c)
        case _ => false
      })
      if (reads) reading.add(e)
      reads
    }
    visit(replaced)
    reading.contains
  }

  /**
   * Whether `cast` reads the current date: `SpecialDatetimeValues` replaces it with the value of
   * 'now', 'today', 'tomorrow' or 'yesterday', or it casts a string with only a time part to a
   * timestamp with local time zone, which completes it with the current date. Only a cast of a
   * constant string at the top level counts, not one into an array, map or struct of timestamps,
   * from a VARIANT, or of a string that only the optimizer makes constant.
   */
  private def castReadsCurrentDate(cast: Cast): Boolean = {
    val completesTime = TimestampFamily.isLtz(cast.dataType)
    (completesTime || SpecialDatetimeValues.rewrites(cast.dataType)) &&
      SpecialDatetimeValues.constantString(cast).exists { s =>
        SpecialDatetimeValues.readsCurrentDate(s, cast.dataType) ||
          (completesTime && DateTimeUtils.isTimeOnlyTimestamp(s))
      }
  }

  /**
   * Whether a write with the default parser settings reads `condition` as this session does, which
   * `StoredExpressionText` decides for a generation expression too, so ANSI mode, the timestamp
   * type and the session time zone are those of this session. A `${...}` placeholder counts as
   * read differently, since a write with `spark.sql.variable.substitute` may replace it.
   */
  private def readsAsOnWrite(condition: String): Boolean = !condition.contains("${") && {
    val conf = SQLConf.get.clone()
    // Under its legacy config, `StoredExpressionText` would compare the session with itself.
    conf.setConf(SQLConf.USE_CURRENT_SQL_CONFIGS_FOR_GENERATED_COLUMN, false)
    SQLConf.withExistingConf(conf)(StoredExpressionText.readsAsInSession(condition))
  }

  /**
   * Whether a write that parses the condition resolves `e` differently: `e` reads a variable, which
   * the write looks up in its own session, a column whose type is or contains CHAR, which the
   * write reads without the CHAR type that pads the literals compared with it, or a constant that
   * may depend on the session time zone and reads no current value, such as a typed `TIMESTAMP`
   * literal or `'2024-01-01 00:00:00'` cast to a timestamp, which the write computes in its own
   * time zone. Apart from a cast, which counts only if it needs the time zone, a foldable
   * expression of a timestamp type that reads no current value counts whether or not it depends on
   * the time zone, such as `timestamp_seconds(0)`, and then keeps its predicate as before. A
   * user-defined function is rejected by `failIfCallsUserDefinedFunction`.
   */
  private def resolvesDifferentlyOnWrite(
      e: Expression,
      replaced: Expression,
      readsCurrentContext: Expression => Boolean): Boolean = {
    e.containsPattern(VARIABLE_REFERENCE) ||
      e.references.exists { a =>
        val rawType = CharVarcharUtils.getRawType(a.metadata)
        rawType.exists(_.existsRecursively(_.isInstanceOf[CharType]))
      } ||
      replaced.exists {
        case c: Cast => c.foldable && c.needsTimeZone && !readsCurrentContext(c)
        case t: TimeZoneAwareExpression => t.foldable && !readsCurrentContext(t)
        // A typed literal, or a function such as `make_timestamp(DATE'2024-01-01', TIME'00:00')`
        // that the replacement turns into a call with the session time zone as an argument.
        case x if x.foldable && x.dataType.existsRecursively(TimestampFamily.isLtz) =>
          !readsCurrentContext(x)
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
        // The name is not Java source, so pass it by reference instead of embedding it.
        val nameRef = ctx.addReferenceObj("columnName", name)
        code"""
              |$colList.add($nameRef);
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
    // These texts are not Java source, so pass them by reference instead of embedding them.
    val nameRef = ctx.addReferenceObj("constraintName", constraintName)
    val sqlRef = ctx.addReferenceObj("predicateSql", predicateSql)
    val ret = code"""${elementValue.code}
          |
          |if (!${elementValue.isNull} && ${elementValue.value} == false) {
          |  ${generateColumnValuesCode(colListName, valListName, ctx)}
          |  throw org.apache.spark.sql.errors.QueryExecutionErrors.checkViolationJava(
          |     $nameRef, $sqlRef, $colListName, $valListName);
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
