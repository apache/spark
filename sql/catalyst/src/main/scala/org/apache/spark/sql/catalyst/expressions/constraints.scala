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
import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, UnresolvedException}
import org.apache.spark.sql.catalyst.expressions.codegen.{Block, CodegenContext, ExprCode, FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.util.V2ExpressionBuilder
import org.apache.spark.sql.connector.catalog.constraints.Constraint
import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{BooleanType, DataType}

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
    override val userProvidedCharacteristic: ConstraintCharacteristic = ConstraintCharacteristic.empty)
  extends UnaryExpression
  with TableConstraint {
// scalastyle:on line.size.limit

  def toV2Constraint: Constraint = {
    val predicate = new V2ExpressionBuilder(child, true).buildPredicate().orNull
    val enforced = userProvidedCharacteristic.enforced.getOrElse(true)
    val rely = userProvidedCharacteristic.rely.getOrElse(false)
    // TODO(SPARK-51903): Change the status to VALIDATED when we support validation on ALTER TABLE
    val validateStatus = Constraint.ValidationStatus.UNVALIDATED
    Constraint
      .check(name)
      .predicateSql(condition)
      .predicate(predicate)
      .rely(rely)
      .enforced(enforced)
      .validationStatus(validateStatus)
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
