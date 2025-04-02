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

import org.antlr.v4.runtime.ParserRuleContext

import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.util.V2ExpressionBuilder
import org.apache.spark.sql.connector.catalog.constraints.Constraint
import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.types.{DataType, StringType}

trait TableConstraint {
  // Convert to a data source v2 constraint
  def asConstraint(isCreateTable: Boolean): Constraint

  def withNameAndCharacteristic(
      name: String,
      c: ConstraintCharacteristic,
      ctx: ParserRuleContext): TableConstraint

  def name: String

  def characteristic: ConstraintCharacteristic

  def generateConstraintNameIfNeeded(tableName: String): TableConstraint = {
    if (name == null || name.isEmpty) {
      this.withNameAndCharacteristic(
        name = generateConstraintName(tableName),
        c = characteristic,
        ctx = null)
    } else {
      this
    }
  }

  // Generate a constraint name based on the table name if the name is not specified
  protected def generateConstraintName(tableName: String): String

  protected def defaultConstraintCharacteristic: ConstraintCharacteristic

  protected def getCharacteristicValues: (Boolean, Boolean) = {
    val rely = characteristic.rely.getOrElse(defaultConstraintCharacteristic.rely.get)
    val enforced = characteristic.enforced.getOrElse(defaultConstraintCharacteristic.enforced.get)
    (rely, enforced)
  }
}

case class ConstraintCharacteristic(enforced: Option[Boolean], rely: Option[Boolean])

object ConstraintCharacteristic {
  val empty: ConstraintCharacteristic = ConstraintCharacteristic(None, None)
}

case class CheckConstraint(
    child: Expression,
    condition: String,
    override val name: String = null,
    override val characteristic: ConstraintCharacteristic = ConstraintCharacteristic.empty)
  extends UnaryExpression
  with Unevaluable
  with TableConstraint {

  def asConstraint(isCreateTable: Boolean): Constraint = {
    val predicate = new V2ExpressionBuilder(child, true).buildPredicate().orNull
    val (rely, enforced) = getCharacteristicValues
    // The validation status is set to UNVALIDATED for create table and
    // VALID for alter table.
    val validateStatus = if (isCreateTable) {
      Constraint.ValidationStatus.UNVALIDATED
    } else {
      Constraint.ValidationStatus.VALID
    }
    Constraint
      .check(name)
      .sql(condition)
      .predicate(predicate)
      .rely(rely)
      .enforced(enforced)
      .validationStatus(validateStatus)
      .build()
  }

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)

  override def withNameAndCharacteristic(
      name: String,
      c: ConstraintCharacteristic,
      ctx: ParserRuleContext): TableConstraint = {
    copy(name = name, characteristic = c)
  }

  override protected def generateConstraintName(tableName: String): String = {
    val base = condition.filter(_.isLetterOrDigit).take(20)
    val rand = scala.util.Random.alphanumeric.take(6).mkString
    s"${tableName}_chk_${base}_$rand"
  }

  override def defaultConstraintCharacteristic: ConstraintCharacteristic =
    ConstraintCharacteristic(enforced = Some(true), rely = Some(false))

  override def sql: String = s"CONSTRAINT $name CHECK ($condition)"

  override def dataType: DataType = StringType
}

case class PrimaryKeyConstraint(
    columns: Seq[String],
    override val name: String = null,
    override val characteristic: ConstraintCharacteristic = ConstraintCharacteristic.empty)
  extends TableConstraint {

  override def asConstraint(isCreateTable: Boolean): Constraint = {
    val (rely, enforced) = getCharacteristicValues
    Constraint
      .primaryKey(name, columns.map(FieldReference.column).toArray)
      .rely(rely)
      .enforced(enforced)
      .validationStatus(Constraint.ValidationStatus.UNVALIDATED)
      .build()
  }

  override def withNameAndCharacteristic(
      name: String,
      c: ConstraintCharacteristic,
      ctx: ParserRuleContext): TableConstraint = {
    if (c.enforced.contains(true)) {
      throw new ParseException(
        errorClass = "UNSUPPORTED_CONSTRAINT_CHARACTERISTIC",
        messageParameters = Map(
          "characteristic" -> "ENFORCED",
          "constraintType" -> "PRIMARY KEY"),
        ctx = ctx
      )
    }
    copy(name = name, characteristic = c)
  }

  override protected def generateConstraintName(tableName: String): String = s"${tableName}_pk"

  override def defaultConstraintCharacteristic: ConstraintCharacteristic =
    ConstraintCharacteristic(enforced = Some(false), rely = Some(false))
}

case class UniqueConstraint(
    columns: Seq[String],
    override val name: String = null,
    override val characteristic: ConstraintCharacteristic = ConstraintCharacteristic.empty)
    extends TableConstraint {

  override def asConstraint(isCreateTable: Boolean): Constraint = {
    val (rely, enforced) = getCharacteristicValues
    Constraint
      .unique(name, columns.map(FieldReference.column).toArray)
      .rely(rely)
      .enforced(enforced)
      .validationStatus(Constraint.ValidationStatus.UNVALIDATED)
      .build()
  }

  override def withNameAndCharacteristic(
    name: String,
    c: ConstraintCharacteristic,
    ctx: ParserRuleContext): TableConstraint = {
    if (c.enforced.contains(true)) {
      throw new ParseException(
        errorClass = "UNSUPPORTED_CONSTRAINT_CHARACTERISTIC",
        messageParameters = Map(
          "characteristic" -> "ENFORCED",
          "constraintType" -> "UNIQUE"),
        ctx = ctx
      )
    }
    copy(name = name, characteristic = c)
  }

  override protected def generateConstraintName(tableName: String): String = {
    val base = columns.map(_.filter(_.isLetterOrDigit)).sorted.mkString("_").take(20)
    val rand = scala.util.Random.alphanumeric.take(6).mkString
    s"${tableName}_uk_${base}_$rand"
  }

  override def defaultConstraintCharacteristic: ConstraintCharacteristic =
    ConstraintCharacteristic(enforced = Some(false), rely = Some(false))
}

case class ForeignKeyConstraint(
    override val name: String = null,
    childColumns: Seq[String] = Seq.empty,
    parentTableId: Seq[String] = Seq.empty,
    parentColumns: Seq[String] = Seq.empty,
    override val characteristic: ConstraintCharacteristic = ConstraintCharacteristic.empty)
  extends TableConstraint {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override def asConstraint(isCreateTable: Boolean): Constraint = {
    val (rely, enforced) = getCharacteristicValues
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

  override def withNameAndCharacteristic(
      name: String,
      c: ConstraintCharacteristic,
      ctx: ParserRuleContext): TableConstraint = {
    if (c.enforced.contains(true)) {
      throw new ParseException(
        errorClass = "UNSUPPORTED_CONSTRAINT_CHARACTERISTIC",
        messageParameters = Map(
          "characteristic" -> "ENFORCED",
          "constraintType" -> "FOREIGN KEY"),
        ctx = ctx
      )
    }
    copy(name = name, characteristic = c)
  }

  override protected def generateConstraintName(tableName: String): String =
    s"${tableName}_${parentTableId.last}_fk"

  override def defaultConstraintCharacteristic: ConstraintCharacteristic =
    ConstraintCharacteristic(enforced = Some(false), rely = Some(false))
}
