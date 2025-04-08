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

import org.antlr.v4.runtime.ParserRuleContext

import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.types.{DataType, StringType}

trait TableConstraint {

  /** Returns the name of the constraint */
  def name: String

  /** Returns the characteristics of the constraint (e.g., ENFORCED, RELY) */
  def characteristic: ConstraintCharacteristic

  /** Creates a new constraint with the given name
   *
   * @param name Constraint name
   * @return New TableConstraint instance
   */
  def withName(name: String): TableConstraint

  /** Creates a new constraint with the given characteristic
   *
   * @param c Constraint characteristic (ENFORCED, RELY)
   * @param ctx Parser context for error reporting
   * @return New TableConstraint instance
   */
  def withCharacteristic(c: ConstraintCharacteristic, ctx: ParserRuleContext): TableConstraint

  // Generate a constraint name based on the table name if the name is not specified
  protected def generateConstraintName(tableName: String): String

  /** Generates a constraint name if one is not provided
   *
   * @param tableName Name of the table containing this constraint
   * @return TableConstraint with a generated name if original name was null/empty
   */
  def generateConstraintNameIfNeeded(tableName: String): TableConstraint = {
    if (name == null || name.isEmpty) {
      this.withName(name = generateConstraintName(tableName))
    } else {
      this
    }
  }

  // This method generates a random identifier that has a similar format to Git commit hashes,
  // which provide a good balance between uniqueness and readability when used as constraint
  // identifiers.
  protected def randomSuffix: String = {
    UUID.randomUUID().toString.replace("-", "").take(7)
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

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)

  override protected def generateConstraintName(tableName: String): String = {
    s"${tableName}_chk_$randomSuffix"
  }

  override def sql: String = s"CONSTRAINT $name CHECK ($condition)"

  override def dataType: DataType = StringType

  override def withName(name: String): TableConstraint = copy(name = name)

  override def withCharacteristic(
      c: ConstraintCharacteristic,
      ctx: ParserRuleContext): TableConstraint =
    copy(characteristic = c)
}

case class PrimaryKeyConstraint(
    columns: Seq[String],
    override val name: String = null,
    override val characteristic: ConstraintCharacteristic = ConstraintCharacteristic.empty)
  extends TableConstraint {
  override protected def generateConstraintName(tableName: String): String = s"${tableName}_pk"

  override def withName(name: String): TableConstraint = copy(name = name)

  override def withCharacteristic(
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
    copy(characteristic = c)
  }
}

case class UniqueConstraint(
    columns: Seq[String],
    override val name: String = null,
    override val characteristic: ConstraintCharacteristic = ConstraintCharacteristic.empty)
    extends TableConstraint {

  override protected def generateConstraintName(tableName: String): String = {
    s"${tableName}_uniq_$randomSuffix"
  }

  override def withName(name: String): TableConstraint = copy(name = name)

  override def withCharacteristic(
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
    copy(characteristic = c)
  }
}

case class ForeignKeyConstraint(
    childColumns: Seq[String] = Seq.empty,
    parentTableId: Seq[String] = Seq.empty,
    parentColumns: Seq[String] = Seq.empty,
    override val name: String = null,
    override val characteristic: ConstraintCharacteristic = ConstraintCharacteristic.empty)
  extends TableConstraint {

  override protected def generateConstraintName(tableName: String): String =
    s"${tableName}_${parentTableId.last}_fk_$randomSuffix"

  override def withName(name: String): TableConstraint = copy(name = name)

  override def withCharacteristic(
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
}
