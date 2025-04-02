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
import org.apache.spark.sql.types.{DataType, StringType}

trait TableConstraint {

  def name: String

  def characteristic: ConstraintCharacteristic

  def withNameAndCharacteristic(
      name: String,
      c: ConstraintCharacteristic,
      ctx: ParserRuleContext): TableConstraint

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

  override def sql: String = s"CONSTRAINT $name CHECK ($condition)"

  override def dataType: DataType = StringType
}

case class PrimaryKeyConstraint(
    columns: Seq[String],
    override val name: String = null,
    override val characteristic: ConstraintCharacteristic = ConstraintCharacteristic.empty)
  extends TableConstraint {

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
}

case class UniqueConstraint(
    columns: Seq[String],
    override val name: String = null,
    override val characteristic: ConstraintCharacteristic = ConstraintCharacteristic.empty)
    extends TableConstraint {

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
}

case class ForeignKeyConstraint(
    override val name: String = null,
    childColumns: Seq[String] = Seq.empty,
    parentTableId: Seq[String] = Seq.empty,
    parentColumns: Seq[String] = Seq.empty,
    override val characteristic: ConstraintCharacteristic = ConstraintCharacteristic.empty)
  extends TableConstraint {
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
}
