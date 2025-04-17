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

import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.types.{DataType, StringType}

trait TableConstraint {

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
  with Unevaluable
  with TableConstraint {
// scalastyle:on line.size.limit

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)

  override protected def generateName(tableName: String): String = {
    s"${tableName}_chk_$randomSuffix"
  }

  override def sql: String = s"CONSTRAINT $userProvidedName CHECK ($condition)"

  override def dataType: DataType = StringType

  override def withUserProvidedName(name: String): TableConstraint = copy(userProvidedName = name)

  override def withTableName(tableName: String): TableConstraint = copy(tableName = tableName)

  override def withUserProvidedCharacteristic(c: ConstraintCharacteristic): TableConstraint =
    copy(userProvidedCharacteristic = c)
}

// scalastyle:off line.size.limit
case class PrimaryKeyConstraint(
    columns: Seq[String],
    override val userProvidedName: String = null,
    override val tableName: String = null,
    override val userProvidedCharacteristic: ConstraintCharacteristic = ConstraintCharacteristic.empty)
  extends TableConstraint {
// scalastyle:on line.size.limit

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
    extends TableConstraint {
// scalastyle:on line.size.limit

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
  extends TableConstraint {
// scalastyle:on line.size.limit

  override protected def generateName(tableName: String): String =
    s"${tableName}_${parentTableId.last}_fk_$randomSuffix"

  override def withUserProvidedName(name: String): TableConstraint = copy(userProvidedName = name)

  override def withTableName(tableName: String): TableConstraint = copy(tableName = tableName)

  override def withUserProvidedCharacteristic(c: ConstraintCharacteristic): TableConstraint = {
    failIfEnforced(c, "FOREIGN KEY")
    copy(userProvidedCharacteristic = c)
  }
}
