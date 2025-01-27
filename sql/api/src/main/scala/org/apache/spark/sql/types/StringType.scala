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

package org.apache.spark.sql.types

import org.json4s.JsonAST.{JString, JValue}

import org.apache.spark.annotation.Stable
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.internal.SqlApiConf

/**
 * The data type representing `String` values. Please use the singleton `DataTypes.StringType`.
 *
 * @since 1.3.0
 * @param collationId
 *   The id of collation for this StringType.
 */
@Stable
class StringType private[sql] (
    val collationId: Int,
    val constraint: StringConstraint = NoConstraint)
    extends AtomicType
    with Serializable {

  /**
   * Support for Binary Equality implies that strings are considered equal only if they are byte
   * for byte equal. E.g. all accent or case-insensitive collations are considered non-binary. If
   * this field is true, byte level operations can be used against this datatype (e.g. for
   * equality and hashing).
   */
  private[sql] def supportsBinaryEquality: Boolean =
    collationId == CollationFactory.UTF8_BINARY_COLLATION_ID ||
      CollationFactory.fetchCollation(collationId).supportsBinaryEquality

  private[sql] def supportsLowercaseEquality: Boolean =
    CollationFactory.fetchCollation(collationId).supportsLowercaseEquality

  private[sql] def isCaseInsensitive: Boolean =
    CollationFactory.isCaseInsensitive(collationId)

  private[sql] def isAccentInsensitive: Boolean =
    CollationFactory.isAccentInsensitive(collationId)

  private[sql] def usesTrimCollation: Boolean =
    CollationFactory.fetchCollation(collationId).supportsSpaceTrimming

  private[sql] def isUTF8BinaryCollation: Boolean =
    collationId == CollationFactory.UTF8_BINARY_COLLATION_ID

  private[sql] def isUTF8LcaseCollation: Boolean =
    collationId == CollationFactory.UTF8_LCASE_COLLATION_ID

  /**
   * Support for Binary Ordering implies that strings are considered equal only if they are byte
   * for byte equal. E.g. all accent or case-insensitive collations are considered non-binary.
   * Also their ordering does not require calls to ICU library, as it follows spark internal
   * implementation. If this field is true, byte level operations can be used against this
   * datatype (e.g. for equality, hashing and ordering).
   */
  private[sql] def supportsBinaryOrdering: Boolean =
    CollationFactory.fetchCollation(collationId).supportsBinaryOrdering

  /**
   * Type name that is shown to the customer. If this is an UTF8_BINARY collation output is
   * `string` due to backwards compatibility.
   */
  override def typeName: String =
    if (isUTF8BinaryCollation) "string"
    else s"string collate $collationName"

  override def toString: String =
    if (isUTF8BinaryCollation) "StringType"
    else s"StringType($collationName)"

  private[sql] def collationName: String =
    CollationFactory.fetchCollation(collationId).collationName

  // Due to backwards compatibility and compatibility with other readers
  // all string types are serialized in json as regular strings and
  // the collation information is written to struct field metadata
  override def jsonValue: JValue = JString("string")

  override def equals(obj: Any): Boolean = {
    obj match {
      case s: StringType => s.collationId == collationId && s.constraint == constraint
      case _ => false
    }
  }

  override def hashCode(): Int = collationId.hashCode()

  /**
   * The default size of a value of the StringType is 20 bytes.
   */
  override def defaultSize: Int = 20

  private[spark] override def asNullable: StringType = this
}

/**
 * Use StringType for expressions supporting only binary collation.
 *
 * @since 1.3.0
 */
@Stable
case object StringType
    extends StringType(CollationFactory.UTF8_BINARY_COLLATION_ID, NoConstraint) {
  private[spark] def apply(collationId: Int): StringType = new StringType(collationId)

  def apply(collation: String): StringType = {
    val collationId = CollationFactory.collationNameToId(collation)
    new StringType(collationId)
  }
}

/**
 * String type that was the result of coercing two different non-explicit collations.
 */
private[spark] case object IndeterminateStringType
    extends StringType(CollationFactory.INDETERMINATE_COLLATION_ID)

sealed trait StringConstraint

case object StringHelper extends PartialOrdering[StringConstraint] {
  override def tryCompare(x: StringConstraint, y: StringConstraint): Option[Int] = {
    (x, y) match {
      case (NoConstraint, NoConstraint) => Some(0)
      case (NoConstraint, _) => Some(-1)
      case (_, NoConstraint) => Some(1)
      case (FixedLength(l1), FixedLength(l2)) => Some(l2.compareTo(l1))
      case (FixedLength(l1), MaxLength(l2)) if l1 <= l2 => Some(1)
      case (MaxLength(l1), FixedLength(l2)) if l1 >= l2 => Some(-1)
      case (MaxLength(l1), MaxLength(l2)) => Some(l2.compareTo(l1))
      case _ => None
    }
  }

  override def lteq(x: StringConstraint, y: StringConstraint): Boolean = {
    tryCompare(x, y).exists(_ <= 0)
  }

  override def gteq(x: StringConstraint, y: StringConstraint): Boolean = {
    tryCompare(x, y).exists(_ >= 0)
  }

  override def equiv(x: StringConstraint, y: StringConstraint): Boolean = {
    tryCompare(x, y).contains(0)
  }

  def isPlainString(s: StringType): Boolean = s.constraint == NoConstraint

  def isMoreConstrained(a: StringType, b: StringType): Boolean =
    gteq(a.constraint, b.constraint)

  def tightestCommonString(s1: StringType, s2: StringType): Option[StringType] = {
    if (s1.collationId != s2.collationId) {
      return None
    }
    if (!SqlApiConf.get.preserveCharVarcharTypeInfo) {
      return Some(StringType(s1.collationId))
    }
    Some((s1.constraint, s2.constraint) match {
      case (FixedLength(l1), FixedLength(l2)) => CharType(l1.max(l2))
      case (MaxLength(l1), FixedLength(l2)) => VarcharType(l1.max(l2))
      case (FixedLength(l1), MaxLength(l2)) => VarcharType(l1.max(l2))
      case (MaxLength(l1), MaxLength(l2)) => VarcharType(l1.max(l2))
      case _ => StringType(s1.collationId)
    })
  }

  def removeCollation(s: StringType): StringType = s match {
    case CharType(length) => CharType(length)
    case VarcharType(length) => VarcharType(length)
    case _: StringType => StringType
  }
}

case object NoConstraint extends StringConstraint

case class FixedLength(length: Int) extends StringConstraint

case class MaxLength(length: Int) extends StringConstraint
