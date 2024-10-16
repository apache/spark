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
 * @param _collationId
 *   The id of collation for this StringType.
 */
@Stable
class StringType private[sql] (private val _collationId: Int) extends AtomicType with Serializable {

  def collationId: Int = _collationId

  /**
   * Support for Binary Equality implies that strings are considered equal only if they are byte
   * for byte equal. E.g. all accent or case-insensitive collations are considered non-binary. If
   * this field is true, byte level operations can be used against this datatype (e.g. for
   * equality and hashing).
   */
  private[sql] def supportsBinaryEquality: Boolean =
    CollationFactory.fetchCollation(collationId).supportsBinaryEquality

  private[sql] def supportsLowercaseEquality: Boolean =
    CollationFactory.fetchCollation(collationId).supportsLowercaseEquality

  private[sql] def isNonCSAI: Boolean =
    !CollationFactory.isCaseSensitiveAndAccentInsensitive(collationId)

  private[sql] def usesTrimCollation: Boolean =
    CollationFactory.usesTrimCollation(collationId)

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
    else s"string collate ${CollationFactory.fetchCollation(collationId).collationName}"

  // Due to backwards compatibility and compatibility with other readers
  // all string types are serialized in json as regular strings and
  // the collation information is written to struct field metadata
  override def jsonValue: JValue = JString("string")

  override def equals(obj: Any): Boolean =
    obj.isInstanceOf[StringType] && obj.asInstanceOf[StringType].collationId == collationId

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
case object StringType extends StringType(0) {
  private[spark] def apply(collationId: Int): StringType = new StringType(collationId)

  def apply(collation: String): StringType = {
    val collationId = CollationFactory.collationNameToId(collation)
    new StringType(collationId)
  }
}

/**
 * The result type of literals, column definitions without explicit collation, casts to string
 * and some expressions that produce strings but whose output type is not based on the types of its
 * children.
 * Idea is to have this behave like a string with the default collation of the session, but that
 * we can still differentiate it from a regular string type, because in some places default string
 * is not the one with the session collation (e.g. in DDL commands).
 */
private[spark] class DefaultStringType extends StringType(CollationFactory.DEFAULT_COLLATION_ID) {
  override def collationId: Int = SqlApiConf.get.defaultStringType.collationId
}

private[spark] case object DefaultStringType extends DefaultStringType {
  def apply(): DefaultStringType = new DefaultStringType()
}

