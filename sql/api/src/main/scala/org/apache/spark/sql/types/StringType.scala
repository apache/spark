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

import org.apache.spark.annotation.Stable
import org.apache.spark.sql.catalyst.util.CollationFactory

/**
 * The data type representing `String` values. Please use the singleton `DataTypes.StringType`.
 *
 * @since 1.3.0
 * @param collationId The id of collation for this StringType.
 */
@Stable
class StringType private(val collationId: Int) extends AtomicType with Serializable {
  /**
   * Returns whether assigned collation is the default spark collation (UCS_BASIC).
   */
  def isDefaultCollation: Boolean = collationId == CollationFactory.DEFAULT_COLLATION_ID

  /**
   * Binary collation implies that strings are considered equal only if they are
   * byte for byte equal. E.g. all accent or case-insensitive collations are considered non-binary.
   * If this field is true, byte level operations can be used against this datatype (e.g. for
   * equality and hashing).
   */
  def isBinaryCollation: Boolean = CollationFactory.fetchCollation(collationId).isBinaryCollation

  /**
   * Type name that is shown to the customer.
   * If this is an UCS_BASIC collation output is `string` due to backwards compatibility.
   */
  override def typeName: String =
    if (isDefaultCollation) "string"
    else s"string COLLATE '${CollationFactory.fetchCollation(collationId).collationName}'"

  override def equals(obj: Any): Boolean =
    obj.isInstanceOf[StringType] && obj.asInstanceOf[StringType].collationId == collationId

  override def hashCode(): Int = collationId.hashCode()

  override private[sql] def acceptsType(other: DataType): Boolean = other.isInstanceOf[StringType]

  /**
   * The default size of a value of the StringType is 20 bytes.
   */
  override def defaultSize: Int = 20

  private[spark] override def asNullable: StringType = this
}

/**
 * @since 1.3.0
 */
@Stable
case object StringType extends StringType(0) {
  def apply(collationId: Int): StringType = new StringType(collationId)
}
