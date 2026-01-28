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

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.util.CollationFactory

/**
 * A data type representing fixed-length character strings with a specified length.
 *
 * @param length
 *   The fixed length of the char string (must be non-negative)
 * @param collation
 *   Optional collation ID for string comparison and sorting. If None, uses
 *   UTF8_BINARY_COLLATION_ID. The reason for using an `Option` is to be able to see in the
 *   analyzer whether the collation was explicitly specified or not.
 */
@Experimental
case class CharType private[sql] (length: Int, collation: Option[Int])
    extends StringType(
      collation.getOrElse(CollationFactory.UTF8_BINARY_COLLATION_ID),
      FixedLength(length)) {
  require(length >= 0, "The length of char type cannot be negative.")

  override def defaultSize: Int = length
  override def typeName: String =
    if (isUTF8BinaryCollation) s"char($length)"
    else s"char($length) collate $collationName"
  override def toString: String =
    if (isUTF8BinaryCollation) s"CharType($length)"
    else s"CharType($length, $collationName)"
  private[spark] override def asNullable: CharType = this

  def toStringType: StringType = {
    if (collation.isEmpty) StringType
    else StringType(collationId)
  }
}

object CharType {
  def apply(length: Int): CharType = new CharType(length, None)

  def apply(length: Int, collationName: String): CharType = {
    val collationId = CollationFactory.collationNameToId(collationName)
    new CharType(length, Some(collationId))
  }

  def apply(length: Int, collationId: Int): CharType =
    new CharType(length, Some(collationId))
}
