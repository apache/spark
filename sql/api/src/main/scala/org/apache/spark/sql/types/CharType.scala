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

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.util.CollationFactory

@Experimental
case class CharType(length: Int, override val collationId: Int = 0)
  extends StringType(collationId, FixedLength(length)) {
  require(length >= 0, "The length of char type cannot be negative.")

  override def defaultSize: Int = length
  override def typeName: String =
    if (isUTF8BinaryCollation) s"char($length)"
    else s"char($length) collate $collationName"
  override def jsonValue: JValue = JString(s"char($length)")
  override def toString: String =
    if (isUTF8BinaryCollation) s"CharType($length)"
    else s"CharType($length, $collationName)"
  private[spark] override def asNullable: CharType = this
}

/**
 * A variant of [[CharType]] defined without explicit collation.
 */
@Experimental
class DefaultCharType(override val length: Int)
  extends CharType(length, CollationFactory.UTF8_BINARY_COLLATION_ID) {
  override def typeName: String = s"char($length) collate $collationName"
  override def toString: String = s"CharType($length, $collationName)"
}

@Experimental
object DefaultCharType {
  def apply(length: Int): DefaultCharType = new DefaultCharType(length)
  def unapply(defaultCharType: DefaultCharType): Option[Int] = Some(defaultCharType.length)
}
