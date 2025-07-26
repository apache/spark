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
case class VarcharType(length: Int, override val collationId: Int = 0)
  extends StringType(collationId, MaxLength(length)) {
  require(length >= 0, "The length of varchar type cannot be negative.")

  override def defaultSize: Int = length
  override def typeName: String =
    if (isUTF8BinaryCollation) s"varchar($length)"
    else s"varchar($length) collate $collationName"
  /** [[jsonValue]] does not have collation, same as for [[StringType]] */
  override def jsonValue: JValue = JString(s"varchar($length)")
  override def toString: String =
    if (isUTF8BinaryCollation) s"VarcharType($length)"
    else s"VarcharType($length, $collationName)"
  private[spark] override def asNullable: VarcharType = this
}

/**
 * A variant of [[VarcharType]] defined without explicit collation.
 */
@Experimental
class DefaultVarcharType(override val length: Int)
  extends VarcharType(length, CollationFactory.UTF8_BINARY_COLLATION_ID) {
  override def typeName: String = s"varchar($length) collate $collationName"
  override def toString: String = s"VarcharType($length, $collationName)"
}

@Experimental
object DefaultVarcharType {
  def apply(length: Int): DefaultVarcharType = new DefaultVarcharType(length)
  def unapply(defaultVarcharType: DefaultVarcharType): Option[Int] = Some(defaultVarcharType.length)
}
