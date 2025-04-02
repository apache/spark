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
case class CharType(length: Int)
    extends StringType(CollationFactory.UTF8_BINARY_COLLATION_ID, FixedLength(length)) {
  require(length >= 0, "The length of char type cannot be negative.")

  override def defaultSize: Int = length
  override def typeName: String = s"char($length)"
  override def jsonValue: JValue = JString(typeName)
  override def toString: String = s"CharType($length)"
  private[spark] override def asNullable: CharType = this
}
