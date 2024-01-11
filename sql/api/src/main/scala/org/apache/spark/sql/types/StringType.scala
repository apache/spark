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

/**
 * The data type representing `String` values. Please use the singleton `DataTypes.StringType`.
 *
 * @since 1.3.0
 */
@Stable
class StringType private(val collation: String) extends AtomicType with Serializable {
  def isDefaultCollation: Boolean = collation == "default"

  override def toString: String = if (this.isDefaultCollation) "String" else s"String($collation)"
  override def typeName: String = if (this.isDefaultCollation) "string" else s"string($collation)"

  /**
   * The default size of a value of the StringType is 20 bytes.
   */
  override def defaultSize: Int = 20

  private[spark] override def asNullable: StringType = this

  override def equals(obj: Any): Boolean =
    obj.isInstanceOf[StringType] && obj.asInstanceOf[StringType].collation == collation

  override def hashCode(): Int = collation.hashCode
}
/**
 * @since 1.3.0
 */
@Stable
case object StringType extends StringType("default") {
  def apply(collation: String): StringType = new StringType(collation)
}
