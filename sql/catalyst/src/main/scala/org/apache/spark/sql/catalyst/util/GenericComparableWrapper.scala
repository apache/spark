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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.sql.catalyst.expressions.Murmur3HashFunction
import org.apache.spark.sql.types.DataType

/**
 * Wraps any internal Spark type with the corresponding [[DataType]] to make it comparable with
 * other values.
 * It uses Spark's internal murmur hash to compute hash code, and uses PhysicalDataType ordering
 * to perform equality checks.
 *
 * @param dataType the data type for the value
 */
class GenericComparableWrapper private (
    val value: Any,
    val dataType: DataType,
    val ordering: Ordering[Any]) {

  override def hashCode(): Int = Murmur3HashFunction.hash(
    value,
    dataType,
    42L,
    isCollationAware = true,
    // legacyCollationAwareHashing only matters when isCollationAware is false.
    legacyCollationAwareHashing = false).toInt

  override def equals(other: Any): Boolean = {
    if (!other.isInstanceOf[GenericComparableWrapper]) {
      return false
    }
    val otherWrapper = other.asInstanceOf[GenericComparableWrapper]
    if (!otherWrapper.dataType.equals(this.dataType)) {
      return false
    }
    ordering.equiv(value, otherWrapper.value)
  }
}

object GenericComparableWrapper {
  /** Creates a shared factory method for a given data type */
  def getGenericComparableWrapperFactory(
      dataType: DataType): Any => GenericComparableWrapper = {
    val ordering = TypeUtils.getInterpretedOrdering(dataType)
    value: Any => new GenericComparableWrapper(value, dataType, ordering)
  }
}
