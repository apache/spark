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

package org.apache.spark.sql.catalyst

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.unsafe.types.UTF8String

/**
 * An abstract class for row used internal in Spark SQL, which only contain the columns as
 * internal types.
 */
abstract class InternalRow extends Row {

  // This is only use for test
  override def getString(i: Int): String = {
    val str = getAs[UTF8String](i)
    if (str != null) str.toString else null
  }

  // These expensive API should not be used internally.
  final override def getDecimal(i: Int): java.math.BigDecimal =
    throw new UnsupportedOperationException
  final override def getDate(i: Int): java.sql.Date =
    throw new UnsupportedOperationException
  final override def getTimestamp(i: Int): java.sql.Timestamp =
    throw new UnsupportedOperationException
  final override def getSeq[T](i: Int): Seq[T] = throw new UnsupportedOperationException
  final override def getList[T](i: Int): java.util.List[T] = throw new UnsupportedOperationException
  final override def getMap[K, V](i: Int): scala.collection.Map[K, V] =
    throw new UnsupportedOperationException
  final override def getJavaMap[K, V](i: Int): java.util.Map[K, V] =
    throw new UnsupportedOperationException
  final override def getStruct(i: Int): Row = throw new UnsupportedOperationException
  final override def getAs[T](fieldName: String): T = throw new UnsupportedOperationException
  final override def getValuesMap[T](fieldNames: Seq[String]): Map[String, T] =
    throw new UnsupportedOperationException

  // A default implementation to change the return type
  override def copy(): InternalRow = this

  protected override def canEqual(other: Any) = other.isInstanceOf[InternalRow]
}

object InternalRow {
  /**
   * This method can be used to construct a [[Row]] with the given values.
   */
  def apply(values: Any*): InternalRow = new GenericInternalRow(values.toArray)

  /**
   * This method can be used to construct a [[Row]] from a [[Seq]] of values.
   */
  def fromSeq(values: Seq[Any]): InternalRow = new GenericInternalRow(values.toArray)

  /** Returns an empty row. */
  val empty = apply()
}
