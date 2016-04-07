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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalColumn
import org.apache.spark.sql.types._

/**
 * An extended interface to [[InternalColumn]] that allows the values for each row to be updated.
 * Setting a value through a primitive function implicitly marks that row as not null.
 */
abstract class MutableColumn extends InternalColumn {
  def setNullAt(i: Int): Unit

  def update(i: Int, value: Any)

  // default implementation (slow)
  def setBoolean(i: Int, value: Boolean): Unit = { update(i, value) }
  def setByte(i: Int, value: Byte): Unit = { update(i, value) }
  def setShort(i: Int, value: Short): Unit = { update(i, value) }
  def setInt(i: Int, value: Int): Unit = { update(i, value) }
  def setLong(i: Int, value: Long): Unit = { update(i, value) }
  def setFloat(i: Int, value: Float): Unit = { update(i, value) }
  def setDouble(i: Int, value: Double): Unit = { update(i, value) }

  /**
   * Update the decimal column at `i`.
   *
   * Note: In order to support update decimal with precision > 18 in UnsafeColumn,
   * CAN NOT call setNullAt() for decimal column on UnsafeColumn,
   * call setDecimal(i, null, precision).
   */
  def setDecimal(i: Int, value: Decimal, precision: Int) { update(i, value) }
}
