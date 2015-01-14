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

package org.apache.spark

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.execution.SparkPlan

/**
 * Allows the execution of relational queries, including those expressed in SQL using Spark.
 *
 *  @groupname dataType Data types
 *  @groupdesc Spark SQL data types.
 *  @groupprio dataType -3
 *  @groupname field Field
 *  @groupprio field -2
 *  @groupname row Row
 *  @groupprio row -1
 */
package object sql {

  /**
   * :: DeveloperApi ::
   *
   * Represents one row of output from a relational operator.
   * @group row
   */
  @DeveloperApi
  type Row = catalyst.expressions.Row

  /**
   * :: DeveloperApi ::
   *
   * A [[Row]] object can be constructed by providing field values. Example:
   * {{{
   * import org.apache.spark.sql._
   *
   * // Create a Row from values.
   * Row(value1, value2, value3, ...)
   * // Create a Row from a Seq of values.
   * Row.fromSeq(Seq(value1, value2, ...))
   * }}}
   *
   * A value of a row can be accessed through both generic access by ordinal,
   * which will incur boxing overhead for primitives, as well as native primitive access.
   * An example of generic access by ordinal:
   * {{{
   * import org.apache.spark.sql._
   *
   * val row = Row(1, true, "a string", null)
   * // row: Row = [1,true,a string,null]
   * val firstValue = row(0)
   * // firstValue: Any = 1
   * val fourthValue = row(3)
   * // fourthValue: Any = null
   * }}}
   *
   * For native primitive access, it is invalid to use the native primitive interface to retrieve
   * a value that is null, instead a user must check `isNullAt` before attempting to retrieve a
   * value that might be null.
   * An example of native primitive access:
   * {{{
   * // using the row from the previous example.
   * val firstValue = row.getInt(0)
   * // firstValue: Int = 1
   * val isNull = row.isNullAt(3)
   * // isNull: Boolean = true
   * }}}
   *
   * Interfaces related to native primitive access are:
   *
   * `isNullAt(i: Int): Boolean`
   *
   * `getInt(i: Int): Int`
   *
   * `getLong(i: Int): Long`
   *
   * `getDouble(i: Int): Double`
   *
   * `getFloat(i: Int): Float`
   *
   * `getBoolean(i: Int): Boolean`
   *
   * `getShort(i: Int): Short`
   *
   * `getByte(i: Int): Byte`
   *
   * `getString(i: Int): String`
   *
   * Fields in a [[Row]] object can be extracted in a pattern match. Example:
   * {{{
   * import org.apache.spark.sql._
   *
   * val pairs = sql("SELECT key, value FROM src").rdd.map {
   *   case Row(key: Int, value: String) =>
   *     key -> value
   * }
   * }}}
   *
   * @group row
   */
  @DeveloperApi
  val Row = catalyst.expressions.Row

  /**
   * Converts a logical plan into zero or more SparkPlans.
   */
  @DeveloperApi
  type Strategy = org.apache.spark.sql.catalyst.planning.GenericStrategy[SparkPlan]
}
