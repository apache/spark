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

package org.apache.spark.sql.api.java

import org.apache.spark.sql.catalyst.expressions.Row

/**
 * A result row from a SparkSQL query.
 */
class JavaRow(row: Row) {

  def length: Int = row.length

  def get(i: Int): Any =
    row(i)

  def isNullAt(i: Int) = get(i) == null

  def getInt(i: Int): Int =
    row.getInt(i)

  def getLong(i: Int): Long =
    row.getLong(i)

  def getDouble(i: Int): Double =
    row.getDouble(i)

  def getBoolean(i: Int): Boolean =
    row.getBoolean(i)

  def getShort(i: Int): Short =
    row.getShort(i)

  def getByte(i: Int): Byte =
    row.getByte(i)

  def getFloat(i: Int): Float =
    row.getFloat(i)

  def getString(i: Int): String =
    row.getString(i)
}

