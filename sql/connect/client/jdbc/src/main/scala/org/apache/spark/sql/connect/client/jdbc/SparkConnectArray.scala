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

package org.apache.spark.sql.connect.client.jdbc

import java.sql.{Array => JdbcArray, _}
import java.util

class SparkConnectArray(
   elementTypeName: String, elementType: Int, array: Array[AnyRef]) extends JdbcArray {

  override def getBaseTypeName: String = elementTypeName

  override def getBaseType: Int = elementType

  override def getArray: AnyRef = array.clone

  override def getArray(map: util.Map[String, Class[_]]): AnyRef =
    throw new SQLFeatureNotSupportedException("getArray not supported")

  override def getArray(index: Long, count: Int): AnyRef = {
    val arrayOffset = index - 1
    if (index < 1 || count < 0 || (arrayOffset + count) > array.length) {
      throw new SQLException("Index out of bounds")
    }
    util.Arrays.copyOfRange(array, arrayOffset.toInt, arrayOffset.toInt + count)
  }

  override def getArray(index: Long, count: Int, map: util.Map[String, Class[_]]): AnyRef =
    throw new SQLFeatureNotSupportedException("getArray not supported")

  override def getResultSet: ResultSet =
    throw new SQLFeatureNotSupportedException("getResultSet not supported")

  override def getResultSet(map: util.Map[String, Class[_]]): ResultSet =
    throw new SQLFeatureNotSupportedException("getResultSet not supported")

  override def getResultSet(index: Long, count: Int): ResultSet =
    throw new SQLFeatureNotSupportedException("getResultSet not supported")

  override def getResultSet(index: Long, count: Int, map: util.Map[String, Class[_]]): ResultSet =
    throw new SQLFeatureNotSupportedException("getResultSet not supported")

  override def free(): Unit = {}

  override def toString: String = util.Arrays.toString(array)

}
