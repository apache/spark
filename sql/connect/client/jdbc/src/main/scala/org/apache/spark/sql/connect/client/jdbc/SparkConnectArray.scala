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

import java.sql.{Array => JdbcArray, ResultSet, SQLFeatureNotSupportedException}
import java.util

import org.apache.spark.sql.connect.client.jdbc.util.JdbcTypeUtils
import org.apache.spark.sql.types.{ArrayType, DataType}

/**
 * A [[java.sql.Array]] backed by an ARRAY value materialized by the Spark Connect client.
 * Elements are converted to standard JDBC objects on access (nested ARRAY/MAP/STRUCT become
 * [[java.sql.Array]] / [[java.util.Map]] / [[java.sql.Struct]]).
 */
private[jdbc] class SparkConnectArray(
    elements: scala.collection.Seq[Any],
    elementType: DataType) extends JdbcArray {

  override def getBaseTypeName: String = elementType.sql

  override def getBaseType: Int = JdbcTypeUtils.getColumnType(elementType)

  override def getArray: AnyRef =
    elements.map(JdbcTypeUtils.toJdbcObject(_, elementType)).toArray

  override def getArray(map: util.Map[String, Class[_]]): AnyRef =
    throw new SQLFeatureNotSupportedException

  override def getArray(index: Long, count: Int): AnyRef =
    throw new SQLFeatureNotSupportedException

  override def getArray(index: Long, count: Int, map: util.Map[String, Class[_]]): AnyRef =
    throw new SQLFeatureNotSupportedException

  override def getResultSet: ResultSet =
    throw new SQLFeatureNotSupportedException

  override def getResultSet(map: util.Map[String, Class[_]]): ResultSet =
    throw new SQLFeatureNotSupportedException

  override def getResultSet(index: Long, count: Int): ResultSet =
    throw new SQLFeatureNotSupportedException

  override def getResultSet(index: Long, count: Int, map: util.Map[String, Class[_]]): ResultSet =
    throw new SQLFeatureNotSupportedException

  override def free(): Unit = ()

  override def toString: String = JdbcTypeUtils.toJson(elements, ArrayType(elementType))
}
