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

import java.sql.{SQLFeatureNotSupportedException, Struct}
import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.connect.client.jdbc.util.JdbcTypeUtils
import org.apache.spark.sql.types.StructType

/**
 * A [[java.sql.Struct]] backed by a STRUCT value materialized by the Spark Connect client.
 * Attributes are converted to standard JDBC objects on access (nested ARRAY/MAP/STRUCT become
 * [[java.sql.Array]] / [[java.util.Map]] / [[java.sql.Struct]]).
 */
private[jdbc] class SparkConnectStruct(
    row: Row,
    structType: StructType) extends Struct {

  override def getSQLTypeName: String = structType.sql

  override def getAttributes: Array[AnyRef] =
    structType.fields.zipWithIndex.map { case (field, i) =>
      JdbcTypeUtils.toJdbcObject(row.get(i), field.dataType)
    }

  override def getAttributes(map: util.Map[String, Class[_]]): Array[AnyRef] =
    throw new SQLFeatureNotSupportedException

  override def toString: String = JdbcTypeUtils.toJson(row, structType)
}
