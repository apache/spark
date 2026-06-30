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

import org.apache.spark.sql.connect.client.jdbc.util.JdbcTypeUtils
import org.apache.spark.sql.types.MapType

/**
 * A [[java.util.Map]] backed by a MAP value materialized by the Spark Connect client. JDBC has
 * no dedicated type for maps, so getObject returns a [[java.util.Map]]; entries are converted to
 * standard JDBC objects (nested ARRAY/MAP/STRUCT become [[java.sql.Array]] / [[java.util.Map]] /
 * [[java.sql.Struct]]). toString renders JSON, consistent with [[SparkConnectArray]] and
 * [[SparkConnectStruct]].
 */
private[jdbc] class SparkConnectMap(
    entries: scala.collection.Map[Any, Any],
    mapType: MapType) extends java.util.LinkedHashMap[AnyRef, AnyRef] {

  entries.foreach { case (k, v) =>
    put(
      JdbcTypeUtils.toJdbcObject(k, mapType.keyType),
      JdbcTypeUtils.toJdbcObject(v, mapType.valueType))
  }

  override def toString: String = JdbcTypeUtils.toJson(entries, mapType)
}
