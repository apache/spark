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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

/**
 * Options for the data source.
 */
class SourceOptions(
     @transient private val parameters: CaseInsensitiveMap[String])
  extends Serializable {
  import SourceOptions._

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  // A flag to disable saving a data source table's metadata in hive compatible way.
  val skipHiveMetadata: Boolean = parameters
    .get(SKIP_HIVE_METADATA).map(_.toBoolean).getOrElse(DEFAULT_SKIP_HIVE_METADATA)

  // A flag to always respect the Spark schema restored from the table properties
  val respectSparkSchema: Boolean = parameters
    .get(RESPECT_SPARK_SCHEMA).map(_.toBoolean).getOrElse(DEFAULT_RESPECT_SPARK_SCHEMA)
}


object SourceOptions {

  val SKIP_HIVE_METADATA = "skipHiveMetadata"
  val DEFAULT_SKIP_HIVE_METADATA = false

  val RESPECT_SPARK_SCHEMA = "respectSparkSchema"
  val DEFAULT_RESPECT_SPARK_SCHEMA = false

}
