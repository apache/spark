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

package org.apache.spark.sql.internal

import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.util.Utils


/**
 * Static SQL configuration is a cross-session, immutable Spark configuration. External users can
 * see the static sql configs via `SparkSession.conf`, but can NOT set/unset them.
 *//**
 * Static SQL configuration is a cross-session, immutable Spark configuration. External users can
 * see the static sql configs via `SparkSession.conf`, but can NOT set/unset them.
 */
object StaticSQLConf {
  val globalConfKeys = java.util.Collections.synchronizedSet(new java.util.HashSet[String]())

  private def buildConf(key: String): ConfigBuilder = {
    ConfigBuilder(key).onCreate { entry =>
      globalConfKeys.add(entry.key)
      SQLConf.register(entry)
    }
  }

  val WAREHOUSE_PATH = buildConf("spark.sql.warehouse.dir")
    .doc("The default location for managed databases and tables.")
    .stringConf
    .createWithDefault(Utils.resolveURI("spark-warehouse").toString)

  val CATALOG_IMPLEMENTATION = buildConf("spark.sql.catalogImplementation")
    .internal()
    .stringConf
    .checkValues(Set("hive", "in-memory"))
    .createWithDefault("in-memory")

  val GLOBAL_TEMP_DATABASE = buildConf("spark.sql.globalTempDatabase")
    .internal()
    .stringConf
    .createWithDefault("global_temp")

  // This is used to control when we will split a schema's JSON string to multiple pieces
  // in order to fit the JSON string in metastore's table property (by default, the value has
  // a length restriction of 4000 characters, so do not use a value larger than 4000 as the default
  // value of this property). We will split the JSON string of a schema to its length exceeds the
  // threshold. Note that, this conf is only read in HiveExternalCatalog which is cross-session,
  // that's why this conf has to be a static SQL conf.
  val SCHEMA_STRING_LENGTH_THRESHOLD = buildConf("spark.sql.sources.schemaStringLengthThreshold")
    .doc("The maximum length allowed in a single cell when " +
      "storing additional schema information in Hive's metastore.")
    .internal()
    .intConf
    .createWithDefault(4000)

  // When enabling the debug, Spark SQL internal table properties are not filtered out; however,
  // some related DDL commands (e.g., ANALYZE TABLE and CREATE TABLE LIKE) might not work properly.
  val DEBUG_MODE = buildConf("spark.sql.debug")
    .internal()
    .doc("Only used for internal debugging. Not all functions are supported when it is enabled.")
    .booleanConf
    .createWithDefault(false)
}
