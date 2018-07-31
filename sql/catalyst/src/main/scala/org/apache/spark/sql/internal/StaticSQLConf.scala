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

import org.apache.spark.util.Utils


/**
 * Static SQL configuration is a cross-session, immutable Spark configuration. External users can
 * see the static sql configs via `SparkSession.conf`, but can NOT set/unset them.
 */
object StaticSQLConf {

  import SQLConf.buildStaticConf

  val WAREHOUSE_PATH = buildStaticConf("spark.sql.warehouse.dir")
    .doc("The default location for managed databases and tables.")
    .stringConf
    .createWithDefault(Utils.resolveURI("spark-warehouse").toString)

  val CATALOG_IMPLEMENTATION = buildStaticConf("spark.sql.catalogImplementation")
    .internal()
    .stringConf
    .checkValues(Set("hive", "in-memory"))
    .createWithDefault("in-memory")

  val GLOBAL_TEMP_DATABASE = buildStaticConf("spark.sql.globalTempDatabase")
    .internal()
    .stringConf
    .createWithDefault("global_temp")

  // This is used to control when we will split a schema's JSON string to multiple pieces
  // in order to fit the JSON string in metastore's table property (by default, the value has
  // a length restriction of 4000 characters, so do not use a value larger than 4000 as the default
  // value of this property). We will split the JSON string of a schema to its length exceeds the
  // threshold. Note that, this conf is only read in HiveExternalCatalog which is cross-session,
  // that's why this conf has to be a static SQL conf.
  val SCHEMA_STRING_LENGTH_THRESHOLD =
    buildStaticConf("spark.sql.sources.schemaStringLengthThreshold")
      .doc("The maximum length allowed in a single cell when " +
        "storing additional schema information in Hive's metastore.")
      .internal()
      .intConf
      .createWithDefault(4000)

  val FILESOURCE_TABLE_RELATION_CACHE_SIZE =
    buildStaticConf("spark.sql.filesourceTableRelationCacheSize")
      .internal()
      .doc("The maximum size of the cache that maps qualified table names to table relation plans.")
      .intConf
      .checkValue(cacheSize => cacheSize >= 0, "The maximum size of the cache must not be negative")
      .createWithDefault(1000)

  val CODEGEN_CACHE_MAX_ENTRIES = buildStaticConf("spark.sql.codegen.cache.maxEntries")
      .internal()
      .doc("When nonzero, enable caching of generated classes for operators and expressions. " +
        "All jobs share the cache that can use up to the specified number for generated classes.")
      .intConf
      .checkValue(maxEntries => maxEntries >= 0, "The maximum must not be negative")
      .createWithDefault(100)

  val CODEGEN_COMMENTS = buildStaticConf("spark.sql.codegen.comments")
    .internal()
    .doc("When true, put comment in the generated code. Since computing huge comments " +
      "can be extremely expensive in certain cases, such as deeply-nested expressions which " +
      "operate over inputs with wide schemas, default is false.")
    .booleanConf
    .createWithDefault(false)

  // When enabling the debug, Spark SQL internal table properties are not filtered out; however,
  // some related DDL commands (e.g., ANALYZE TABLE and CREATE TABLE LIKE) might not work properly.
  val DEBUG_MODE = buildStaticConf("spark.sql.debug")
    .internal()
    .doc("Only used for internal debugging. Not all functions are supported when it is enabled.")
    .booleanConf
    .createWithDefault(false)

  val HIVE_THRIFT_SERVER_SINGLESESSION =
    buildStaticConf("spark.sql.hive.thriftServer.singleSession")
      .doc("When set to true, Hive Thrift server is running in a single session mode. " +
        "All the JDBC/ODBC connections share the temporary views, function registries, " +
        "SQL configuration and the current database.")
      .booleanConf
      .createWithDefault(false)

  val SPARK_SESSION_EXTENSIONS = buildStaticConf("spark.sql.extensions")
    .doc("Name of the class used to configure Spark Session extensions. The class should " +
      "implement Function1[SparkSessionExtension, Unit], and must have a no-args constructor.")
    .stringConf
    .createOptional

  val QUERY_EXECUTION_LISTENERS = buildStaticConf("spark.sql.queryExecutionListeners")
    .doc("List of class names implementing QueryExecutionListener that will be automatically " +
      "added to newly created sessions. The classes should have either a no-arg constructor, " +
      "or a constructor that expects a SparkConf argument.")
    .stringConf
    .toSequence
    .createOptional

  val STREAMING_QUERY_LISTENERS = buildStaticConf("spark.sql.streaming.streamingQueryListeners")
    .doc("List of class names implementing StreamingQueryListener that will be automatically " +
      "added to newly created sessions. The classes should have either a no-arg constructor, " +
      "or a constructor that expects a SparkConf argument.")
    .stringConf
    .toSequence
    .createOptional

  val UI_RETAINED_EXECUTIONS =
    buildStaticConf("spark.sql.ui.retainedExecutions")
      .doc("Number of executions to retain in the Spark UI.")
      .intConf
      .createWithDefault(1000)

}
