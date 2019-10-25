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

package org.apache.spark.sql.hive.thriftserver

import test.custom.listener.{DummyQueryExecutionListener, DummyStreamingQueryListener}

import org.apache.spark.SparkFunSuite
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.sql.hive.HiveUtils.{HIVE_METASTORE_JARS, HIVE_METASTORE_VERSION}
import org.apache.spark.sql.hive.test.TestHiveContext
import org.apache.spark.sql.internal.StaticSQLConf.{QUERY_EXECUTION_LISTENERS, STREAMING_QUERY_LISTENERS, WAREHOUSE_PATH}

class SparkSQLEnvSuite extends SparkFunSuite {
  test("SPARK-29604 external listeners should be initialized with Spark classloader") {
    withSystemProperties(
      // Intentionally place listeners to the out of spark package, because IsolatedClientLoader
      // leverages Spark classloader for shared classess including spark package.
      QUERY_EXECUTION_LISTENERS.key -> classOf[DummyQueryExecutionListener].getCanonicalName,
      STREAMING_QUERY_LISTENERS.key -> classOf[DummyStreamingQueryListener].getCanonicalName,
      WAREHOUSE_PATH.key -> TestHiveContext.makeWarehouseDir().toURI.getPath,
      // The issue occured from "maven" and list of custom jars, but providing list of custom
      // jars to initialize HiveClient isn't trivial, so just use "maven".
      HIVE_METASTORE_JARS.key -> "maven",
      HIVE_METASTORE_VERSION.key -> null,
      SparkLauncher.SPARK_MASTER -> "local[2]",
      "spark.app.name" -> "testApp") {

      try {
        SparkSQLEnv.init()
      } finally {
        SparkSQLEnv.stop()
      }
    }
  }

  private def withSystemProperties(pairs: (String, String)*)(f: => Unit): Unit = {
    val oldValues = pairs.map { kv => kv._1 -> System.getProperty(kv._1) }.toMap
    try {
      pairs.foreach { case (key, value) =>
        if (value != null) {
          System.setProperty(key, value)
        } else {
          System.clearProperty(key)
        }
      }
      f
    } finally {
      oldValues.foreach { case (key, value) =>
        if (value != null) {
          System.setProperty(key, value)
        } else {
          System.clearProperty(key)
        }
      }
    }
  }
}
