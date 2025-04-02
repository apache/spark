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

package org.apache.spark.sql.api.python

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}

class PythonSQLUtilsSuite extends SparkFunSuite {

  test("listing sql configurations contains runtime ones only") {
    val configs = PythonSQLUtils.listRuntimeSQLConfigs()

    // static sql configurations
    assert(!configs.exists(entry => entry._1 == StaticSQLConf.SPARK_SESSION_EXTENSIONS.key),
      "listSQLConfigs should contain public static sql configuration")
    assert(!configs.exists(entry => entry._1 == StaticSQLConf.DEBUG_MODE.key),
      "listSQLConfigs should not contain internal static sql configuration")

    // dynamic sql configurations
    assert(configs.exists(entry => entry._1 == SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key),
      "listSQLConfigs should contain public dynamic sql configuration")
    assert(!configs.exists(entry => entry._1 == SQLConf.ANALYZER_MAX_ITERATIONS.key),
      "listSQLConfigs should not contain internal dynamic sql configuration")

    // spark core configurations
    assert(!configs.exists(entry => entry._1 == "spark.master"),
      "listSQLConfigs should not contain core configuration")
  }

  test("listing static sql configurations contains public static ones only") {
    val configs = PythonSQLUtils.listStaticSQLConfigs()

    // static sql configurations
    assert(configs.exists(entry => entry._1 == StaticSQLConf.SPARK_SESSION_EXTENSIONS.key),
      "listStaticSQLConfigs should contain public static sql configuration")
    assert(!configs.exists(entry => entry._1 == StaticSQLConf.DEBUG_MODE.key),
      "listStaticSQLConfigs should not contain internal static sql configuration")

    // dynamic sql configurations
    assert(!configs.exists(entry => entry._1 == SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key),
      "listStaticSQLConfigs should not contain dynamic sql configuration")
    assert(!configs.exists(entry => entry._1 == SQLConf.ANALYZER_MAX_ITERATIONS.key),
      "listStaticSQLConfigs should not contain internal dynamic sql configuration")

    // spark core configurations
    assert(!configs.exists(entry => entry._1 == "spark.master"),
      "listStaticSQLConfigs should not contain core configuration")
  }
}
