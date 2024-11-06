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

package org.apache.spark.sql

import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.config
import org.apache.spark.internal.config.DEFAULT_PARALLELISM
import org.apache.spark.sql.internal.{RuntimeConfigImpl, SQLConf}
import org.apache.spark.sql.internal.SQLConf.CHECKPOINT_LOCATION
import org.apache.spark.sql.internal.StaticSQLConf.GLOBAL_TEMP_DATABASE

class RuntimeConfigSuite extends SparkFunSuite {

  private def newConf(): RuntimeConfig = new RuntimeConfigImpl()

  test("set and get") {
    val conf = newConf()
    conf.set("k1", "v1")
    conf.set("k2", 2)
    conf.set("k3", value = false)

    assert(conf.get("k1") == "v1")
    assert(conf.get("k2") == "2")
    assert(conf.get("k3") == "false")

    intercept[NoSuchElementException] {
      conf.get("notset")
    }
  }

  test("getOption") {
    val conf = newConf()
    conf.set("k1", "v1")
    assert(conf.getOption("k1") == Some("v1"))
    assert(conf.getOption("notset") == None)
  }

  test("unset") {
    val conf = newConf()
    conf.set("k1", "v1")
    assert(conf.get("k1") == "v1")
    conf.unset("k1")
    intercept[NoSuchElementException] {
      conf.get("k1")
    }
  }

  test("SPARK-24761: is a config parameter modifiable") {
    val conf = newConf()

    // SQL configs
    assert(!conf.isModifiable(GLOBAL_TEMP_DATABASE.key))
    assert(conf.isModifiable(CHECKPOINT_LOCATION.key))
    // Core configs
    assert(!conf.isModifiable(config.CPUS_PER_TASK.key))
    assert(!conf.isModifiable("spark.executor.cores"))
    // Invalid config parameters
    assert(!conf.isModifiable(""))
    assert(!conf.isModifiable("invalid config parameter"))
  }

  test("reject SparkConf entries") {
    val conf = newConf()

    val ex = intercept[AnalysisException] {
      conf.set(config.CPUS_PER_TASK.key, 4)
    }
    assert(ex.getMessage.contains("Spark config"))
  }

  test("set and get a config with defaultValue") {
    val conf = newConf()
    val key = SQLConf.SESSION_LOCAL_TIMEZONE.key
    // By default, the value when getting an unset config entry is its defaultValue.
    assert(conf.get(key) == SQLConf.SESSION_LOCAL_TIMEZONE.defaultValue.get)
    assert(conf.getOption(key).contains(SQLConf.SESSION_LOCAL_TIMEZONE.defaultValue.get))
    // Get the unset config entry with a different default value, which should return the given
    // default parameter.
    assert(conf.get(key, "Europe/Amsterdam") == "Europe/Amsterdam")

    // Set a config entry.
    conf.set(key, "Europe/Berlin")
    // Get the set config entry.
    assert(conf.get(key) == "Europe/Berlin")
    // Unset the config entry.
    conf.unset(key)
    // Get the unset config entry, which should return its defaultValue again.
    assert(conf.get(key) == SQLConf.SESSION_LOCAL_TIMEZONE.defaultValue.get)
  }

  test("SPARK-48773: set spark.default.parallelism does not fail") {
    val conf = newConf()
    // this set should not fail
    conf.set(DEFAULT_PARALLELISM.key, "1")
  }
}
