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
import org.apache.spark.sql.internal.SQLConf.CHECKPOINT_LOCATION
import org.apache.spark.sql.internal.StaticSQLConf.SCHEMA_STRING_LENGTH_THRESHOLD

class RuntimeConfigSuite extends SparkFunSuite {

  private def newConf(): RuntimeConfig = new RuntimeConfig

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
    assert(!conf.isModifiable(SCHEMA_STRING_LENGTH_THRESHOLD.key))
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
}
