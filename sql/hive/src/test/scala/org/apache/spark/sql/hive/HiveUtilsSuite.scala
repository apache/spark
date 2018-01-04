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

package org.apache.spark.sql.hive

import java.net.URL

import org.apache.hadoop.hive.conf.HiveConf.ConfVars

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.util.{ChildFirstURLClassLoader, MutableURLClassLoader}

class HiveUtilsSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {

  test("newTemporaryConfiguration overwrites listener configurations") {
    Seq(true, false).foreach { useInMemoryDerby =>
      val conf = HiveUtils.newTemporaryConfiguration(useInMemoryDerby)
      assert(conf(ConfVars.METASTORE_PRE_EVENT_LISTENERS.varname) === "")
      assert(conf(ConfVars.METASTORE_EVENT_LISTENERS.varname) === "")
      assert(conf(ConfVars.METASTORE_END_FUNCTION_LISTENERS.varname) === "")
    }
  }

  test("newTemporaryConfiguration respect spark.hadoop.foo=bar in SparkConf") {
    sys.props.put("spark.hadoop.foo", "bar")
    Seq(true, false) foreach { useInMemoryDerby =>
      val hiveConf = HiveUtils.newTemporaryConfiguration(useInMemoryDerby)
      assert(!hiveConf.contains("spark.hadoop.foo"))
      assert(hiveConf("foo") === "bar")
    }
  }

  test("ChildFirstURLClassLoader's parent is null") {
    val conf = new SparkConf
    val contextClassLoader = Thread.currentThread().getContextClassLoader
    val loader = new FakeChildFirstURLClassLoader(Array(), contextClassLoader)
    try {
      Thread.currentThread().setContextClassLoader(loader)
      intercept[IllegalArgumentException](
        HiveUtils.newClientForMetadata(
          conf,
          SparkHadoopUtil.newConfiguration(conf),
          HiveUtils.newTemporaryConfiguration(useInMemoryDerby = true)))
    } finally {
      Thread.currentThread().setContextClassLoader(contextClassLoader)
    }
  }

  test("ChildFirstURLClassLoader's parent is null, get spark classloader instead") {
    val conf = new SparkConf
    val contextClassLoader = Thread.currentThread().getContextClassLoader
    val loader = new ChildFirstURLClassLoader(Array(), contextClassLoader)
    try {
      Thread.currentThread().setContextClassLoader(loader)
      HiveUtils.newClientForMetadata(
        conf,
        SparkHadoopUtil.newConfiguration(conf),
        HiveUtils.newTemporaryConfiguration(useInMemoryDerby = true))
    } finally {
      Thread.currentThread().setContextClassLoader(contextClassLoader)
    }
  }
}

/**
 * A Fake [[ChildFirstURLClassLoader]] used for test
 */
private[spark] class FakeChildFirstURLClassLoader(urls: Array[URL], parent: ClassLoader)
  extends MutableURLClassLoader(urls, null)
