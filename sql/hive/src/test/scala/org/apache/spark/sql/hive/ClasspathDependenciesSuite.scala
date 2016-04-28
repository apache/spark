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

import org.apache.spark.SparkFunSuite

/**
 * Verify that some classes load and that others are not found on the classpath.
 *
 * This is used to detect classpath and shading conflicts.
 */
class ClasspathDependenciesSuite extends SparkFunSuite {
  private val classloader = this.getClass.getClassLoader

  private def assertLoads(classname: String): Unit = {
    val resourceURL: URL = Option(findResource(classname)).getOrElse {
      fail(s"Class $classname not found as ${resourceName(classname)}")
    }

    logInfo(s"Class $classname at $resourceURL")
    classloader.loadClass(classname)
  }

  private def findResource(classname: String): URL = {
    val resource = resourceName(classname)
    classloader.getResource(resource)
  }

  private def resourceName(classname: String): String = {
    classname.replace(".", "/") + ".class"
  }

  private def assertClassNotFound(classname: String): Unit = {
    Option(findResource(classname)).foreach { resourceURL =>
      fail(s"Class $classname found at $resourceURL")
    }

    intercept[ClassNotFoundException] {
      classloader.loadClass(classname)
    }
  }

  test("shaded Protobuf") {
    assertLoads("org.apache.hive.com.google.protobuf.ServiceException")
  }

  test("shaded Kryo") {
    assertLoads("org.apache.hive.com.esotericsoftware.kryo.Kryo")
  }

  test("hive-common") {
    assertLoads("org.apache.hadoop.hive.conf.HiveConf")
  }

  test("hive-exec") {
    assertLoads("org.apache.hadoop.hive.ql.CommandNeedRetryException")
  }

  private val STD_INSTANTIATOR = "org.objenesis.strategy.StdInstantiatorStrategy"

  test("Forbidden Dependencies") {
    assertClassNotFound("com.esotericsoftware.shaded." + STD_INSTANTIATOR)
    assertClassNotFound("org.apache.hive.com.esotericsoftware.shaded." + STD_INSTANTIATOR)
  }

  test("parquet-hadoop-bundle") {
    assertLoads("parquet.hadoop.ParquetOutputFormat")
    assertLoads("parquet.hadoop.ParquetInputFormat")
  }
}
