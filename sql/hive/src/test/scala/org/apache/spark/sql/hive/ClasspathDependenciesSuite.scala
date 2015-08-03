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
 *
 * This is used to detect classpath and shading conflict, especially between
 * Spark's required Kryo version and that which can be found in some Hive versions.
 */
class ClasspathDependenciesSuite extends SparkFunSuite {

  val classloader = this.getClass.getClassLoader

  def assertLoads(classname: String): Unit = {
    val resourceURL: URL = findResource(classname)
    if( resourceURL == null) {
      fail(s"Class $classname not found as ${resourceName(classname)}")
    }
    logInfo(s"Class $classname at $resourceURL")
    val clazz = classloader.loadClass(classname)
  }

  def assertLoads(l: List[String]): Unit = {
    l.foreach(assertLoads(_))
  }

  def findResource(classname: String): URL = {
    val resource = resourceName(classname)
    classloader.getResource(resource)
  }

  def resourceName(classname: String): String = {
    classname.replace(".", "/") + ".class"
  }

  def assertClassNotFound(classname: String): Unit = {
    val resourceURL: URL = findResource(classname)
    if (resourceURL != null) {
      fail(s"Class $classname found at $resourceURL")
    }
    intercept[ClassNotFoundException] {
      classloader.loadClass(classname)
    }
  }

  def assertClassNotFound(l: List[String]): Unit = {
    l.foreach(assertClassNotFound(_))
  }

  val KRYO = "com.esotericsoftware.kryo.Kryo"

  val SPARK_HIVE = "org.apache.hive."
  val SPARK_SHADED = "org.spark-project.hive.shaded."


  test("shaded Protobuf") {
    assertLoads(SPARK_SHADED + "com.google.protobuf.ServiceException")
  }

  test("hive-common") {
    assertLoads("org.apache.hadoop.hive.conf.HiveConf")
  }

  test("hive-exec") {
    assertLoads("org.apache.hadoop.hive.ql.CommandNeedRetryException")
  }


  val STD_INSTANTIATOR = "org.objenesis.strategy.StdInstantiatorStrategy"
  test("unshaded kryo") {
    assertLoads(List(KRYO, STD_INSTANTIATOR))
  }

  test("Forbidden Depencencies") {
    assertClassNotFound(List(
      SPARK_HIVE + KRYO,
      SPARK_SHADED + KRYO,
      "org.apache.hive." + KRYO,
      "com.esotericsoftware.shaded." + STD_INSTANTIATOR,
      SPARK_HIVE + "com.esotericsoftware.shaded." + STD_INSTANTIATOR,
      "org.apache.hive.com.esotericsoftware.shaded." + STD_INSTANTIATOR
    ))
  }


}
