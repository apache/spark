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

package org.apache.spark.sql.hive.test

import java.io.File
import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets

import org.apache.spark.TestUtils
import org.apache.spark.util.Utils

/**
 * Provides a dynamically compiled JAR containing GenericUDTFCount2,
 * replacing the pre-built TestUDTF.jar that was previously checked into the repository.
 */
object TestUDTFJar {

  private def loadResource(name: String): String = {
    val url = Thread.currentThread().getContextClassLoader.getResource(name)
    assert(url != null, s"Resource not found: $name")
    Utils.tryWithResource(url.openStream()) { is =>
      new String(is.readAllBytes(), StandardCharsets.UTF_8)
    }
  }

  private val source = Map(
    "org.apache.spark.sql.hive.execution.GenericUDTFCount2" ->
      loadResource("hive-test-udfs/GenericUDTFCount2.java"))

  val jar: File = {
    val jarFile = File.createTempFile("TestUDTF", ".jar", Utils.createTempDir())
    val classpath = ManagementFactory.getRuntimeMXBean.getClassPath
      .split(File.pathSeparator).map(p => new File(p).toURI.toURL).toSeq
    TestUtils.createJarWithJavaSources(source, jarFile, classpath)
    jarFile
  }
}
