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

import org.apache.spark.util.{SparkTestUtils, Utils}

/**
 * Generates hive-test-udfs.jar at test runtime from Java source files
 * under src/test/resources/hive-test-udfs/.
 *
 * The original sources are from:
 * https://github.com/HeartSaVioR/hive/commit/12f3f036b6efd0299cd1d457c0c0a65e0fd7e5f2
 *
 * The JAR is placed in a temp directory (NOT on classpath) because
 * HiveUDFDynamicLoadSuite tests dynamic JAR loading via `USING JAR`.
 */
object TestHiveUdfsJar {

  lazy val jar: File = {
    val dir = Utils.createTempDir()
    val jarFile = new File(dir, "hive-test-udfs.jar")
    val cp = ManagementFactory.getRuntimeMXBean.getClassPath
      .split(File.pathSeparator).map(p => new File(p).toURI.toURL).toSeq
    SparkTestUtils.createJarWithJavaSources(sources, jarFile, cp)
    jarFile
  }

  private val sources: Map[String, String] = {
    val resourceDir = "hive-test-udfs"
    Map(
      "org.apache.hadoop.hive.contrib.udf.example.UDFExampleAdd2" ->
        readResource(s"$resourceDir/UDFExampleAdd2.java"),
      "org.apache.hadoop.hive.contrib.udf.example.GenericUDFTrim2" ->
        readResource(s"$resourceDir/GenericUDFTrim2.java"),
      "org.apache.hadoop.hive.contrib.udtf.example.GenericUDTFCount3" ->
        readResource(s"$resourceDir/GenericUDTFCount3.java"),
      "org.apache.hadoop.hive.contrib.udaf.example.UDAFExampleMax2" ->
        readResource(s"$resourceDir/UDAFExampleMax2.java"),
      "org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum2" ->
        readResource(s"$resourceDir/GenericUDAFSum2.java")
    )
  }

  private def readResource(name: String): String = {
    val url = Thread.currentThread().getContextClassLoader.getResource(name)
    assert(url != null, s"Resource not found: $name")
    Utils.tryWithResource(url.openStream()) { is =>
      new String(is.readAllBytes(), StandardCharsets.UTF_8)
    }
  }
}
