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

package org.apache.spark.sql.hive.client

import java.io.File
import java.net.URLClassLoader

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.hive.{HiveExternalCatalog, HiveUtils}
import org.apache.spark.util.Utils

/**
 * This test suite requires a clean JVM because it's testing the initialization of static codes in
 * `org.apache.hadoop.util.VersionInfo`.
 */
class HadoopVersionInfoSuite extends SparkFunSuite {
  override protected val enableAutoThreadAudit = false

  test("SPARK-32256: Hadoop VersionInfo should be preloaded") {
    val ivyPath =
      Utils.createTempDir(namePrefix = s"${classOf[HadoopVersionInfoSuite].getSimpleName}-ivy")
    try {
      val hadoopConf = new Configuration()
      hadoopConf.set("test", "success")
      hadoopConf.set("datanucleus.schema.autoCreateAll", "true")
      hadoopConf.set("hive.metastore.schema.verification", "false")

      // Download jars for Hive 2.0
      val client = IsolatedClientLoader.forVersion(
        hiveMetastoreVersion = "2.0",
        hadoopVersion = "2.7.4",
        sparkConf = new SparkConf(),
        hadoopConf = hadoopConf,
        config = HiveClientBuilder.buildConf(Map.empty),
        ivyPath = Some(ivyPath.getCanonicalPath))
      val jars = client.classLoader.getParent.asInstanceOf[URLClassLoader].getURLs
        .map(u => new File(u.toURI))
        // Drop all Hadoop jars to use the existing Hadoop jars on the classpath
        .filter(!_.getName.startsWith("org.apache.hadoop_hadoop-"))

      val sparkConf = new SparkConf()
      sparkConf.set(HiveUtils.HIVE_METASTORE_VERSION, "2.0")
      sparkConf.set(
        HiveUtils.HIVE_METASTORE_JARS,
        jars.map(_.getCanonicalPath).mkString(File.pathSeparator))
      HiveClientBuilder.buildConf(Map.empty).foreach { case (k, v) =>
        hadoopConf.set(k, v)
      }
      new HiveExternalCatalog(sparkConf, hadoopConf).client.getState
    } finally {
      Utils.deleteRecursively(ivyPath)
    }
  }
}
