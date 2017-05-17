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
package org.apache.spark.deploy.kubernetes.submit.v2

import org.apache.spark.SparkFunSuite

class ContainerLocalizedFilesResolverSuite extends SparkFunSuite {
  private val SPARK_JARS = Seq(
    "hdfs://localhost:9000/app/jars/jar1.jar",
    "file:///app/jars/jar2.jar",
    "local:///app/jars/jar3.jar",
    "http://app/jars/jar4.jar")
  private val SPARK_FILES = Seq(
    "hdfs://localhost:9000/app/files/file1.txt",
    "file:///app/files/file2.txt",
    "local:///app/files/file3.txt",
    "http://app/files/file4.txt")
  private val JARS_DOWNLOAD_PATH = "/var/data/spark-jars"
  private val FILES_DOWNLOAD_PATH = "/var/data/spark-files"
  private val localizedFilesResolver = new ContainerLocalizedFilesResolverImpl(
    SPARK_JARS,
    SPARK_FILES,
    JARS_DOWNLOAD_PATH,
    FILES_DOWNLOAD_PATH)

  test("Submitted and remote Spark jars should resolve non-local uris to download path.") {
    val resolvedJars = localizedFilesResolver.resolveSubmittedAndRemoteSparkJars()
    val expectedResolvedJars = Seq(
      s"$JARS_DOWNLOAD_PATH/jar1.jar",
      s"$JARS_DOWNLOAD_PATH/jar2.jar",
      "/app/jars/jar3.jar",
      s"$JARS_DOWNLOAD_PATH/jar4.jar")
    assert(resolvedJars === expectedResolvedJars)
  }

  test("Submitted Spark jars should resolve to the download path.") {
    val resolvedJars = localizedFilesResolver.resolveSubmittedSparkJars()
    val expectedResolvedJars = Seq(
      "hdfs://localhost:9000/app/jars/jar1.jar",
      s"$JARS_DOWNLOAD_PATH/jar2.jar",
      "local:///app/jars/jar3.jar",
      "http://app/jars/jar4.jar")
    assert(resolvedJars === expectedResolvedJars)
  }

  test("Submitted Spark files should resolve to the download path.") {
    val resolvedFiles = localizedFilesResolver.resolveSubmittedSparkFiles()
    val expectedResolvedFiles = Seq(
      "hdfs://localhost:9000/app/files/file1.txt",
      s"$FILES_DOWNLOAD_PATH/file2.txt",
      "local:///app/files/file3.txt",
      "http://app/files/file4.txt")
    assert(resolvedFiles === expectedResolvedFiles)
  }
}
