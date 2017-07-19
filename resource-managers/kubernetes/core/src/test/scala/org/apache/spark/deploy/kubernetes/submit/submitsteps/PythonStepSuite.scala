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
package org.apache.spark.deploy.kubernetes.submit.submitsteps

import io.fabric8.kubernetes.api.model._
import org.scalatest.BeforeAndAfter
import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, SparkFunSuite}

class PythonStepSuite extends SparkFunSuite with BeforeAndAfter {
  private val FILE_DOWNLOAD_PATH = "/var/data/spark-files"
  private val PYSPARK_FILES = Seq(
    "hdfs://localhost:9000/app/files/file1.py",
    "file:///app/files/file2.py",
    "local:///app/files/file3.py",
    "http://app/files/file4.py")
  private val RESOLVED_PYSPARK_FILES = Seq(
    FILE_DOWNLOAD_PATH + "/file1.py",
    FILE_DOWNLOAD_PATH + "/file2.py",
    "/app/files/file3.py",
    FILE_DOWNLOAD_PATH + "/file4.py").mkString(",")
  private val PYSPARK_PRIMARY_FILE = "file:///app/files/file5.py"
  private val RESOLVED_PYSPARK_PRIMARY_FILE = FILE_DOWNLOAD_PATH + "/file5.py"

  test("testing PySpark with --py-files both local and remote files") {
    val pyStep = new PythonStep(
      PYSPARK_PRIMARY_FILE,
      PYSPARK_FILES,
      FILE_DOWNLOAD_PATH)
    val returnedDriverContainer = pyStep.configureDriver(
      KubernetesDriverSpec(
        new Pod(),
        new Container(),
        Seq.empty[HasMetadata],
        new SparkConf))
    assert(returnedDriverContainer.driverContainer.getEnv
      .asScala.map(env => (env.getName, env.getValue)).toMap ===
      Map(
        "PYSPARK_PRIMARY" -> RESOLVED_PYSPARK_PRIMARY_FILE,
        "PYSPARK_FILES" -> RESOLVED_PYSPARK_FILES))
  }

  test("testing PySpark with empty --py-files ") {
    val pyStep = new PythonStep(
      PYSPARK_PRIMARY_FILE,
      Seq.empty[String],
      FILE_DOWNLOAD_PATH)
    val returnedDriverContainer = pyStep.configureDriver(
      KubernetesDriverSpec(
        new Pod(),
        new Container(),
        Seq.empty[HasMetadata],
        new SparkConf))
    assert(returnedDriverContainer.driverContainer.getEnv
      .asScala.map(env => (env.getName, env.getValue)).toMap ===
      Map(
        "PYSPARK_PRIMARY" -> RESOLVED_PYSPARK_PRIMARY_FILE,
        "PYSPARK_FILES" -> "null"))
  }

}
