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

package org.apache.spark.util.logging

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path

import org.apache.spark._
import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.internal.config._
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.util.Utils

class DriverLoggerSuite extends SparkFunSuite with LocalSparkContext {

  private var rootDfsDir : File = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    rootDfsDir = Utils.createTempDir(namePrefix = "dfs_logs")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    JavaUtils.deleteRecursively(rootDfsDir)
  }

  test("driver logs are persisted locally and synced to dfs") {
    val sc = getSparkContext()

    val app_id = sc.applicationId
    // Run a simple spark application
    sc.parallelize(1 to 1000).count()

    // Assert driver log file exists
    val rootDir = Utils.getLocalDir(sc.getConf)
    val driverLogsDir = FileUtils.getFile(rootDir, DriverLogger.DRIVER_LOG_DIR)
    assert(driverLogsDir.exists())
    val files = driverLogsDir.listFiles()
    assert(files.length === 1)
    assert(files(0).getName.equals(DriverLogger.DRIVER_LOG_FILE))

    sc.stop()
    assert(!driverLogsDir.exists())
    val dfsFile = FileUtils.getFile(sc.getConf.get(DRIVER_LOG_DFS_DIR).get,
      app_id + DriverLogger.DRIVER_LOG_FILE_SUFFIX)
    assert(dfsFile.exists())
    assert(dfsFile.length() > 0)
  }

  test("SPARK-40901: driver logs are persisted locally and synced to dfs when log " +
    "dir is absolute URI") {
    val sparkConf = new SparkConf()
    sparkConf.set(DRIVER_LOG_DFS_DIR, "file://" + rootDfsDir.getAbsolutePath())
    val sc = getSparkContext(sparkConf)
    val app_id = sc.applicationId
    // Run a simple spark application
    sc.parallelize(1 to 1000).count()

    // Assert driver log file exists
    val rootDir = Utils.getLocalDir(sc.getConf)
    val driverLogsDir = FileUtils.getFile(rootDir, DriverLogger.DRIVER_LOG_DIR)
    assert(driverLogsDir.exists())
    val files = driverLogsDir.listFiles()
    assert(files.length === 1)
    assert(files(0).getName.equals(DriverLogger.DRIVER_LOG_FILE))

    sc.stop()
    assert(!driverLogsDir.exists())
    assert(sc.getConf.get(DRIVER_LOG_DFS_DIR).get.startsWith("file:///"))
    val dfsFile = new Path(sc.getConf.get(DRIVER_LOG_DFS_DIR).get +
      "/" + app_id + DriverLogger.DRIVER_LOG_FILE_SUFFIX)
    val dfsFileStatus = dfsFile.getFileSystem(sc.hadoopConfiguration).getFileStatus(dfsFile)

    assert(dfsFileStatus.isFile)
    assert(dfsFileStatus.getLen > 0)
  }

  test("SPARK-44214: DriverLogger.apply returns None when only spark.driver.log.localDir exists") {
    val sparkConf = new SparkConf()
    assert(DriverLogger(sparkConf).isEmpty)
    withTempDir { dir =>
      assert(DriverLogger(sparkConf.set(DRIVER_LOG_LOCAL_DIR, dir.getCanonicalPath)).isEmpty)
    }
  }

  private def getSparkContext(): SparkContext = {
    getSparkContext(new SparkConf())
  }

  private def getSparkContext(conf: SparkConf): SparkContext = {
    conf.setIfMissing(DRIVER_LOG_DFS_DIR, rootDfsDir.getAbsolutePath())
    conf.setIfMissing(DRIVER_LOG_PERSISTTODFS, true)
    conf.setIfMissing(SparkLauncher.SPARK_MASTER, "local")
    conf.setIfMissing(SparkLauncher.DEPLOY_MODE, "client")
    sc = new SparkContext("local", "DriverLogTest", conf)
    sc
  }

}

