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

import java.io.{BufferedInputStream, FileInputStream}

import org.apache.commons.io.FileUtils

import org.apache.spark._
import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.internal.config._
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.util.Utils

class DriverLoggerSuite extends SparkFunSuite with LocalSparkContext {

  private val DRIVER_LOG_DIR_DEFAULT = "/tmp/hdfs_logs"

  override def beforeAll(): Unit = {
    FileUtils.forceMkdir(FileUtils.getFile(DRIVER_LOG_DIR_DEFAULT))
  }

  override def afterAll(): Unit = {
    JavaUtils.deleteRecursively(FileUtils.getFile(DRIVER_LOG_DIR_DEFAULT))
  }

  test("driver logs are persisted") {
    val sc = getSparkContext()

    val app_id = sc.applicationId
    // Run a simple spark application
    sc.parallelize(1 to 1000).count()

    // Assert driver log file exists
    val rootDir = Utils.getLocalDir(sc.getConf)
    val driverLogsDir = FileUtils.getFile(rootDir, "driver_logs")
    assert(driverLogsDir.exists())
    val files = driverLogsDir.listFiles()
    assert(files.length === 1)
    assert(files(0).getName.equals("driver.log"))

    sc.stop()
    // On application end, file is moved to Hdfs (which is a local dir for this test)
    assert(!driverLogsDir.exists())
    val hdfsDir = FileUtils.getFile(sc.getConf.get(DRIVER_LOG_DFS_DIR).get, app_id)
    assert(hdfsDir.exists())
    val hdfsFiles = hdfsDir.listFiles()
    assert(hdfsFiles.length > 0)
    JavaUtils.deleteRecursively(hdfsDir)
    assert(!hdfsDir.exists())
  }

  test("driver logs are synced to hdfs continuously") {
    val sc = getSparkContext()

    val app_id = sc.applicationId
    // Run a simple spark application
    sc.parallelize(1 to 1000).count()

    // Assert driver log file exists
    val rootDir = Utils.getLocalDir(sc.getConf)
    val driverLogsDir = FileUtils.getFile(rootDir, "driver_logs")
    assert(driverLogsDir.exists())
    val files = driverLogsDir.listFiles()
    assert(files.length === 1)
    assert(files(0).getName.equals("driver.log"))
    for (i <- 1 to 1000) {
      logInfo("Log enough data to log file so that it can be flushed")
    }

    // After 5 secs, file contents are synced to Hdfs (which is a local dir for this test)
    Thread.sleep(6000)
    val hdfsDir = FileUtils.getFile(sc.getConf.get(DRIVER_LOG_DFS_DIR).get, app_id)
    assert(hdfsDir.exists())
    val hdfsFiles = hdfsDir.listFiles()
    assert(hdfsFiles.length > 0)
    val driverLogFile = hdfsFiles.filter(f => f.getName.equals("driver.log")).head
    val hdfsIS = new BufferedInputStream(new FileInputStream(driverLogFile))
    assert(hdfsIS.available() > 0)

    sc.stop()
    // Ensure that the local file is deleted on application end
    assert(!driverLogsDir.exists())
    JavaUtils.deleteRecursively(hdfsDir)
    assert(!hdfsDir.exists())
  }

  private def getSparkContext(): SparkContext = {
    val conf = new SparkConf()
    conf.set("spark.local.dir", "/tmp")
    conf.set(DRIVER_LOG_DFS_DIR, DRIVER_LOG_DIR_DEFAULT)
    conf.set(DRIVER_LOG_SYNCTODFS, true)
    conf.set(SparkLauncher.SPARK_MASTER, "local")
    conf.set(SparkLauncher.DEPLOY_MODE, "client")
    sc = new SparkContext("local", "DriverLogTest", conf)
    sc
  }

}

