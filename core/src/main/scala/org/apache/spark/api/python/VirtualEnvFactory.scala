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

package org.apache.spark.api.python

import java.io.File
import java.util.{Map => JMap}
import java.util.Arrays
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import com.google.common.io.Files

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging


class VirtualEnvFactory(pythonExec: String, conf: SparkConf, isDriver: Boolean)
  extends Logging {

  private val virtualEnvType = conf.get("spark.pyspark.virtualenv.type", "native")
  private val virtualEnvBinPath = conf.get("spark.pyspark.virtualenv.bin.path", "")
  private val initPythonPackages = conf.getOption("spark.pyspark.virtualenv.packages")
  private var virtualEnvName: String = _
  private var virtualPythonExec: String = _
  private val VIRTUALENV_ID = new AtomicInteger()
  private var isLauncher: Boolean = false

  // used by launcher when user want to use virtualenv in pyspark shell. Launcher need this class
  // to create virtualenv for driver.
  def this(pythonExec: String, properties: JMap[String, String], isDriver: java.lang.Boolean) {
    this(pythonExec, new SparkConf().setAll(properties.asScala), isDriver)
    this.isLauncher = true
  }

  /*
   * Create virtualenv using native virtualenv or conda
   *
   */
  def setupVirtualEnv(): String = {
    /*
     *
     * Native Virtualenv:
     *   -  Execute command: virtualenv -p <pythonExec> --no-site-packages <virtualenvName>
     *   -  Execute command: python -m pip --cache-dir <cache-dir> install -r <requirement_file>
     *
     * Conda
     *   -  Execute command: conda create --prefix <prefix> --file <requirement_file> -y
     *
     */
    logInfo("Start to setup virtualenv...")
    logDebug("user.dir=" + System.getProperty("user.dir"))
    logDebug("user.home=" + System.getProperty("user.home"))

    require(virtualEnvType == "native" || virtualEnvType == "conda",
      s"VirtualEnvType: $virtualEnvType is not supported." )
    require(new File(virtualEnvBinPath).exists(),
      s"VirtualEnvBinPath: $virtualEnvBinPath is not defined or doesn't exist.")
    // Two scenarios of creating virtualenv:
    // 1. created in yarn container. Yarn will clean it up after container is exited
    // 2. created outside yarn container. Spark need to create temp directory and clean it after app
    //    finish.
    //      - driver of PySpark shell
    //      - driver of yarn-client mode
    if (isLauncher ||
      (isDriver && conf.get("spark.submit.deployMode") == "client")) {
      val virtualenvBasedir = Files.createTempDir()
      virtualenvBasedir.deleteOnExit()
      virtualEnvName = virtualenvBasedir.getAbsolutePath
    } else if (isDriver && conf.get("spark.submit.deployMode") == "cluster") {
      virtualEnvName = "virtualenv_driver"
    } else {
      // use the working directory of Executor
      virtualEnvName = "virtualenv_" + conf.getAppId + "_" + VIRTUALENV_ID.getAndIncrement()
    }

    // Use the absolute path of requirement file in the following cases
    // 1. driver of pyspark shell
    // 2. driver of yarn-client mode
    // otherwise just use filename as it would be downloaded to the working directory of Executor
    val pysparkRequirements =
      if (isLauncher ||
        (isDriver && conf.get("spark.submit.deployMode") == "client")) {
        conf.getOption("spark.pyspark.virtualenv.requirements")
      } else {
        conf.getOption("spark.pyspark.virtualenv.requirements").map(_.split("/").last)
      }

    val createEnvCommand =
      if (virtualEnvType == "native") {
        List(virtualEnvBinPath,
          "-p", pythonExec,
          "--no-site-packages", virtualEnvName)
      } else {
        // Two cases of conda
        //    1. requirement file is specified. (Batch mode)
        //    2. requirement file is not specified. (Interactive mode).
        //       In this case `spark.pyspark.virtualenv.python_version` must be specified.

        if (pysparkRequirements.isDefined) {
          List(virtualEnvBinPath,
            "create", "--prefix", virtualEnvName,
            "--file", pysparkRequirements.get, "-y")
        } else {
          require(conf.contains("spark.pyspark.virtualenv.python_version"),
            "spark.pyspark.virtualenv.python_version is not set when using conda " +
              "in interactive mode")
          val pythonVersion = conf.get("spark.pyspark.virtualenv.python_version")
          List(virtualEnvBinPath,
            "create", "--prefix", virtualEnvName,
            "python=" + pythonVersion, "-y")
        }
      }
    execCommand(createEnvCommand)

    virtualPythonExec = virtualEnvName + "/bin/python"
    if (virtualEnvType == "native" && pysparkRequirements.isDefined) {
      // requirement file for native is not mandatory, run this only when requirement file
      // is specified.
      execCommand(List(virtualPythonExec, "-m", "pip",
        "--cache-dir", System.getProperty("user.home"),
        "install", "-r", pysparkRequirements.get))
    }
    // install additional packages
    if (initPythonPackages.isDefined) {
      execCommand(List(virtualPythonExec, "-m", "pip",
        "install") ::: initPythonPackages.get.split(":").toList);
    }
    logInfo(s"virtualenv is created at to $virtualPythonExec")
    virtualPythonExec
  }

  private def execCommand(commands: List[String]): Unit = {
    logInfo("Running command:" + commands.mkString(" "))
    val pb = new ProcessBuilder(commands.asJava)
    // don't inheritIO when it is used in launcher, because launcher would capture the standard
    // output to assemble the spark-submit command.
    if(!isLauncher) {
      pb.inheritIO();
    }
    // pip internally use environment variable `HOME`
    pb.environment().put("HOME", System.getProperty("user.home"))
    val proc = pb.start()
    val exitCode = proc.waitFor()
    if (exitCode != 0) {
      throw new RuntimeException("Fail to run command: " + commands.mkString(" "))
    }
  }
}
