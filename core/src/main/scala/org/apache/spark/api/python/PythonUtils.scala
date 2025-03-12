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
import java.nio.file.Paths
import java.util.{List => JList}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.sys.process.Process

import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{PATH, PYTHON_PACKAGES, PYTHON_VERSION}
import org.apache.spark.util.ArrayImplicits.SparkArrayOps
import org.apache.spark.util.Utils

private[spark] object PythonUtils extends Logging {
  val PY4J_ZIP_NAME = "py4j-0.10.9.9-src.zip"

  /** Get the PYTHONPATH for PySpark, either from SPARK_HOME, if it is set, or from our JAR */
  def sparkPythonPath: String = {
    val pythonPath = new ArrayBuffer[String]
    for (sparkHome <- sys.env.get("SPARK_HOME")) {
      pythonPath += Seq(sparkHome, "python", "lib", "pyspark.zip").mkString(File.separator)
      pythonPath +=
        Seq(sparkHome, "python", "lib", PY4J_ZIP_NAME).mkString(File.separator)
    }
    pythonPath ++= SparkContext.jarOfObject(this)
    pythonPath.mkString(File.pathSeparator)
  }

  /** Merge PYTHONPATHS with the appropriate separator. Ignores blank strings. */
  def mergePythonPaths(paths: String*): String = {
    paths.filter(_ != "").mkString(File.pathSeparator)
  }

  def generateRDDWithNull(sc: JavaSparkContext): JavaRDD[String] = {
    sc.parallelize(List("a", null, "b"))
  }

  /**
   * Convert list of T into seq of T (for calling API with varargs)
   */
  def toSeq[T](vs: JList[T]): Seq[T] = {
    vs.asScala.toSeq
  }

  /**
   * Convert list of T into a (Scala) List of T
   */
  def toList[T](vs: JList[T]): List[T] = {
    vs.asScala.toList
  }

  /**
   * Convert list of T into array of T (for calling API with array)
   */
  def toArray[T](vs: JList[T]): Array[T] = {
    vs.toArray().asInstanceOf[Array[T]]
  }

  /**
   * Convert java map of K, V into Map of K, V (for calling API with varargs)
   */
  def toScalaMap[K, V](jm: java.util.Map[K, V]): Map[K, V] = {
    jm.asScala.toMap
  }

  def isEncryptionEnabled(sc: JavaSparkContext): Boolean = {
    sc.conf.get(org.apache.spark.internal.config.IO_ENCRYPTION_ENABLED)
  }

  def getBroadcastThreshold(sc: JavaSparkContext): Long = {
    sc.conf.get(org.apache.spark.internal.config.BROADCAST_FOR_UDF_COMPRESSION_THRESHOLD)
  }

  def getPythonAuthSocketTimeout(sc: JavaSparkContext): Long = {
    sc.conf.get(org.apache.spark.internal.config.Python.PYTHON_AUTH_SOCKET_TIMEOUT)
  }

  def getSparkBufferSize(sc: JavaSparkContext): Int = {
    sc.conf.get(org.apache.spark.internal.config.BUFFER_SIZE)
  }

  def logPythonInfo(pythonExec: String): Unit = {
    if (SparkEnv.get.conf.get(org.apache.spark.internal.config.Python.PYTHON_LOG_INFO)) {
      import scala.sys.process._
      def runCommand(process: ProcessBuilder): Option[String] = {
        try {
          val stdout = new StringBuilder
          val processLogger = ProcessLogger(line => stdout.append(line).append(" "), _ => ())
          if (process.run(processLogger).exitValue() == 0) {
            Some(stdout.toString.trim)
          } else {
            None
          }
        } catch {
          case _: Throwable => None
        }
      }

      val pythonVersionCMD = Seq(pythonExec, "-VV")
      val pythonPath = PythonUtils.mergePythonPaths(
        PythonUtils.sparkPythonPath,
        sys.env.getOrElse("PYTHONPATH", ""))
      val environment = Map("PYTHONPATH" -> pythonPath)
      logInfo(log"Python path ${MDC(PATH, pythonPath)}")

      val processPythonVer = Process(pythonVersionCMD, None, environment.toSeq: _*)
      val output = runCommand(processPythonVer)
      logInfo(log"Python version: ${MDC(PYTHON_VERSION, output.getOrElse("Unable to determine"))}")

      val pythonCode =
        """
          |import pkg_resources
          |
          |installed_packages = pkg_resources.working_set
          |installed_packages_list = sorted(["%s:%s" % (i.key, i.version)
          |                                 for i in installed_packages])
          |
          |for package in installed_packages_list:
          |    print(package)
          |""".stripMargin

      val listPackagesCMD = Process(Seq(pythonExec, "-c", pythonCode))
      val listOfPackages = runCommand(listPackagesCMD)

      def formatOutput(output: String): String = {
        output.replaceAll("\\s+", ", ")
      }
      listOfPackages.foreach(x => logInfo(log"List of Python packages :-" +
        log" ${MDC(PYTHON_PACKAGES, formatOutput(x))}"))
    }
  }

  // Only for testing.
  private[spark] var additionalTestingPath: Option[String] = None

  private[spark] val defaultPythonExec: String = sys.env.getOrElse(
    "PYSPARK_DRIVER_PYTHON", sys.env.getOrElse("PYSPARK_PYTHON", "python3"))

  private[spark] def createPythonFunction(command: Array[Byte]): SimplePythonFunction = {
    val sourcePython = if (Utils.isTesting) {
      // Put PySpark source code instead of the build zip archive so we don't need
      // to build PySpark every time during development.
      val sparkHome: String = {
        require(
          sys.props.contains("spark.test.home") || sys.env.contains("SPARK_HOME"),
          "spark.test.home or SPARK_HOME is not set.")
        sys.props.getOrElse("spark.test.home", sys.env("SPARK_HOME"))
      }
      val sourcePath = Paths.get(sparkHome, "python").toAbsolutePath
      val py4jPath = Paths.get(
        sparkHome, "python", "lib", PythonUtils.PY4J_ZIP_NAME).toAbsolutePath
      val merged = mergePythonPaths(sourcePath.toString, py4jPath.toString)
      // Adds a additional path to search Python packages for testing.
      additionalTestingPath.map(mergePythonPaths(_, merged)).getOrElse(merged)
    } else {
      PythonUtils.sparkPythonPath
    }
    val pythonPath = PythonUtils.mergePythonPaths(
      sourcePython, sys.env.getOrElse("PYTHONPATH", ""))

    val pythonVer: String =
      Process(
        Seq(defaultPythonExec, "-c", "import sys; print('%d.%d' % sys.version_info[:2])"),
        None,
        "PYTHONPATH" -> pythonPath).!!.trim()

    SimplePythonFunction(
      command = command.toImmutableArraySeq,
      envVars = mutable.Map("PYTHONPATH" -> pythonPath).asJava,
      pythonIncludes = List.empty.asJava,
      pythonExec = defaultPythonExec,
      pythonVer = pythonVer,
      broadcastVars = List.empty.asJava,
      accumulator = null)
  }
}
