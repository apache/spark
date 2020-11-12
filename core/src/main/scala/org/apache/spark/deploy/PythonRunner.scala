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

package org.apache.spark.deploy

import java.io.File
import java.net.URI
import java.nio.file.Files

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import org.apache.spark.{SparkConf, SparkUserAppException}
import org.apache.spark.api.python.{Py4JServer, PythonUtils}
import org.apache.spark.internal.config._
import org.apache.spark.util.{RedirectThread, Utils}

/**
 * A main class used to launch Python applications. It executes python as a
 * subprocess and then has it connect back to the JVM to access system properties, etc.
 */
object PythonRunner {
  def main(args: Array[String]): Unit = {
    val pythonFile = args(0)
    val pyFiles = args(1)
    val otherArgs = args.slice(2, args.length)
    val sparkConf = new SparkConf()
    val pythonExec = sparkConf.get(PYSPARK_DRIVER_PYTHON)
      .orElse(sparkConf.get(PYSPARK_PYTHON))
      .orElse(sys.env.get("PYSPARK_DRIVER_PYTHON"))
      .orElse(sys.env.get("PYSPARK_PYTHON"))
      .getOrElse("python")

    // Format python file paths before adding them to the PYTHONPATH
    val formattedPythonFile = formatPath(pythonFile)
    val formattedPyFiles = resolvePyFiles(formatPaths(pyFiles))

    val gatewayServer = new Py4JServer(sparkConf)

    val thread = new Thread(() => Utils.logUncaughtExceptions { gatewayServer.start() })
    thread.setName("py4j-gateway-init")
    thread.setDaemon(true)
    thread.start()

    // Wait until the gateway server has started, so that we know which port is it bound to.
    // `gatewayServer.start()` will start a new thread and run the server code there, after
    // initializing the socket, so the thread started above will end as soon as the server is
    // ready to serve connections.
    thread.join()

    // Build up a PYTHONPATH that includes the Spark assembly (where this class is), the
    // python directories in SPARK_HOME (if set), and any files in the pyFiles argument
    val pathElements = new ArrayBuffer[String]
    pathElements ++= formattedPyFiles
    pathElements += PythonUtils.sparkPythonPath
    pathElements += sys.env.getOrElse("PYTHONPATH", "")
    val pythonPath = PythonUtils.mergePythonPaths(pathElements.toSeq: _*)

    // Launch Python process
    val builder = new ProcessBuilder((Seq(pythonExec, formattedPythonFile) ++ otherArgs).asJava)
    val env = builder.environment()
    env.put("PYTHONPATH", pythonPath)
    // This is equivalent to setting the -u flag; we use it because ipython doesn't support -u:
    env.put("PYTHONUNBUFFERED", "YES") // value is needed to be set to a non-empty string
    env.put("PYSPARK_GATEWAY_PORT", "" + gatewayServer.getListeningPort)
    env.put("PYSPARK_GATEWAY_SECRET", gatewayServer.secret)
    // pass conf spark.pyspark.python to python process, the only way to pass info to
    // python process is through environment variable.
    sparkConf.get(PYSPARK_PYTHON).foreach(env.put("PYSPARK_PYTHON", _))
    sys.env.get("PYTHONHASHSEED").foreach(env.put("PYTHONHASHSEED", _))
    // if OMP_NUM_THREADS is not explicitly set, override it with the number of cores
    if (sparkConf.getOption("spark.yarn.appMasterEnv.OMP_NUM_THREADS").isEmpty &&
        sparkConf.getOption("spark.mesos.driverEnv.OMP_NUM_THREADS").isEmpty &&
        sparkConf.getOption("spark.kubernetes.driverEnv.OMP_NUM_THREADS").isEmpty) {
      // SPARK-28843: limit the OpenMP thread pool to the number of cores assigned to the driver
      // this avoids high memory consumption with pandas/numpy because of a large OpenMP thread pool
      // see https://github.com/numpy/numpy/issues/10455
      sparkConf.getOption("spark.driver.cores").foreach(env.put("OMP_NUM_THREADS", _))
    }
    builder.redirectErrorStream(true) // Ugly but needed for stdout and stderr to synchronize
    try {
      val process = builder.start()

      new RedirectThread(process.getInputStream, System.out, "redirect output").start()

      val exitCode = process.waitFor()
      if (exitCode != 0) {
        throw new SparkUserAppException(exitCode)
      }
    } finally {
      gatewayServer.shutdown()
    }
  }

  /**
   * Format the python file path so that it can be added to the PYTHONPATH correctly.
   *
   * Python does not understand URI schemes in paths. Before adding python files to the
   * PYTHONPATH, we need to extract the path from the URI. This is safe to do because we
   * currently only support local python files.
   */
  def formatPath(path: String, testWindows: Boolean = false): String = {
    if (Utils.nonLocalPaths(path, testWindows).nonEmpty) {
      throw new IllegalArgumentException("Launching Python applications through " +
        s"spark-submit is currently only supported for local files: $path")
    }
    // get path when scheme is file.
    val uri = Try(new URI(path)).getOrElse(new File(path).toURI)
    var formattedPath = uri.getScheme match {
      case null => path
      case "file" | "local" => uri.getPath
      case _ => null
    }

    // Guard against malformed paths potentially throwing NPE
    if (formattedPath == null) {
      throw new IllegalArgumentException(s"Python file path is malformed: $path")
    }

    // In Windows, the drive should not be prefixed with "/"
    // For instance, python does not understand "/C:/path/to/sheep.py"
    if (Utils.isWindows && formattedPath.matches("/[a-zA-Z]:/.*")) {
      formattedPath = formattedPath.stripPrefix("/")
    }
    formattedPath
  }

  /**
   * Format each python file path in the comma-delimited list of paths, so it can be
   * added to the PYTHONPATH correctly.
   */
  def formatPaths(paths: String, testWindows: Boolean = false): Array[String] = {
    Option(paths).getOrElse("")
      .split(",")
      .filter(_.nonEmpty)
      .map { p => formatPath(p, testWindows) }
  }

  /**
   * Resolves the ".py" files. ".py" file should not be added as is because PYTHONPATH does
   * not expect a file. This method creates a temporary directory and puts the ".py" files
   * if exist in the given paths.
   */
  private def resolvePyFiles(pyFiles: Array[String]): Array[String] = {
    lazy val dest = Utils.createTempDir(namePrefix = "localPyFiles")
    pyFiles.flatMap { pyFile =>
      // In case of client with submit, the python paths should be set before context
      // initialization because the context initialization can be done later.
      // We will copy the local ".py" files because ".py" file shouldn't be added
      // alone but its parent directory in PYTHONPATH. See SPARK-24384.
      if (pyFile.endsWith(".py")) {
        val source = new File(pyFile)
        if (source.exists() && source.isFile && source.canRead) {
          Files.copy(source.toPath, new File(dest, source.getName).toPath)
          Some(dest.getAbsolutePath)
        } else {
          // Don't have to add it if it doesn't exist or isn't readable.
          None
        }
      } else {
        Some(pyFile)
      }
    }.distinct
  }
}
