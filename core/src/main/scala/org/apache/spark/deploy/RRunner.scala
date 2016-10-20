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

import java.io._
import java.util.concurrent.{Semaphore, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkException, SparkUserAppException}
import org.apache.spark.api.r.{RBackend, RUtils}
import org.apache.spark.{SparkException, SparkUserAppException}
import org.apache.spark.util.RedirectThread

/**
 * Main class used to launch SparkR applications using spark-submit. It executes R as a
 * subprocess and then has it connect back to the JVM to access system properties etc.
 */
object RRunner {
  def main(args: Array[String]): Unit = {
    val rFile = PythonRunner.formatPath(args(0))

    val otherArgs = args.slice(1, args.length)

    // Time to wait for SparkR backend to initialize in seconds
    val backendTimeout = sys.env.getOrElse("SPARKR_BACKEND_TIMEOUT", "120").toInt
    val rCommand = {
      // "spark.sparkr.r.command" is deprecated and replaced by "spark.r.command",
      // but kept here for backward compatibility.
      var cmd = sys.props.getOrElse("spark.sparkr.r.command", "Rscript")
      cmd = sys.props.getOrElse("spark.r.command", cmd)
      if (sys.props.getOrElse("spark.submit.deployMode", "client") == "client") {
        cmd = sys.props.getOrElse("spark.r.driver.command", cmd)
      }
      cmd
    }

    // Check if the file path exists.
    // If not, change directory to current working directory for YARN cluster mode
    val rF = new File(rFile)
    val rFileNormalized = if (!rF.exists()) {
      new Path(rFile).getName
    } else {
      rFile
    }

    // Launch a SparkR backend server for the R process to connect to; this will let it see our
    // Java system properties etc.
    val sparkRBackend = new RBackend()
    @volatile var sparkRBackendPort = 0
    val initialized = new Semaphore(0)
    val sparkRBackendThread = new Thread("SparkR backend") {
      override def run() {
        sparkRBackendPort = sparkRBackend.init()
        initialized.release()
        sparkRBackend.run()
      }
    }

    sparkRBackendThread.start()
    // Wait for RBackend initialization to finish
    if (initialized.tryAcquire(backendTimeout, TimeUnit.SECONDS)) {
      // Launch R
      val returnCode = try {
        val builder = new ProcessBuilder((Seq(rCommand, rFileNormalized) ++ otherArgs).asJava)
        val env = builder.environment()
        env.put("EXISTING_SPARKR_BACKEND_PORT", sparkRBackendPort.toString)
        val rPackageDir = RUtils.sparkRPackagePath(isDriver = true)
        // Put the R package directories into an env variable of comma-separated paths
        env.put("SPARKR_PACKAGE_DIR", rPackageDir.mkString(","))
        env.put("R_PROFILE_USER",
          Seq(rPackageDir(0), "SparkR", "profile", "general.R").mkString(File.separator))
        builder.redirectErrorStream(true) // Ugly but needed for stdout and stderr to synchronize
        val process = builder.start()

        new RedirectThread(process.getInputStream, System.out, "redirect R output").start()

        process.waitFor()
      } finally {
        sparkRBackend.close()
      }
      if (returnCode != 0) {
        throw new SparkUserAppException(returnCode)
      }
    } else {
      val errorMessage = s"SparkR backend did not initialize in $backendTimeout seconds"
      // scalastyle:off println
      System.err.println(errorMessage)
      // scalastyle:on println
      throw new SparkException(errorMessage)
    }
  }
}
