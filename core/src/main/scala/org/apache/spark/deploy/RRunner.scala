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
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.SparkUserAppException
import org.apache.spark.api.conda.CondaEnvironment
import org.apache.spark.api.r.RBackend
import org.apache.spark.api.r.RUtils
import org.apache.spark.api.r.SparkRDefaults
import org.apache.spark.deploy.Common.Provenance
import org.apache.spark.internal.Logging
import org.apache.spark.util.RedirectThread

/**
 * Main class used to launch SparkR applications using spark-submit. It executes R as a
 * subprocess and then has it connect back to the JVM to access system properties etc.
 */
object RRunner extends CondaRunner with Logging {
  override def run(args: Array[String], maybeConda: Option[CondaEnvironment]): Unit = {
    val rFile = PythonRunner.formatPath(args(0))

    val otherArgs = args.slice(1, args.length)

    // Time to wait for SparkR backend to initialize in seconds
    val backendTimeout = sys.env.getOrElse("SPARKR_BACKEND_TIMEOUT", "120").toInt

    val presetRCommand = {
      val driverPreset = if (sys.props.getOrElse("spark.submit.deployMode", "client") == "client") {
        Provenance.fromConf("spark.r.driver.command")
      } else {
        None
      }
      // "spark.sparkr.r.command" is deprecated and replaced by "spark.r.command",
      // but kept here for backward compatibility.
      driverPreset
        .orElse(Provenance.fromConf("spark.r.command"))
        .orElse(Provenance.fromConf("spark.sparkr.r.command"))
    }

    val rCommand: String = maybeConda.map { conda =>
      presetRCommand.foreach { exec =>
        sys.error(s"It's forbidden to configure the r command when using conda, but found: $exec")
      }
      conda.condaEnvDir + "/bin/Rscript"
    }.orElse(presetRCommand.map(_.value))
     .getOrElse("Rscript")

    //  Connection timeout set by R process on its connection to RBackend in seconds.
    val backendConnectionTimeout = sys.props.getOrElse(
      "spark.r.backendConnectionTimeout", SparkRDefaults.DEFAULT_CONNECTION_TIMEOUT.toString)

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
        // If there is a CondaEnvironment set up, initialise our process' env from that
        maybeConda.foreach(_.initializeJavaEnvironment(env))
        env.put("EXISTING_SPARKR_BACKEND_PORT", sparkRBackendPort.toString)
        env.put("SPARKR_BACKEND_CONNECTION_TIMEOUT", backendConnectionTimeout)
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
