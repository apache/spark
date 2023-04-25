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

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._
import scala.util.Using

import org.apache.commons.codec.digest.DigestUtils

import org.apache.spark.SPARK_VERSION
import org.apache.spark.util.Utils


object PythonEnvManager {

  val PIP_CACHE_DIR = "pip_cache_pkgs"

  private def getVirtualenvCommandExtraEnv(envRootDir: String): Map[String, String] = {
    Map(
      // PIP_NO_INPUT=1 makes pip run in non-interactive mode,
      // otherwise pip might prompt "yes or no" and ask stdin input
      "PIP_NO_INPUT" -> "1",
      // Specify pip cache dir
      "PIP_CACHE_DIR" -> new File(envRootDir, PIP_CACHE_DIR).getAbsolutePath()
    )
  }

  private def getPythonEnvironmentKey(
                               pipDependencies: Seq[String],
                               pipConstraints: Seq[String]
                             ): String = {
    val data = pipDependencies.mkString(",") + "\n" + pipConstraints.mkString(",")
    "python-" + DigestUtils.sha1Hex(data)
  }

  // TODO: Support creating python env with specified python version.
  def getOrCreatePythonEnvironment(
                                    pythonExec: String,
                                    rootEnvDir: String,
                                    pipDependencies: Seq[String],
                                    pipConstraints: Seq[String]
                                  ): String = {
    // Adds a global lock when creating python environment,
    // to avoid race conditions.
    // race conditions includes:
    //  - python env under the same rootEnvDir shares the pip cache directory,
    //    but concurrent pip installation causes race condition
    //  - When creating environment failed, we need to clean the directory,
    //    concurrent creation/deletion causes race condition
    // TODO: use file lock instead to ensure safety, e.g. the case that multiple
    //  spark executors running on the same machine.
    synchronized {
      val key = getPythonEnvironmentKey(pipDependencies, pipConstraints)
      val envDir = Path.of(rootEnvDir, key).toString

      if (!new File(envDir).exists()) {
        try {
          createPythonEnvironment(pythonExec, rootEnvDir, envDir, pipDependencies, pipConstraints)
        } catch {
          case e: Exception =>
            // Clean environment directory that is in some undefined status
            Utils.deleteRecursively(new File(envDir))
            throw new RuntimeException(
              s"Create python environment failed. Root cause: ${e.toString}", e
            )
        }
      }
      Path.of(envDir, "bin", "python").toString
    }
  }

  private def createPythonEnvironment(
      pythonExec: String,
      rootEnvDir: String,
      envDir: String,
      pipDependencies: Seq[String],
      pipConstraints: Seq[String]
  ): Unit = {

    val pb = new ProcessBuilder(java.util.Arrays.asList(pythonExec, "-m", "virtualenv", envDir))
    val proc = pb.start()
    val retCode = proc.waitFor()

    if (retCode != 0) {
      throw new RuntimeException(
        s"Create python environment by virtualenv command failed (return code $retCode)."
      )
    }

    val pipTempDir = Files.createTempDirectory("pip-temp-").toString

    try {
      val pipReqFilePath = Path.of(pipTempDir, "requirements.txt").toString
      val pipConstraintsFilePath = Path.of(pipTempDir, "constraints.txt").toString

      Using(new PrintWriter(pipReqFilePath)) { writer =>
        for (req <- pipDependencies) {
          writer.print(req)
          writer.print(System.lineSeparator())
        }
        writer.print(s"pyspark==$SPARK_VERSION")
        writer.print(System.lineSeparator())
        writer.print(s"-c $pipConstraintsFilePath")
        writer.print(System.lineSeparator())
      }

      Using(new PrintWriter(pipConstraintsFilePath)) { writer =>
        for (constraint <- pipConstraints) {
          writer.print(constraint)
          writer.print(System.lineSeparator())
        }
      }

      val pipPb = new ProcessBuilder(
        java.util.Arrays.asList(pythonExec, "-m", "pip", "install", "--quiet", "-r", pipReqFilePath)
      )
      pipPb.environment().putAll(
        getVirtualenvCommandExtraEnv(rootEnvDir).asJava
      )
      val pipProc = pipPb.start()
      val pipRetCode = pipProc.waitFor()

      if (pipRetCode != 0) {
        throw new RuntimeException(
          s"Create python environment by virtualenv command failed (return code is $pipRetCode)."
        )
      }
    } finally {
      Files.deleteIfExists(Path.of(pipTempDir))
    }
  }
}
