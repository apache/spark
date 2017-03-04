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
package org.apache.spark.api.conda

import scala.sys.process.Process

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.CONDA_BINARY_PATH
import org.apache.spark.internal.config.CONDA_CHANNEL_URLS
import org.apache.spark.SparkConf
import org.apache.spark.SparkException

final class CondaEnvironmentManager(condaBinaryPath: String, condaChannelUrls: Seq[String])
    extends Logging {
  require(condaChannelUrls.nonEmpty, "Can't have an empty list of conda channel URLs")

  def create(condaEnvDir: String, bootstrapDeps: Seq[String]): CondaEnvironment = {
    require(bootstrapDeps.nonEmpty, "Expected at least one bootstrap dependency.")
    // Attempt to create environment
    val command = Process(
      List(condaBinaryPath, "create", "-p", condaEnvDir, "-y", "--override-channels")
        ++: condaChannelUrls.flatMap(Iterator("--channel", _))
        ++: bootstrapDeps)
    logInfo(s"About to execute: $command")
    val exitCode = command.!
    if (exitCode != 0) {
      throw new SparkException("Attempt to create conda env exited with code: "
        + f"$exitCode%nCommand was: $command")
    }

    new CondaEnvironment(condaEnvDir)
  }

  def installPackages(condaEnv: CondaEnvironment, deps: String*): Unit = {
    val command = Process(
      List(condaBinaryPath, "install", "-p", condaEnv.condaEnvDir, "-y", "--override-channels")
        ++: condaChannelUrls.flatMap(Iterator("--channel", _))
        ++: deps)
    logInfo(s"About to execute: $command")
    val exitCode = command.!
    if (exitCode != 0) {
      throw new SparkException("Attempt to install dependencies in conda env exited with code: "
        + f"$exitCode%nCommand was: $command")
    }
  }

  /** For use from pyspark. */
  def installPackage(condaEnv: CondaEnvironment, dep: String): Unit = {
    installPackages(condaEnv, dep)
  }
}

object CondaEnvironmentManager {
  def isConfigured(sparkConf: SparkConf): Boolean = {
    sparkConf.contains(CONDA_BINARY_PATH)
  }

  def fromConf(sparkConf: SparkConf): CondaEnvironmentManager = {
    val condaBinaryPath = sparkConf.get(CONDA_BINARY_PATH).getOrElse(
      sys.error(s"Expected $CONDA_BINARY_PATH to be set"))
    val condaChannelUrls = sparkConf.get(CONDA_CHANNEL_URLS)
    require(condaChannelUrls.nonEmpty,
      s"Must define at least one conda channel in $CONDA_CHANNEL_URLS")
    new CondaEnvironmentManager(condaBinaryPath, condaChannelUrls)
  }
}
