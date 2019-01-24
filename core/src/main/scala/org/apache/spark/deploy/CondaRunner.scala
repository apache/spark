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

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.api.conda.CondaEnvironment
import org.apache.spark.api.conda.CondaEnvironmentManager
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.util.Utils

/**
 * A runner template used to launch applications using Conda. It bootstraps a Conda env,
 * then delegates to the [[CondaRunner.run]].
 */
abstract class CondaRunner extends Logging {
  final def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    run(args, CondaRunner.setupCondaEnvironmentAutomatically(sparkConf))
  }

  def run(args: Array[String], maybeConda: Option[CondaEnvironment]): Unit
}

object CondaRunner {
  private[spark] val condaEnvironment: AtomicReference[Option[CondaEnvironment]] =
      new AtomicReference(None)

  /**
   * Sets up a conda environment if [[CondaEnvironmentManager.isConfigured]] returns true.
   * Once an environment has been set up, calling this method again (or the [[main]] method)
   * will throw a [[RuntimeException]].
   */
  def setupCondaEnvironmentAutomatically(sparkConf: SparkConf): Option[CondaEnvironment] = {
    if (CondaEnvironmentManager.isConfigured(sparkConf)) {
      val condaBootstrapDeps = sparkConf.get(CONDA_BOOTSTRAP_PACKAGES)
      val condaChannelUrls = sparkConf.get(CONDA_CHANNEL_URLS)
      val condaExtraArgs = sparkConf.get(CONDA_EXTRA_ARGUMENTS)
      val condaEnvVariables = extractEnvVariables(sparkConf)
      val condaBaseDir = Utils.createTempDir(Utils.getLocalDir(sparkConf), "conda").getAbsolutePath
      val condaEnvironmentManager = CondaEnvironmentManager.fromConf(sparkConf)
      val environment =
        condaEnvironmentManager.create(
          condaBaseDir,
          condaBootstrapDeps,
          condaChannelUrls,
          condaExtraArgs,
          condaEnvVariables)
      setCondaEnvironment(environment)
      Some(environment)
    } else {
      None
    }
  }

  /**
   * Extracts environment variables specified in the form
   * "spark.conda.env.[EnvironmentVariableName]" from the sparkConf.
   */
  def extractEnvVariables(sparkConf: SparkConf): Map[String, String] = {
    val condaEnvPrefix = "spark.conda.env."
    sparkConf.getAll
      .filter { case (k, v) => k.startsWith(condaEnvPrefix) }
      .map { case (k, v) => (k.substring(condaEnvPrefix.length), v) }
      .toMap
  }

  /**
   * Sets the given environment as the global environment, which will be accessible by calling
   * [[SparkContext.condaEnvironment]]. This method can only be called once! If an environment
   * has already been set, calling this method again will throw a [[RuntimeException]].
   */
  def setCondaEnvironment(environment: CondaEnvironment): Unit = {
    // Save this as a global in order for SparkContext to be able to access it later, in case we
    // are shelling out, but providing a bridge back into this JVM.
    require(CondaRunner.condaEnvironment.compareAndSet(None, Some(environment)),
      "Couldn't set condaEnvironment to the newly created environment, it was already set to: "
        + CondaRunner.condaEnvironment.get())
  }
}
