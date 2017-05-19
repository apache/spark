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

import java.io.File
import java.nio.file.Path
import java.util.{Map => JMap}

import scala.collection.mutable

import org.apache.spark.internal.Logging

/**
 * A stateful class that describes a Conda environment and also keeps track of packages that have
 * been added, as well as additional channels.
 *
 * @param rootPath  The root path under which envs/ and pkgs/ are located.
 * @param envName   The name of the environment.
 */
final class CondaEnvironment(val manager: CondaEnvironmentManager,
                             val rootPath: Path,
                             val envName: String,
                             bootstrapPackages: Seq[String],
                             bootstrapChannels: Seq[String]) extends Logging {

  import CondaEnvironment._

  private[this] val packages = mutable.Buffer(bootstrapPackages: _*)
  private[this] val channels = bootstrapChannels.toBuffer

  val condaEnvDir: Path = rootPath.resolve("envs").resolve(envName)

  def activatedEnvironment(startEnv: Map[String, String] = Map.empty): Map[String, String] = {
    require(!startEnv.contains("PATH"), "Defining PATH in a CondaEnvironment's startEnv is " +
      s"prohibited; found PATH=${startEnv("PATH")}")
    import collection.JavaConverters._
    val newVars = System.getenv().asScala.toIterator ++ startEnv ++ List(
      "CONDA_PREFIX" -> condaEnvDir.toString,
      "CONDA_DEFAULT_ENV" -> condaEnvDir.toString,
      "PATH" -> (condaEnvDir.resolve("bin").toString +
        sys.env.get("PATH").map(File.pathSeparator + _).getOrElse(""))
    )
    newVars.toMap
  }

  def addChannel(url: String): Unit = {
    channels += url
  }

  def installPackages(packages: Seq[String]): Unit = {
    manager.runCondaProcess(rootPath,
      List("install", "-n", envName, "-y")
        ::: "--" :: packages.toList,
      description = s"install dependencies in conda env $condaEnvDir",
      channels = channels.toList
    )

    this.packages ++= packages
  }

  /**
   * Clears the given java environment and replaces all variables with the environment
   * produced after calling `activate` inside this conda environment.
   */
  def initializeJavaEnvironment(env: JMap[String, String]): Unit = {
    env.clear()
    val activatedEnv = activatedEnvironment()
    activatedEnv.foreach { case (k, v) => env.put(k, v) }
    logDebug(s"Initialised environment from conda: $activatedEnv")
  }

  /**
   * This is for sending the instructions to the executors so they can replicate the same steps.
   */
  def buildSetupInstructions: CondaSetupInstructions = {
    CondaSetupInstructions(packages.toList, channels.toList)
  }
}

object CondaEnvironment {
  case class CondaSetupInstructions(packages: Seq[String], channels: Seq[String]) {
    require(channels.nonEmpty)
    require(packages.nonEmpty)
  }
}
