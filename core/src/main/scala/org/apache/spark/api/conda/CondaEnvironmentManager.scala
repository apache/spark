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

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

import scala.collection.JavaConverters._
import scala.sys.process.BasicIO
import scala.sys.process.Process
import scala.sys.process.ProcessBuilder
import scala.sys.process.ProcessIO

import org.apache.spark.SparkConf
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.CONDA_BINARY_PATH
import org.apache.spark.internal.config.CONDA_CHANNEL_URLS
import org.apache.spark.internal.config.CONDA_VERBOSITY
import org.apache.spark.util.Utils

final class CondaEnvironmentManager(condaBinaryPath: String, verbosity: Int = 0) extends Logging {

  require(verbosity >= 0 && verbosity <= 3, "Verbosity must be between 0 and 3 inclusively")

  def create(
              baseDir: String,
              condaPackages: Seq[String],
              condaChannelUrls: Seq[String]): CondaEnvironment = {
    require(condaPackages.nonEmpty, "Expected at least one conda package.")
    require(condaChannelUrls.nonEmpty, "Can't have an empty list of conda channel URLs")
    val name = "conda-env"

    // must link in /tmp to reduce path length in case baseDir is very long...
    // If baseDir path is too long, this breaks conda's 220 character limit for binary replacement.
    // Don't even try to use java.io.tmpdir - yarn sets this to a very long path
    val linkedBaseDir = Utils.createTempDir("/tmp", "conda").toPath.resolve("real")
    logInfo(s"Creating symlink $linkedBaseDir -> $baseDir")
    Files.createSymbolicLink(linkedBaseDir, Paths.get(baseDir))

    val verbosityFlags = 0.until(verbosity).map(_ => "-v").toList

    // Attempt to create environment
    runCondaProcess(
      linkedBaseDir,
      List("create", "-n", name, "-y", "--override-channels", "--no-default-packages")
        ::: verbosityFlags
        ::: condaChannelUrls.flatMap(Iterator("--channel", _)).toList
        ::: "--" :: condaPackages.toList,
      description = "create conda env"
    )

    new CondaEnvironment(this, linkedBaseDir, name, condaPackages, condaChannelUrls)
  }

  /**
   * Create a condarc that only exposes package and env directories under the given baseRoot,
   * on top of the from the default pkgs directory inferred from condaBinaryPath.
   *
   * The file will be placed directly inside the given `baseRoot` dir, and link to `baseRoot/pkgs`
   * as the first package cache.
   *
   * This hack is necessary otherwise conda tries to use the homedir for pkgs cache.
   */
  private[this] def generateCondarc(baseRoot: Path): Path = {
    val condaPkgsPath = Paths.get(condaBinaryPath).getParent.getParent.resolve("pkgs")
    val condarc = baseRoot.resolve("condarc")
    val condarcContents =
      s"""pkgs_dirs:
         | - $baseRoot/pkgs
         | - $condaPkgsPath
         |envs_dirs:
         | - $baseRoot/envs
         |show_channel_urls: false
         |channels: []
         |default_channels: []
      """.stripMargin
    Files.write(condarc, List(condarcContents).asJava)
    logInfo(f"Using condarc at $condarc:%n$condarcContents")
    condarc
  }

  private[conda] def runCondaProcess(baseRoot: Path,
                                     args: List[String],
                                     description: String): Unit = {
    val condarc = generateCondarc(baseRoot)
    val fakeHomeDir = baseRoot.resolve("home")
    // Attempt to create fake home dir
    Files.createDirectories(fakeHomeDir)

    val extraEnv = List(
      "CONDARC" -> condarc.toString,
      "HOME" -> fakeHomeDir.toString
    )

    val command = Process(
      condaBinaryPath :: args,
      None,
      extraEnv: _*
    )

    logInfo(s"About to execute $command with environment $extraEnv")
    runOrFail(command, description)
    logInfo(s"Successfully executed $command with environment $extraEnv")
  }

  private[this] def runOrFail(command: ProcessBuilder, description: String): Unit = {
    val buffer = new StringBuffer
    val collectErrOutToBuffer = new ProcessIO(
    BasicIO.input(false),
    BasicIO.processFully(buffer),
    BasicIO.processFully(buffer))
    val exitCode = command.run(collectErrOutToBuffer).exitValue()
    if (exitCode != 0) {
      throw new SparkException(s"Attempt to $description exited with code: "
      + f"$exitCode%nCommand was: $command%nOutput was:%n${buffer.toString}")
    }
  }
}

object CondaEnvironmentManager {
  def isConfigured(sparkConf: SparkConf): Boolean = {
    sparkConf.contains(CONDA_BINARY_PATH)
  }

  def fromConf(sparkConf: SparkConf): CondaEnvironmentManager = {
    val condaBinaryPath = sparkConf.get(CONDA_BINARY_PATH).getOrElse(
      sys.error(s"Expected config ${CONDA_BINARY_PATH.key} to be set"))
    val verbosity = sparkConf.get(CONDA_VERBOSITY)
    new CondaEnvironmentManager(condaBinaryPath, verbosity)
  }
}
