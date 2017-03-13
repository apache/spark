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
import org.apache.spark.util.Utils

final class CondaEnvironmentManager(condaBinaryPath: String, condaChannelUrls: Seq[String])
    extends Logging {
  require(condaChannelUrls.nonEmpty, "Can't have an empty list of conda channel URLs")

  def create(baseDir: String, bootstrapPackages: Seq[String]): CondaEnvironment = {
    require(bootstrapPackages.nonEmpty, "Expected at least one bootstrap package.")
    val name = "conda-env"

    // must link in /tmp to reduce path length in case baseDir is very long...
    // If baseDir path is too long, this breaks conda's 220 character limit for binary replacement.
    // Don't even try to use java.io.tmpdir - yarn sets this to a very long path
    val linkedBaseDir = Utils.createTempDir("/tmp", "conda").toPath resolve "real"
    logInfo(s"Creating symlink $linkedBaseDir -> $baseDir")
    Files.createSymbolicLink(linkedBaseDir, Paths get baseDir)

    // Expose the symlinked path, through /tmp
    val condaEnvDir = (linkedBaseDir resolve "envs" resolve name).toString

    val condarc = generateCondarc(linkedBaseDir)

    // Attempt to create environment
    val command = makeCondaProcess(
      linkedBaseDir,
      List("create", "-n", name, "-y", "--override-channels", "-vv",
        "--no-default-packages")
        ::: condaChannelUrls.flatMap(Iterator("--channel", _)).toList
        ::: "--" :: bootstrapPackages.toList
    )
    runOrFail(command, "create conda env")

    new CondaEnvironment(condaEnvDir)
  }

  def installPackages(condaEnv: CondaEnvironment,
                      packages: Seq[String],
                      noDeps: Boolean = true): Unit = {
    val command = makeCondaProcess(condaEnv.envRoot.getParent,
      List("install", "-n", condaEnv.envName, "-y", "--override-channels",
        "--no-update-deps")
        ::: (if (noDeps) List("--no-deps") else Nil)
        ::: condaChannelUrls.flatMap(Iterator("--channel", _)).toList
        ::: "--" :: packages.toList
    )
    runOrFail(command, s"install dependencies in conda env ${condaEnv.condaEnvDir}")
  }

  /** For use from pyspark. */
  def installPackage(condaEnv: CondaEnvironment, dep: String, noDeps: Boolean): Unit = {
    installPackages(condaEnv, List(dep), noDeps)
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
    val condaPkgsPath = Paths.get(condaBinaryPath).getParent.getParent resolve "pkgs"
    val condarc = baseRoot resolve "condarc"
    val condarcContents =
      s"""pkgs_dirs:
         | - $baseRoot/pkgs
         | - $condaPkgsPath
         |envs_dirs:
         | - $baseRoot/envs
      """.stripMargin
    Files.write(condarc, List(condarcContents).asJava)
    logInfo(f"Using condarc at $condarc:%n$condarcContents")
    condarc
  }

  private[this] def makeCondaProcess(baseRoot: Path, args: List[String]): ProcessBuilder = {
    val condarc = generateCondarc(baseRoot)
    val fakeHomeDir = baseRoot resolve "home"
    // Attempt to create fake home dir
    Files.createDirectories(fakeHomeDir)

    Process(
      condaBinaryPath :: args,
      None,
      "CONDARC" -> condarc.toString,
      "HOME" -> fakeHomeDir.toString
    )
  }

  private[this] def runOrFail(command: ProcessBuilder, description: String): Unit = {
    logInfo(s"About to execute: $command")
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
      sys.error(s"Expected $CONDA_BINARY_PATH to be set"))
    val condaChannelUrls = sparkConf.get(CONDA_CHANNEL_URLS)
    require(condaChannelUrls.nonEmpty,
      s"Must define at least one conda channel in $CONDA_CHANNEL_URLS")
    new CondaEnvironmentManager(condaBinaryPath, condaChannelUrls)
  }
}
