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
import java.nio.file.attribute.PosixFilePermission
import java.util.regex.Pattern

import scala.collection.JavaConverters._
import scala.sys.process.BasicIO
import scala.sys.process.Process
import scala.sys.process.ProcessBuilder
import scala.sys.process.ProcessIO

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.google.common.collect.ImmutableSet
import org.json4s.JsonAST.JValue
import org.json4s.jackson.Json4sScalaModule
import org.json4s.jackson.JsonMethods

import org.apache.spark.SparkConf
import org.apache.spark.SparkEnv
import org.apache.spark.SparkException
import org.apache.spark.api.conda.CondaEnvironment.CondaSetupInstructions
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.CONDA_BINARY_PATH
import org.apache.spark.internal.config.CONDA_GLOBAL_PACKAGE_DIRS
import org.apache.spark.internal.config.CONDA_VERBOSITY
import org.apache.spark.util.Utils

final class CondaEnvironmentManager(condaBinaryPath: String,
                                    verbosity: Int = 0,
                                    packageDirs: Seq[String] = Nil) extends Logging {

  require(verbosity >= 0 && verbosity <= 3, "Verbosity must be between 0 and 3 inclusively")

  CondaEnvironmentManager.ensureExecutable(condaBinaryPath)

  lazy val defaultInfo: Map[String, JValue] = {
    logInfo("Retrieving the conda installation's info")
    val command = Process(List(condaBinaryPath, "info", "--json"), None)

    val out = runOrFail(command, "retrieving the conda installation's info")

    implicit val format = org.json4s.DefaultFormats
    JsonMethods.parse(out).extract[Map[String, JValue]]
  }

  lazy val defaultPkgsDirs: List[String] = {
    implicit val format = org.json4s.DefaultFormats
    defaultInfo("pkgs_dirs").extract[List[String]]
  }

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
      List("create", "-n", name, "-y", "--no-default-packages")
        ::: verbosityFlags
        ::: "--" :: condaPackages.toList,
      description = "create conda env",
      channels = condaChannelUrls.toList
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
  private[this] def generateCondarc(baseRoot: Path, channelUrls: Seq[String]): Path = {

    val condarc = baseRoot.resolve("condarc")

    import org.json4s.JsonAST._
    import org.json4s.JsonDSL._

    // building it in json4s AST since it gives us more control over how it will be mapped
    val condarcNode = JObject(
      "pkgs_dirs" -> (packageDirs ++: s"$baseRoot/pkgs" +: defaultPkgsDirs),
      "envs_dirs" -> List(s"$baseRoot/envs"),
      "show_channel_urls" -> false,
      "default_channels" -> JArray(Nil),
      "channels" -> channelUrls
    )
    val mapper = new ObjectMapper(new YAMLFactory()).registerModule(Json4sScalaModule)

    Files.write(condarc, mapper.writeValueAsBytes(condarcNode))

    val sanitizedCondarc = condarcNode removeField { case (name, _) => name == "channels" }
    logInfo(f"Using condarc at $condarc (channels have been edited out):%n"
        + mapper.writeValueAsString(sanitizedCondarc))

    condarc
  }

  private[conda] def runCondaProcess(baseRoot: Path,
                                     args: List[String],
                                     channels: List[String],
                                     description: String): Unit = {
    val condarc = generateCondarc(baseRoot, channels)
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

  /**
   * Attempts to run a command and return its stdout,
   * @return the stdout of the process
   */
  private[this] def runOrFail(command: ProcessBuilder, description: String): String = {
    import CondaEnvironmentManager.redactCredentials
    val out = new StringBuffer
    val err = new StringBuffer
    val collectErrOutToBuffer = new ProcessIO(
      BasicIO.input(false),
      BasicIO.processFully((redactCredentials _).andThen(line => {
        out.append(line)
        out.append(BasicIO.Newline)
        ()
      })),
      BasicIO.processFully((redactCredentials _).andThen(line => {
        err.append(line)
        err.append(BasicIO.Newline)
        log.info(s"<conda> $line")
      })))
    val exitCode = command.run(collectErrOutToBuffer).exitValue()
    if (exitCode != 0) {
      throw new SparkException(s"Attempt to $description exited with code: "
      + f"$exitCode%nCommand was: $command%nStdout was:%n$out%nStderr was:%n$err")
    }
    out.toString
  }
}

object CondaEnvironmentManager extends Logging {
  def ensureExecutable(filePath: String): Unit = {
    val path = Paths.get(filePath)
    if (!Files.isExecutable(path)) {
      logInfo(s"Attempting to make file '$filePath' executable")
      val currentPerms = Files.getPosixFilePermissions(path).asScala
      val newPerms = ImmutableSet.copyOf(
        (currentPerms.iterator ++ Iterator(PosixFilePermission.OWNER_EXECUTE)).asJava)
      Files.setPosixFilePermissions(path, newPerms)
      require(Files.isExecutable(path), s"File '$filePath' still not executable")
    }
  }

  def isConfigured(sparkConf: SparkConf): Boolean = {
    sparkConf.contains(CONDA_BINARY_PATH)
  }

  private[this] val httpUrlToken =
    Pattern.compile("(\\b\\w+://[^:/@]*:)([^/@]+)(?=@([\\w-.]+)(:\\d+)?\\b)")

  private[conda] def redactCredentials(line: String): String = {
    httpUrlToken.matcher(line).replaceAll("$1<password>")
  }

  def fromConf(sparkConf: SparkConf): CondaEnvironmentManager = {
    val condaBinaryPath = sparkConf.get(CONDA_BINARY_PATH).getOrElse(
      sys.error(s"Expected config ${CONDA_BINARY_PATH.key} to be set"))
    val verbosity = sparkConf.get(CONDA_VERBOSITY)
    val packageDirs = sparkConf.get(CONDA_GLOBAL_PACKAGE_DIRS)
    new CondaEnvironmentManager(condaBinaryPath, verbosity, packageDirs)
  }

  /**
   * Helper method to create a conda environment from [[CondaEnvironment.CondaSetupInstructions]].
   * This is intended to be called on the executor with serialized instructions.
   */
  def createCondaEnvironment(instructions: CondaSetupInstructions): CondaEnvironment = {
    val condaPackages = instructions.packages
    val env = SparkEnv.get
    val condaEnvManager = CondaEnvironmentManager.fromConf(env.conf)
    val envDir = {
      // Which local dir to create it in?
      val localDirs = env.blockManager.diskBlockManager.localDirs
      val hash = Utils.nonNegativeHash(condaPackages)
      val dirId = hash % localDirs.length
      Utils.createTempDir(localDirs(dirId).getAbsolutePath, "conda").getAbsolutePath
    }
    condaEnvManager.create(envDir, condaPackages, instructions.channels)
  }

}
