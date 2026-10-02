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
package org.apache.spark.security.aws.integrationtest

import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths}
import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * Minimal spark-submit driver used by the OIDC E2E tests.
 *
 * These helpers are intentionally self-contained rather than depending on the
 * `spark-kubernetes-integration-tests` test-jar. Depending on it would force every
 * build of this module to also activate `-Pkubernetes-integration-tests`, so the small
 * set of helpers is duplicated here instead. They mirror the equivalents of the same
 * name in `resource-managers/kubernetes/integration-tests`.
 */
private[integrationtest] class SparkAppConf {

  private val map = mutable.Map[String, String]()

  def set(key: String, value: String): SparkAppConf = {
    map.put(key, value)
    this
  }

  def get(key: String): String = map.getOrElse(key, "")

  override def toString: String = map.toString

  def toStringArray: Iterable[String] =
    map.toList.flatMap(t => List("--conf", s"${t._1}=${t._2}"))
}

private[integrationtest] case class SparkAppArguments(
    mainAppResource: String,
    mainClass: String,
    appArgs: Array[String])

private[integrationtest] object SparkAppLauncher extends Logging {

  def launch(
      appArguments: SparkAppArguments,
      appConf: SparkAppConf,
      timeoutSecs: Int,
      sparkHomeDir: Path): Unit = {
    val sparkSubmitExecutable = sparkHomeDir.resolve(Paths.get("bin", "spark-submit"))
    logInfo(s"Launching a spark app with arguments $appArguments and conf $appConf")
    val commandLine = mutable.ArrayBuffer(
      sparkSubmitExecutable.toFile.getAbsolutePath,
      "--deploy-mode", "cluster",
      "--class", appArguments.mainClass,
      "--master", appConf.get("spark.master")) ++
      appConf.toStringArray :+ appArguments.mainAppResource

    if (appArguments.appArgs.nonEmpty) {
      commandLine ++= appArguments.appArgs
    }
    logInfo(s"Launching a spark app with command line: ${commandLine.mkString(" ")}")
    ProcessUtils.executeProcess(commandLine.toArray, timeoutSecs)
  }
}

private[integrationtest] object ProcessUtils extends Logging {
  /**
   * executeProcess is used to run a command and return the output if it
   * completes within timeout seconds.
   */
  def executeProcess(
      fullCommand: Array[String],
      timeout: Long,
      dumpOutput: Boolean = true,
      dumpErrors: Boolean = true): Seq[String] = {
    val pb = new ProcessBuilder().command(fullCommand: _*)
    pb.redirectErrorStream(true)
    val proc = pb.start()
    val outputLines = new ArrayBuffer[String]
    Utils.tryWithResource(proc.getInputStream)(
      Source.fromInputStream(_, StandardCharsets.UTF_8.name()).getLines().foreach { line =>
        if (dumpOutput) {
          logInfo(line)
        }
        outputLines += line
      })
    assert(proc.waitFor(timeout, TimeUnit.SECONDS),
      s"Timed out while executing ${fullCommand.mkString(" ")}")
    assert(proc.exitValue == 0,
      s"Failed to execute -- ${fullCommand.mkString(" ")} --" +
        s"${if (dumpErrors) "\n" + outputLines.mkString("\n")}")
    outputLines.toSeq
  }
}
