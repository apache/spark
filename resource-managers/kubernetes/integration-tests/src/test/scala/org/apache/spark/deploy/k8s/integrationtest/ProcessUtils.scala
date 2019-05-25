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
package org.apache.spark.deploy.k8s.integrationtest

import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

import org.apache.spark.internal.Logging

object ProcessUtils extends Logging {
  /**
   * executeProcess is used to run a command and return the output if it
   * completes within timeout seconds.
   */
  def executeProcess(
      fullCommand: Array[String],
      timeout: Long,
      dumpErrors: Boolean = false): Seq[String] = {
    val pb = new ProcessBuilder().command(fullCommand: _*)
    pb.redirectErrorStream(true)
    val proc = pb.start()
    val outputLines = new ArrayBuffer[String]
    Utils.tryWithResource(proc.getInputStream)(
      Source.fromInputStream(_, "UTF-8").getLines().foreach { line =>
        logInfo(line)
        outputLines += line
      })
    assert(proc.waitFor(timeout, TimeUnit.SECONDS),
      s"Timed out while executing ${fullCommand.mkString(" ")}")
    assert(proc.exitValue == 0,
      s"Failed to execute ${fullCommand.mkString(" ")}" +
        s"${if (dumpErrors) "\n" + outputLines.mkString("\n")}")
    outputLines
  }
}
