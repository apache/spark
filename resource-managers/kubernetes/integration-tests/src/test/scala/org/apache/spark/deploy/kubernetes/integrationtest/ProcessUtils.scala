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
package org.apache.spark.deploy.kubernetes.integrationtest

import java.io.{BufferedReader, InputStreamReader}
import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

object ProcessUtils extends Logging {
  /**
    * executeProcess is used to run a command and return the output if it
    * completes within timeout seconds.
    */
  def executeProcess(fullCommand: Array[String], timeout: Long): Seq[String] = {
    val pb = new ProcessBuilder().command(fullCommand: _*)
    pb.redirectErrorStream(true)
    val proc = pb.start()
    val outputLines = new ArrayBuffer[String]

    Utils.tryWithResource(new InputStreamReader(proc.getInputStream)) { procOutput =>
      Utils.tryWithResource(new BufferedReader(procOutput)) { (bufferedOutput: BufferedReader) =>
        var line: String = null
        do {
          line = bufferedOutput.readLine()
          if (line != null) {
            logInfo(line)
            outputLines += line
          }
        } while (line != null)
      }
    }
    assert(proc.waitFor(timeout, TimeUnit.SECONDS),
      s"Timed out while executing ${fullCommand.mkString(" ")}")
    assert(proc.exitValue == 0, s"Failed to execute ${fullCommand.mkString(" ")}")
    outputLines.toSeq
  }
}
