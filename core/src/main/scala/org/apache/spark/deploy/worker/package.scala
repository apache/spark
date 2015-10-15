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

import java.io.File

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.executor.ExecutorExitCode

package object worker {

  type StateChangeListener[D <: Description, T <: ChildRunnerInfo[D]] =
    (ChildProcessRunner[D, T], Option[String], Option[Exception]) => Unit

  type ExecutorRunner = ChildProcessRunner[ApplicationDescription, ExecutorRunnerInfo]
  type DriverRunner = ChildProcessRunner[DriverDescription, DriverRunnerInfo]

  private[deploy] trait ChildRunnerInfo[+T <: Description] {
    def setup: ChildProcessCommonSetup[T]
    def state: Enumeration#Value
    def exception: Option[Exception]
  }

  private[deploy] trait ChildProcessRunner[D <: Description, T <: ChildRunnerInfo[D]] {
    def start(): Unit

    def kill(): Unit

    def info: T
  }

  private[spark] class NonZeroExitCodeException(val exitCode: Int)
    extends Exception(ExecutorExitCode.explainExitCode(exitCode))

  case class ChildProcessCommonSetup[+T <: Description](
    id: String,
    cores: Int,
    memory: Int,
    host: String,
    publicAddress: String,
    webUIPort: Int,
    workDir: File,
    description: T
  )

  case class WorkerSetup(
    workerUri: String,
    sparkHome: File,
    conf: SparkConf,
    securityManager: SecurityManager
  )


}
