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

package org.apache.spark.deploy.worker

import org.apache.spark.deploy.{DriverDescription, Description, ApplicationDescription, ExecutorState}

private[deploy] trait ChildRunnerFactory[D <: Description, T <: ChildRunnerInfo[D]] {
  def createRunner(
    appId: Option[String],
    processSetup: ChildProcessCommonSetup[D],
    workerSetup: WorkerSetup,
    stateChangeListener: StateChangeListener[D, T],
    localDirs: Seq[String]): ChildProcessRunner[D, T]
}

private[deploy] object DriverRunnerFactoryImpl
  extends ChildRunnerFactory[DriverDescription, DriverRunnerInfo] {

  override def createRunner(
    appId: Option[String],
    processSetup: ChildProcessCommonSetup[DriverDescription],
    workerSetup: WorkerSetup,
    stateChangeListener: StateChangeListener[DriverDescription, DriverRunnerInfo],
    localDirs: Seq[String]): DriverRunner = {

    val manager = new DriverRunnerImpl(
      processSetup,
      workerSetup,
      stateChangeListener)

    manager
  }
}

private[deploy] object ExecutorRunnerFactoryImpl
  extends ChildRunnerFactory[ApplicationDescription, ExecutorRunnerInfo] {

  override def createRunner(
    appId: Option[String],
    processSetup: ChildProcessCommonSetup[ApplicationDescription],
    workerSetup: WorkerSetup,
    stateChangeListener:
      StateChangeListener[ApplicationDescription, ExecutorRunnerInfo],
    localDirs: Seq[String]): ExecutorRunner = {

    val manager = new ExecutorRunnerImpl(
      processSetup,
      workerSetup,
      stateChangeListener,
      appId.get,
      localDirs,
      ExecutorState.LOADING)

    manager
  }

}
