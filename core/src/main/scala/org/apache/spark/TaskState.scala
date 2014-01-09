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

package org.apache.spark

import org.apache.mesos.Protos.{TaskState => MesosTaskState}

private[spark] object TaskState extends Enumeration {

  val LAUNCHING, RUNNING, FINISHED, FAILED, KILLED, LOST = Value

  val FINISHED_STATES = Set(FINISHED, FAILED, KILLED, LOST)

  type TaskState = Value

  def isFinished(state: TaskState) = FINISHED_STATES.contains(state)

  def toMesos(state: TaskState): MesosTaskState = state match {
    case LAUNCHING => MesosTaskState.TASK_STARTING
    case RUNNING => MesosTaskState.TASK_RUNNING
    case FINISHED => MesosTaskState.TASK_FINISHED
    case FAILED => MesosTaskState.TASK_FAILED
    case KILLED => MesosTaskState.TASK_KILLED
    case LOST => MesosTaskState.TASK_LOST
  }

  def fromMesos(mesosState: MesosTaskState): TaskState = mesosState match {
    case MesosTaskState.TASK_STAGING => LAUNCHING
    case MesosTaskState.TASK_STARTING => LAUNCHING
    case MesosTaskState.TASK_RUNNING => RUNNING
    case MesosTaskState.TASK_FINISHED => FINISHED
    case MesosTaskState.TASK_FAILED => FAILED
    case MesosTaskState.TASK_KILLED => KILLED
    case MesosTaskState.TASK_LOST => LOST
  }
}
