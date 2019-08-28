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
package org.apache.spark.scheduler.cluster.k8s

import io.fabric8.kubernetes.api.model.Pod

sealed trait ExecutorState {
  def pod: Pod
}

case class ExecutorRunning(pod: Pod) extends ExecutorState

case class ExecutorPending(pod: Pod) extends ExecutorState

sealed trait FinalExecutorState extends ExecutorState

case class ExecutorSucceeded(pod: Pod) extends FinalExecutorState

case class ExecutorFailed(pod: Pod) extends FinalExecutorState

case class ExecutorPodDeleted(pod: Pod) extends FinalExecutorState

case class ExecutorPodUnknown(pod: Pod) extends ExecutorState
