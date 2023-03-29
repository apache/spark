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
package org.apache.spark.deploy.k8s.submit

import io.fabric8.kubernetes.api.model.{PodBuilder, PodStatusBuilder}
import io.fabric8.kubernetes.client.Watcher.Action

import org.apache.spark.SparkConf
import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.k8s.Constants.DEFAULT_DRIVER_CONTAINER_NAME
import org.apache.spark.deploy.k8s.KubernetesTestConf

class LoggingPodStatusWatcherSuite extends SparkFunSuite {

  private val pod = new PodBuilder()
    .withNewMetadata()
      .withName("test-pod")
      .withNamespace("spark")
      .withUid("test-spark-uid")
      .withCreationTimestamp("2020-01-01T00:00:00Z")
    .endMetadata()
    .withNewSpec()
      .withServiceAccount("spark")
      .withNodeName("test-node")
    .endSpec()
    .build()


  test("[SPARK-26365] Get watched driver container exit code") {
    import org.apache.spark.SparkException
    val sparkConf = new SparkConf()
    val kubernetesConf = KubernetesTestConf.createDriverConf(sparkConf)
    val watcher = new LoggingPodStatusWatcherImpl(kubernetesConf)

    val errorCode = 1234
    val completedCode = 0

    val errorState = new PodStatusBuilder()
      .withPhase("Failed")
      .addNewContainerStatus()
        .withName(DEFAULT_DRIVER_CONTAINER_NAME)
        .withNewState()
          .withNewTerminated()
            .withExitCode(errorCode)
          .endTerminated()
        .endState()
      .endContainerStatus()
      .build()

    val completedState = new PodStatusBuilder()
      .withPhase("Succeeded")
      .addNewContainerStatus()
        .withName(DEFAULT_DRIVER_CONTAINER_NAME)
          .withNewState()
            .withNewTerminated()
              .withExitCode(completedCode)
            .endTerminated()
          .endState()
        .endContainerStatus()
      .build()

    val runningState = new PodStatusBuilder()
      .withPhase("Running")
      .build()

    val errorPod = new PodBuilder(pod).withStatus(errorState).build()
    watcher.eventReceived(Action.MODIFIED, errorPod)

    assert(watcher.getDriverExitCode().nonEmpty)
    assert(watcher.getDriverExitCode().get == errorCode)

    val completedPod = new PodBuilder(pod).withStatus(completedState).build()
    watcher.eventReceived(Action.MODIFIED, completedPod)

    assert(watcher.getDriverExitCode().nonEmpty)
    assert(watcher.getDriverExitCode().get == completedCode)

    val runningPod = new PodBuilder(pod).withStatus(runningState).build()
    watcher.eventReceived(Action.MODIFIED, runningPod)

    // non-completed case
    assertThrows[SparkException] {
      watcher.getDriverExitCode()
    }
  }
}
