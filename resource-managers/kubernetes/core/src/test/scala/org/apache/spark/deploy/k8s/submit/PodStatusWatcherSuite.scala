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

import java.util

import io.fabric8.kubernetes.api.model.{ObjectMeta, Pod, PodSpec, PodStatus}
import io.fabric8.kubernetes.client.Watcher.Action.{ADDED, MODIFIED}
import org.mockito.Mock
import org.mockito.Mockito.{times, verify}
import org.mockito.MockitoAnnotations
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.k8s.KubernetesTestConf
import org.apache.spark.launcher.SparkAppHandle

class PodStatusWatcherSuite extends SparkFunSuite with BeforeAndAfter {

  @Mock
  private var launcherBackend: KubernetesLauncherBackend = _

  private val podStatusWatcher = new PodStatusWatcherImpl(k8sDriverConf)

  before {
    MockitoAnnotations.openMocks(this).close()
    podStatusWatcher.registerLauncherBackend(launcherBackend)
  }

  test("The pod status watcher should notify launcher backend about status change.") {
    podStatusWatcher.eventReceived(ADDED, podWithPhase("Running"))
    verify(launcherBackend).setState(SparkAppHandle.State.RUNNING)
  }

  test("The pod status watcher should not notify launcher backend if status is not changed.") {
    podStatusWatcher.eventReceived(MODIFIED, podWithPhase("Succeeded"))
    podStatusWatcher.eventReceived(MODIFIED, podWithPhase("Succeeded"))
    verify(launcherBackend, times(1)).setState(SparkAppHandle.State.FINISHED)
  }

  test("The pod status watcher should notify launcher backend " +
    "with unknown state if pod status is not known.") {
    podStatusWatcher.eventReceived(ADDED, podWithPhase("SomeUnknownPodStatus"))
    verify(launcherBackend).setState(SparkAppHandle.State.UNKNOWN)
  }

  private def k8sDriverConf = KubernetesTestConf.createDriverConf(
    resourceNamePrefix = Some("resource-example")
  )

  private def podWithPhase(phase: String): Pod = {
    new Pod() {
      setMetadata(new ObjectMeta() {
        setName("pod")
        setNamespace("namespace")
        setLabels(new util.HashMap())
        setUid("uid")
        setCreationTimestamp("now")
      })
      setSpec(new PodSpec() {
        setServiceAccountName("sa")
        setNodeName("nodeName")
      })
      setStatus(new PodStatus() {
        setPhase(phase)
        setStartTime("now")
      })
    }
  }

}
