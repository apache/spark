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

import com.google.common.util.concurrent.SettableFuture
import io.fabric8.kubernetes.api.model.HasMetadata
import io.fabric8.kubernetes.client.{KubernetesClientException, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import io.fabric8.kubernetes.client.internal.readiness.Readiness

private[spark] class SparkReadinessWatcher[T <: HasMetadata] extends Watcher[T] {

  private val signal = SettableFuture.create[Boolean]

  override def eventReceived(action: Action, resource: T): Unit = {
    if ((action == Action.MODIFIED || action == Action.ADDED) &&
        Readiness.isReady(resource)) {
      signal.set(true)
    }
  }

  override def onClose(cause: KubernetesClientException): Unit = {}

  def waitUntilReady(): Boolean = signal.get(60, TimeUnit.SECONDS)
}
