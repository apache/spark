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

package org.apache.spark.deploy.k8s.integrationtest.kerberos

import java.util.concurrent.locks.{Condition, Lock, ReentrantLock}

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientException, Watch, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action

import org.apache.spark.internal.Logging

 /**
  * This class is responsible for ensuring that the driver-pod launched by the KerberosTestPod
  * is running before trying to grab its logs for the sake of monitoring success of completition.
  */
private[spark] class KerberosDriverWatcherCache(
  kubernetesClient: KubernetesClient,
  labels: Map[String, String]) extends Logging {
  private var podWatcher: Watch = _
  private var podCache =
     scala.collection.mutable.Map[String, String]()
  private var lock: Lock = new ReentrantLock()
  private var driverRunning: Condition = lock.newCondition()
  private var driverIsUp: Boolean = false
  private val blockingThread = new Thread(new Runnable {
     override def run(): Unit = {
       logInfo("Beginning of Driver lock")
       lock.lock()
       try {
         while (!driverIsUp) driverRunning.await()
       } finally {
         logInfo("Ending the Driver lock")
         lock.unlock()
         stop()
       }
     }
   })

  private val podWatcherThread = new Thread(new Runnable {
    override def run(): Unit = {
      logInfo("Beginning the watch of Driver pod")
      podWatcher = kubernetesClient
        .pods()
        .withLabels(labels.asJava)
        .watch(new Watcher[Pod] {
          override def onClose(cause: KubernetesClientException): Unit =
            logInfo("Ending the watch of Driver pod")
          override def eventReceived(action: Watcher.Action, resource: Pod): Unit = {
            val name = resource.getMetadata.getName
            action match {
              case Action.DELETED | Action.ERROR =>
                logInfo(s"$name either deleted or error")
                podCache.remove(name)
              case Action.ADDED | Action.MODIFIED =>
                val phase = resource.getStatus.getPhase
                logInfo(s"$name is as $phase")
                podCache(name) = phase
                if (maybeDriverDone(name)) {
                    lock.lock()
                    try {
                      driverIsUp = true
                      driverRunning.signalAll()
                    } finally {
                      lock.unlock()
                    }
                }}}})
    }})

  def start(): Unit = {
    blockingThread.start()
    podWatcherThread.start()
    blockingThread.join()
    podWatcherThread.join()
  }

  def stop(): Unit = {
    podWatcher.close()
  }

  private def maybeDriverDone(name: String): Boolean = podCache.get(name).contains("Running")
}
