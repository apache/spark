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

import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.api.model.extensions.Deployment
import io.fabric8.kubernetes.client.{KubernetesClientException, Watch, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import org.scalatest.Matchers
import org.scalatest.concurrent.Eventually
import scala.collection.JavaConverters._

import org.apache.spark.deploy.k8s.integrationtest.KubernetesSuite.{INTERVAL, TIMEOUT}
import org.apache.spark.internal.Logging

 /**
  * This class is responsible for ensuring that the driver-pod launched by the KerberosTestPod
  * is running before trying to grab its logs for the sake of monitoring success of completition.
  */
private[spark] class KerberosDriverWatcherCache(
   kerberosUtils: KerberosUtils,
   labels: Map[String, String])
   extends WatcherCacheConfiguration[DeploymentStorage] with Logging with Eventually with Matchers {
   private val kubernetesClient = kerberosUtils.getClient
   private val namespace = kerberosUtils.getNamespace
   private var driverName: String = ""
   private val podCache =
     scala.collection.mutable.Map[String, String]()
   private val watcher: Watch = kubernetesClient
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
             if (name.contains("driver")) { driverName = name }
          }}})

   override def check(name: String): Boolean = podCache.get(name).contains("Running")

   override def deploy(storage: DeploymentStorage) : Unit = {
     kubernetesClient.extensions().deployments()
       .inNamespace(namespace).create(storage.resource)
     Eventually.eventually(TIMEOUT, INTERVAL) { check(driverName) should be (true) }
   }

   override def stopWatch(): Unit = {
     // Closing Watch
     watcher.close()
   }
}
