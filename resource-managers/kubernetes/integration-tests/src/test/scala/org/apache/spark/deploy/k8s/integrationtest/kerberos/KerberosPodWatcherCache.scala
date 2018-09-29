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

import io.fabric8.kubernetes.api.model.{Pod, Service}
import io.fabric8.kubernetes.client.{KubernetesClientException, Watch, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import org.scalatest.Matchers
import org.scalatest.concurrent.Eventually
import scala.collection.JavaConverters._

import org.apache.spark.deploy.k8s.integrationtest.KubernetesSuite.{INTERVAL, TIMEOUT}
import org.apache.spark.internal.Logging

 /**
  * This class is used to ensure that the Hadoop cluster that is launched is executed
  * in this order: KDC --> NN --> DN --> Data-Populator and that each one of these nodes
  * is running before launching the Kerberos test.
  */
private[spark] class KerberosPodWatcherCache(
  kerberosUtils: KerberosUtils,
  labels: Map[String, String]) extends Logging with Eventually with Matchers {

   private val kubernetesClient = kerberosUtils.getClient
   private val namespace = kerberosUtils.getNamespace
   private val podCache = scala.collection.mutable.Map[String, String]()
   private val serviceCache = scala.collection.mutable.Map[String, String]()
   private var kdcName: String = _
   private var nnName: String = _
   private var dnName: String = _
   private var dpName: String = _
   private val podWatcher: Watch = kubernetesClient
     .pods()
     .withLabels(labels.asJava)
     .watch(new Watcher[Pod] {
       override def onClose(cause: KubernetesClientException): Unit =
         logInfo("Ending the watch of Pods")
       override def eventReceived(action: Watcher.Action, resource: Pod): Unit = {
         val name = resource.getMetadata.getName
         val keyName = podNameParse(name)
         action match {
           case Action.DELETED | Action.ERROR =>
             logInfo(s"$name either deleted or error")
             podCache.remove(keyName)
           case Action.ADDED | Action.MODIFIED =>
             val phase = resource.getStatus.getPhase
             logInfo(s"$name is as $phase")
             if (keyName == "kerberos") { kdcName = name }
             if (keyName == "nn") { nnName = name }
             if (keyName == "dn1") { dnName = name }
             if (keyName == "data-populator") { dpName = name }
             podCache(keyName) = phase }}})

   private val serviceWatcher: Watch = kubernetesClient
     .services()
     .withLabels(labels.asJava)
     .watch(new Watcher[Service] {
       override def onClose(cause: KubernetesClientException): Unit =
         logInfo("Ending the watch of Services")
       override def eventReceived(action: Watcher.Action, resource: Service): Unit = {
         val name = resource.getMetadata.getName
         action match {
           case Action.DELETED | Action.ERROR =>
             logInfo(s"$name either deleted or error")
             serviceCache.remove(name)
           case Action.ADDED | Action.MODIFIED =>
             val bound = resource.getSpec.getSelector.get("kerberosService")
             logInfo(s"$name is bounded to $bound")
             serviceCache(name) = bound }}})

   private def additionalCheck(name: String): Boolean = {
     name match {
       case "kerberos" => hasInLogs(kdcName, "krb5kdc: starting")
       case "nn" => hasInLogs(nnName, "createNameNode")
       case "dn1" => hasInLogs(dnName, "Got finalize command for block pool")
       case "data-populator" => hasInLogs(dpName, "Entered Krb5Context.initSecContext")
     }
   }

   private def check(name: String): Boolean = {
     podCache.get(name).contains("Running") &&
     serviceCache.get(name).contains(name) &&
     additionalCheck(name)
    }

   def deploy(kdc: KerberosDeployment) : Unit = {
     logInfo("Launching the Deployment")
     kubernetesClient
       .extensions().deployments().inNamespace(namespace).create(kdc.podDeployment)
     // Making sure Pod is running
     Eventually.eventually(TIMEOUT, INTERVAL) {
       (podCache(kdc.name) == "Running") should be (true) }
     kubernetesClient
       .services().inNamespace(namespace).create(kdc.service)
     Eventually.eventually(TIMEOUT, INTERVAL) { check(kdc.name) should be (true) }
  }

   def stopWatch(): Unit = {
     // Closing Watchers
     podWatcher.close()
     serviceWatcher.close()
   }

  private def podNameParse(name: String) : String = {
    name match {
      case _ if name.startsWith("kerberos") => "kerberos"
      case _ if name.startsWith("nn") => "nn"
      case _ if name.startsWith("dn1") => "dn1"
      case _ if name.startsWith("data-populator") => "data-populator"
    }
  }

  def hasInLogs(name: String, expectation: String): Boolean = {
    kubernetesClient
      .pods()
      .withName(name)
      .getLog().contains(expectation)
  }
}
