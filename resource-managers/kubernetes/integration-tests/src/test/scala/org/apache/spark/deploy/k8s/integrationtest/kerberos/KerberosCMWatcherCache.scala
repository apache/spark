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

import io.fabric8.kubernetes.api.model.ConfigMap
import io.fabric8.kubernetes.client.{KubernetesClientException, Watch, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import org.scalatest.Matchers
import org.scalatest.concurrent.Eventually
import scala.collection.JavaConverters._

import org.apache.spark.deploy.k8s.integrationtest.KubernetesSuite.{INTERVAL, TIMEOUT}
import org.apache.spark.internal.Logging

 /**
  * This class is responsible for ensuring that no logic progresses in the cluster launcher
  * until a configmap with the HADOOP_CONF_DIR specifications has been created.
  */
private[spark] class KerberosCMWatcherCache(kerberosUtils: KerberosUtils)
   extends WatcherCacheConfiguration with Logging with Eventually with Matchers {
   private val kubernetesClient = kerberosUtils.getClient
   private val namespace = kerberosUtils.getNamespace
   private val requiredFiles = Seq("core-site.xml", "hdfs-site.xml", "krb5.conf")
   private val cmCache = scala.collection.mutable.Map[String, Map[String, String]]()
   private val configMapName = kerberosUtils.getConfigMap.resource.getMetadata.getName
   // Watching ConfigMaps
   logInfo("Beginning the watch of the Kerberos Config Map")
   private val watcher: Watch = kubernetesClient
     .configMaps()
     .withName(configMapName)
     .watch(new Watcher[ConfigMap] {
       override def onClose(cause: KubernetesClientException): Unit =
         logInfo("Ending the watch of Kerberos Config Map")
       override def eventReceived(action: Watcher.Action, resource: ConfigMap): Unit = {
         val name = resource.getMetadata.getName
         action match {
           case Action.DELETED | Action.ERROR =>
             logInfo(s"$name either deleted or error")
             cmCache.remove(name)
           case Action.ADDED | Action.MODIFIED =>
             val data = resource.getData.asScala.toMap
             logInfo(s"$name includes ${data.keys.mkString(",")}")
             cmCache(name) = data
         }}})
   // Check for CM to have proper files
   override def check(name: String): Boolean = {
     cmCache.get(name).exists{ data => requiredFiles.forall(data.keys.toSeq.contains)}
   }

   override def deploy[T <: ResourceStorage[ConfigMap]](storage: T): Unit = {
     logInfo("Launching the ConfigMap")
     kerberosUtils.getClient.configMaps()
       .inNamespace(namespace).createOrReplace(storage.resource)
     // Making sure CM has correct files
     Eventually.eventually(TIMEOUT, INTERVAL) {
       check(configMapName) should be (true) }
   }

   override def stopWatch() : Unit = {
     // Closing Watcher
     watcher.close()
   }
}
