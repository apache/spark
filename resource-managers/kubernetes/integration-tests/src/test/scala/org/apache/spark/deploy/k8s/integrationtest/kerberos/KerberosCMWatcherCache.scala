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

import io.fabric8.kubernetes.api.model.ConfigMap
import io.fabric8.kubernetes.client.{KubernetesClientException, Watch, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action

import org.apache.spark.internal.Logging

 /**
  * This class is responsible for ensuring that no logic progresses in the cluster launcher
  * until a configmap with the HADOOP_CONF_DIR specifications has been created.
  */
private[spark] class KerberosCMWatcherCache(kerberosUtils: KerberosUtils) extends Logging {
  private val kubernetesClient = kerberosUtils.getClient
  private val namespace = kerberosUtils.getNamespace
  private val requiredFiles = Seq("core-site.xml", "hdfs-site.xml", "krb5.conf")
  private var watcher: Watch = _
  private var cmCache = scala.collection.mutable.Map[String, Map[String, String]]()
  private var lock: Lock = new ReentrantLock()
  private var cmCreated: Condition = lock.newCondition()
  private val configMap = kerberosUtils.getConfigMap
  private val configMapName = configMap.getMetadata.getName
  private val blockingThread = new Thread(new Runnable {
    override def run(): Unit = {
      logInfo("Beginning of ConfigMap lock")
      lock.lock()
      try {
        while (!created()) cmCreated.await()
      } finally {
        logInfo("Ending the ConfigMap lock")
        lock.unlock()
        stop()
      }
    }})

  private val watcherThread = new Thread(new Runnable {
    override def run(): Unit = {
      logInfo("Beginning the watch of the Kerberos Config Map")
      watcher = kubernetesClient
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
                if (created()) {
                  lock.lock()
                  try {
                    cmCreated.signalAll()
                  } finally {
                    lock.unlock()
                  }
                }
            }}}
        )
      logInfo("Launching the Config Map")
      kerberosUtils.getClient.configMaps().inNamespace(namespace).createOrReplace(configMap)
    }})

  def start(): Unit = {
    blockingThread.start()
    watcherThread.start()
    blockingThread.join()
    watcherThread.join()}

  def stop(): Unit = {
    watcher.close()
  }

  def created(): Boolean = {
    cmCache.get(configMapName).exists{ data =>
      requiredFiles.forall(data.keys.toSeq.contains)}
  }
}
