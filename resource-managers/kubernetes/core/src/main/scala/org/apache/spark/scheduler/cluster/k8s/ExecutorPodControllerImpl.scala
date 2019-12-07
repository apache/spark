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

import java.util.concurrent.LinkedBlockingQueue

import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.KubernetesClient
import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Constants._


private[spark] class ExecutorPodControllerImpl(
    val conf: SparkConf)
  extends ExecutorPodController {

  private var kubernetesClient: KubernetesClient = _
  private var removePodList: LinkedBlockingQueue[String] = _
  private var addPodList: LinkedBlockingQueue[Pod] = _

  private var appId: String = _

  override def initialize(kClient: KubernetesClient, _appId: String) : Unit = {
    kubernetesClient = kClient
    appId = _appId
    removePodList = new LinkedBlockingQueue[String]()
    addPodList = new LinkedBlockingQueue[Pod]()
  }
  override def addPod(pod: Pod): Unit = {
    addPodList.add(pod)
  }

  override def commitAndGetTotalAllocated(): Int = {
    val finalAddList = mutable.Buffer.empty[Pod].asJava
    addPodList.drainTo(finalAddList)
    val finalList = finalAddList.asScala.map{
      kubernetesClient.pods().create(_)
    }
    finalList.size
  }

  override def removePodById(execId: String): Unit = {
    removePodList.add(execId)
  }

  override def removePod(pod: Pod): Unit = {
    kubernetesClient.pods().delete(pod)
  }

  override def commitAndGetTotalDeleted(): Int = {
    val finalRemoveList = mutable.Buffer.empty[String].asJava
    removePodList.drainTo(finalRemoveList)
    val finalList = finalRemoveList.asScala
    kubernetesClient
      .pods()
      .withLabel(SPARK_APP_ID_LABEL, appId)
      .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
      .withLabelIn(SPARK_EXECUTOR_ID_LABEL, finalList: _*)
      .delete()
    finalRemoveList.size()
  }

  override def removePods(execIds: Seq[Long], status: Option[String]): Iterable[Long] = {
    var shouldSearch = false
    var ids = execIds
    val execStringIds = execIds.map(_.toString)
    val filterable = status match {
      case Some(phase) =>
        shouldSearch = true
        kubernetesClient
          .pods()
          .withField("status.phase", phase)
          .withLabel(SPARK_APP_ID_LABEL, appId)
          .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
          .withLabelIn(SPARK_EXECUTOR_ID_LABEL, execStringIds: _*)
      case None =>
        kubernetesClient
          .pods()
          .withLabel(SPARK_APP_ID_LABEL, appId)
          .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
          .withLabelIn(SPARK_EXECUTOR_ID_LABEL, execStringIds: _*)
    }
    if (shouldSearch) {
      ids = filterable.list().getItems().asScala.map {
        _.getMetadata().getLabels().get(SPARK_EXECUTOR_ID_LABEL).toLong }
    }
    filterable.delete()
    ids
  }

  override def removeAllPods(): Boolean = {
    kubernetesClient
      .pods()
      .withLabel(SPARK_APP_ID_LABEL, appId)
      .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
      .delete()
  }

  override def stop(): Boolean = {
    removeAllPods()
  }
}
