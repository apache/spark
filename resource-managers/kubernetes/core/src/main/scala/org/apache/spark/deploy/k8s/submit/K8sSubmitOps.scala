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

import scala.jdk.CollectionConverters._

import K8SSparkSubmitOperation.getGracePeriod
import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.PodResource

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkSubmitOperation
import org.apache.spark.deploy.k8s.{KubernetesUtils, SparkKubernetesClientFactory}
import org.apache.spark.deploy.k8s.Config.{KUBERNETES_AUTH_SUBMISSION_CONF_PREFIX, KUBERNETES_SUBMIT_GRACE_PERIOD}
import org.apache.spark.deploy.k8s.Constants.{SPARK_POD_DRIVER_ROLE, SPARK_ROLE_LABEL}
import org.apache.spark.deploy.k8s.KubernetesUtils.formatPodState
import org.apache.spark.util.{CommandLineLoggingUtils, Utils}

private sealed trait K8sSubmitOp extends CommandLineLoggingUtils {
  def executeOnPod(pName: String, namespace: Option[String], sparkConf: SparkConf)
      (implicit client: KubernetesClient): Unit
  def executeOnGlob(pods: List[Pod], ns: Option[String], sparkConf: SparkConf)
      (implicit client: KubernetesClient): Unit
  def getPod(namespace: Option[String], name: String)
      (implicit client: KubernetesClient): PodResource = {
    namespace match {
      case Some(ns) => client.pods.inNamespace(ns).withName(name)
      case None => client.pods.withName(name)
    }
  }
}

private class KillApplication extends K8sSubmitOp  {
  override def executeOnPod(pName: String, namespace: Option[String], sparkConf: SparkConf)
      (implicit client: KubernetesClient): Unit = {
    val podToDelete = getPod(namespace, pName)

    if (Option(podToDelete).isDefined) {
      getGracePeriod(sparkConf) match {
        case Some(period) => podToDelete.withGracePeriod(period).delete()
        case _ => podToDelete.delete()
      }
    } else {
      printMessage("Application not found.")
    }
  }

  override def executeOnGlob(pods: List[Pod], namespace: Option[String], sparkConf: SparkConf)
      (implicit client: KubernetesClient): Unit = {
    if (pods.nonEmpty) {
      pods.foreach { pod => printMessage(s"Deleting driver pod: ${pod.getMetadata.getName}.") }
      getGracePeriod(sparkConf) match {
        case Some(period) =>
          client.resourceList(pods.asJava).withGracePeriod(period).delete()
        case _ =>
          client.resourceList(pods.asJava).delete()
      }
    } else {
      printMessage("No applications found.")
    }
  }
}

private class ListStatus extends K8sSubmitOp {
  override def executeOnPod(pName: String, namespace: Option[String], sparkConf: SparkConf)
      (implicit client: KubernetesClient): Unit = {
    val pod = getPod(namespace, pName).get()
    if (Option(pod).isDefined) {
      printMessage("Application status (driver): " +
        Option(pod).map(formatPodState).getOrElse("unknown."))
    } else {
      printMessage("Application not found.")
    }
  }

  override def executeOnGlob(pods: List[Pod], ns: Option[String], sparkConf: SparkConf)
      (implicit client: KubernetesClient): Unit = {
    if (pods.nonEmpty) {
      for (pod <- pods) {
        printMessage("Application status (driver): " +
          Option(pod).map(formatPodState).getOrElse("unknown."))
      }
    } else {
      printMessage("No applications found.")
    }
  }
}

private[spark] class K8SSparkSubmitOperation extends SparkSubmitOperation
  with CommandLineLoggingUtils {

  private def isGlob(name: String): Boolean = {
    name.last == '*'
  }

  def execute(submissionId: String, sparkConf: SparkConf, op: K8sSubmitOp): Unit = {
    val master = KubernetesUtils.parseMasterUrl(sparkConf.get("spark.master"))
    submissionId.split(":", 2) match {
      case Array(part1, part2@_*) =>
        val namespace = if (part2.isEmpty) None else Some(part1)
        val pName = if (part2.isEmpty) part1 else part2.headOption.get
        Utils.tryWithResource(SparkKubernetesClientFactory.createKubernetesClient(
          master,
          namespace,
          KUBERNETES_AUTH_SUBMISSION_CONF_PREFIX,
          SparkKubernetesClientFactory.ClientType.Submission,
          sparkConf,
          None)
        ) { kubernetesClient =>
          implicit val client: KubernetesClient = kubernetesClient
          if (isGlob(pName)) {
            val ops = namespace match {
              case Some(ns) =>
                kubernetesClient
                  .pods
                  .inNamespace(ns)
              case None =>
                kubernetesClient
                  .pods
            }
            val pods = ops
              .withLabel(SPARK_ROLE_LABEL, SPARK_POD_DRIVER_ROLE)
              .list()
              .getItems
              .asScala
              .filter { pod =>
                pod.getMetadata.getName.startsWith(pName.stripSuffix("*"))
              }.toList
            op.executeOnGlob(pods, namespace, sparkConf)
          } else {
            op.executeOnPod(pName, namespace, sparkConf)
          }
        }
      case _ =>
        printErrorAndExit(s"Submission ID: {$submissionId} is invalid.")
    }
  }

  override def kill(submissionId: String, conf: SparkConf): Unit = {
    printMessage(s"Submitting a request to kill submission " +
      s"${submissionId} in ${conf.get("spark.master")}. " +
      s"Grace period in secs: ${getGracePeriod(conf).getOrElse("not set.")}")
    execute(submissionId, conf, new KillApplication)
  }

  override def printSubmissionStatus(submissionId: String, conf: SparkConf): Unit = {
    printMessage(s"Submitting a request for the status of submission" +
      s" ${submissionId} in ${conf.get("spark.master")}.")
    execute(submissionId, conf, new ListStatus)
  }

  override def supports(master: String): Boolean = {
    master.startsWith("k8s://")
  }
}

private object K8SSparkSubmitOperation {
  def getGracePeriod(sparkConf: SparkConf): Option[Long] = {
    sparkConf.get(KUBERNETES_SUBMIT_GRACE_PERIOD)
  }
}
