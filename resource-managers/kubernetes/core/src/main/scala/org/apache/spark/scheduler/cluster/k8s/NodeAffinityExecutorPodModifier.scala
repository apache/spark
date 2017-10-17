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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.fabric8.kubernetes.api.model.{Pod, PodBuilder}

import org.apache.spark.deploy.k8s.constants.ANNOTATION_EXECUTOR_NODE_AFFINITY
import org.apache.spark.internal.Logging

// Applies a node affinity annotation to executor pods so that pods can be placed optimally for
// locality.
private[spark] trait NodeAffinityExecutorPodModifier {
   def addNodeAffinityAnnotationIfUseful(
       baseExecutorPod: Pod, nodeToTaskCount: Map[String, Int]): Pod
}

private[spark] object NodeAffinityExecutorPodModifierImpl
    extends NodeAffinityExecutorPodModifier with Logging {

  private val OBJECT_MAPPER = new ObjectMapper().registerModule(DefaultScalaModule)

  private def scaleToRange(
      value: Int,
      baseMin: Double,
      baseMax: Double,
      rangeMin: Double,
      rangeMax: Double): Int = {
    (((rangeMax - rangeMin) * (value - baseMin) / (baseMax - baseMin)) + rangeMin).toInt
  }
  override def addNodeAffinityAnnotationIfUseful(
      baseExecutorPod: Pod, nodeToTaskCount: Map[String, Int]): Pod = {
    if (nodeToTaskCount.nonEmpty) {
      val taskTotal = nodeToTaskCount.foldLeft(0)(_ + _._2)
      // Normalize to node affinity weights in 1 to 100 range.
      val nodeToWeight = nodeToTaskCount.map {
        case (node, taskCount) =>
          (node, scaleToRange(taskCount, 1, taskTotal, rangeMin = 1, rangeMax = 100))
      }
      val weightToNodes = nodeToWeight.groupBy(_._2).mapValues(_.keys)
      // @see https://kubernetes.io/docs/concepts/configuration/assign-pod-node
      val nodeAffinityJson = OBJECT_MAPPER.writeValueAsString(SchedulerAffinity(NodeAffinity(
        preferredDuringSchedulingIgnoredDuringExecution =
          for ((weight, nodes) <- weightToNodes) yield {
            WeightedPreference(
              weight,
              Preference(Array(MatchExpression("kubernetes.io/hostname", "In", nodes))))
          })))
      // TODO: Use non-annotation syntax when we switch to K8s version 1.6.
      logDebug(s"Adding nodeAffinity as annotation $nodeAffinityJson")
      new PodBuilder(baseExecutorPod)
        .editMetadata()
        .addToAnnotations(ANNOTATION_EXECUTOR_NODE_AFFINITY, nodeAffinityJson)
        .endMetadata()
        .build()
    } else {
      baseExecutorPod
    }
  }
}

// These case classes model K8s node affinity syntax fo
// preferredDuringSchedulingIgnoredDuringExecution.
// see https://kubernetes.io/docs/concepts/configuration/assign-pod-node
private case class SchedulerAffinity(nodeAffinity: NodeAffinity)
private case class NodeAffinity(
    preferredDuringSchedulingIgnoredDuringExecution: Iterable[WeightedPreference])
private case class WeightedPreference(weight: Int, preference: Preference)
private case class Preference(matchExpressions: Array[MatchExpression])
private case class MatchExpression(key: String, operator: String, values: Iterable[String])
