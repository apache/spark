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
package org.apache.spark.deploy.k8s

import java.util.NoSuchElementException

import scala.util.{Failure, Success, Try}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config._

private[spark] object KubernetesTolerationUtils {
  def parseTolerationsWithPrefix(
    sparkConf: SparkConf,
    prefix: String): Iterable[KubernetesTolerationSpec] = {
    val properties = sparkConf.getAllWithPrefix(prefix).toMap

    properties.groupBy { case (k, v) => k.split('.')(0) }
      .map { case (idx, tolerationProperties) =>
        val keyKey = s"$idx.$KUBERNETES_TOLERATIONS_KEY_KEY"
        val operatorKey = s"$idx.$KUBERNETES_TOLERATIONS_OPERATOR_KEY"
        val effectKey = s"$idx.$KUBERNETES_TOLERATIONS_EFFECT_KEY"
        val valueKey = s"$idx.$KUBERNETES_TOLERATIONS_VALUE_KEY"
        val tolerationSecondsKey = s"$idx.$KUBERNETES_TOLERATIONS_SECOND_KEY"

        KubernetesTolerationSpec(
          key = properties.get(keyKey),
          operator = properties(operatorKey),
          effect = properties.get(effectKey),
          value = properties.get(valueKey),
          tolerationSeconds = properties.get(tolerationSecondsKey) match {
            case Some(a) => Some(a.toLong)
            case _ => None
          }
        )
      }
  }
}
