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

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.features.KubernetesTolerationSpec

private[spark] object KubernetesSchedulerUtils {
  /**
   * Extract Spark toleration configuration properties with a given name prefix.
   *
   * @param sparkConf Spark configuration
   * @param prefix the given property name prefix
   * @return a Map storing with toleration key and
   *         effect and value and operator and tolerationSeconds
   */
  def parseTolerationsWithPrefix(sparkConf: SparkConf,
                                 prefix: String): Seq[KubernetesTolerationSpec] = {
    val properties = sparkConf.getAllWithPrefix(prefix).toMap

    getTaintKeys(properties).map { taintKey =>
      val valueKey = s"$taintKey.$KUBERNETES_TOLERATIONS_VALUE"
      val effectKey = s"$taintKey.$KUBERNETES_TOLERATIONS_EFFECT"
      val operatorKeys = s"$taintKey.$KUBERNETES_TOLERATIONS_OPTIONS_OPERATOR"
      val tolerationSecondsKeys = s"$taintKey.$KUBERNETES_TOLERATIONS_OPTIONS_TOLERATIONS_SECONDS"

      KubernetesTolerationSpec(
        key = taintKey,
        value = properties(valueKey),
        effect = properties(effectKey),
        operator = properties.get(operatorKeys),
        tolerationSeconds = properties.get(tolerationSecondsKeys)
      )
    }.toSeq
  }

  /**
   * Get unique pairs of toleration properties,
   * assuming options are formatted in this way:
   * `taintKey`.`property` = `value`
   * @param properties flat mapping of property names to values
   * @return Set[taintKey]
   */
  private def getTaintKeys(properties: Map[String, String]): Set[String] = {
    properties.keys.flatMap { k =>
      k.split('.').toList match {
        case tpe :: _ :: _ => Some(tpe)
        case _ => None
      }
    }.toSet
  }
}
