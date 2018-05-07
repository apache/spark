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

import scala.collection.mutable.HashMap

import io.fabric8.kubernetes.api.model._

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.Config._

private[spark] object KubernetesVolumeUtils {

  /**
   * Extract Spark volume configuration properties with a given name prefix.
   *
   * @param sparkConf Spark configuration
   * @param prefix the given property name prefix
   * @return a Map storing with volume name as key and spec as value
   */
  def parseVolumesWithPrefix(
    sparkConf: SparkConf,
    prefix: String): Iterable[KubernetesVolumeSpec] = {
    val properties = sparkConf.getAllWithPrefix(prefix)

    val propsByTypeName = properties.map { case (k, v) =>
      k.split('.').toSeq match {
        case tpe :: name :: rest => ((tpe, name), (rest.mkString("."), v))
      }
    }.groupBy(_._1).mapValues(_.map(_._2))

    propsByTypeName.map { case ((tpe, name), props) =>
      val mountProps = props.filter(_._1.startsWith(KUBERNETES_VOLUMES_MOUNT_KEY)).toMap
      val options = props.filter(_._1.startsWith(KUBERNETES_VOLUMES_OPTIONS_KEY)).toMap

      KubernetesVolumeSpec(
        volumeName = name,
        volumeType = tpe,
        mountPath = mountProps(KUBERNETES_VOLUMES_PATH_KEY),
        mountReadOnly = mountProps(KUBERNETES_VOLUMES_READONLY_KEY).toBoolean,
        optionsSpec = options
      )
    }
  }

}
