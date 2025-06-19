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

private[spark] sealed trait KubernetesVolumeSpecificConf

private[spark] case class KubernetesHostPathVolumeConf(hostPath: String, volumeType: String)
  extends KubernetesVolumeSpecificConf

private[spark] case class KubernetesPVCVolumeConf(
    claimName: String,
    storageClass: Option[String] = None,
    size: Option[String] = None,
    labels: Option[Map[String, String]] = None,
    annotations: Option[Map[String, String]] = None)
  extends KubernetesVolumeSpecificConf

private[spark] case class KubernetesEmptyDirVolumeConf(
    medium: Option[String],
    sizeLimit: Option[String])
  extends KubernetesVolumeSpecificConf

private[spark] case class KubernetesNFSVolumeConf(
    path: String,
    server: String)
  extends KubernetesVolumeSpecificConf

private[spark] case class KubernetesVolumeSpec(
    volumeName: String,
    mountPath: String,
    mountSubPath: String,
    mountSubPathExpr: String,
    mountReadOnly: Boolean,
    volumeConf: KubernetesVolumeSpecificConf)
