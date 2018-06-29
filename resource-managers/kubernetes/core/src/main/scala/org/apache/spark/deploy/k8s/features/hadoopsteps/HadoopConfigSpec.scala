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
package org.apache.spark.deploy.k8s.features.hadoopsteps

import io.fabric8.kubernetes.api.model.{EnvVar, Secret, Volume, VolumeMount}

 /**
  * Represents a given configuration of the hadoop configuration logic, informing the
  * HadoopConfigBootstrapStep of how the driver should be configured. This includes:
  * <p>
  * - Volumes that need to mounted onto the pod
  * - Environmental variables that need to be launched with the container
  * - Volume Mounts that need to mounted with the container
  * - The properties that will be stored into the config map which have (key, value)
  *   pairs of (path, data)
  * - The secret containing a DT, either previously specified or built on the fly
  * - The name of the secret where the DT will be stored
  * - The data item-key on the secret which correlates with where the current DT data is stored
  * - The Job User's username
  */
private[spark] case class HadoopConfigSpec(
  podVolumes: Seq[Volume],
  containerEnvs: Seq[EnvVar],
  containerVMs: Seq[VolumeMount],
  configMapProperties: Map[String, String],
  dtSecret: Option[Secret],
  dtSecretName: String,
  dtSecretItemKey: Option[String],
  jobUserName: Option[String])
