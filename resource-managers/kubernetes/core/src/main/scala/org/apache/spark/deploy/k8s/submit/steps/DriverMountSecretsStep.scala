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
package org.apache.spark.deploy.k8s.submit.steps

import org.apache.spark.deploy.k8s.MountSecretsBootstrap
import org.apache.spark.deploy.k8s.submit.KubernetesDriverSpec

/**
 * A driver configuration step for mounting user-specified secrets onto user-specified paths.
 *
 * @param bootstrap a utility actually handling mounting of the secrets.
 */
private[spark] class DriverMountSecretsStep(
    bootstrap: MountSecretsBootstrap) extends DriverConfigurationStep {

  override def configureDriver(driverSpec: KubernetesDriverSpec): KubernetesDriverSpec = {
    val (pod, container) = bootstrap.mountSecrets(
      driverSpec.driverPod, driverSpec.driverContainer, addNewVolumes = true)
    driverSpec.copy(
      driverPod = pod,
      driverContainer = container
    )
  }
}
