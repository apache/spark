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
package org.apache.spark.deploy.k8s.submit.steps.initcontainer

import org.apache.spark.deploy.k8s.MountSecretsBootstrap

/**
 * An init-container configuration step for mounting user-specified secrets onto user-specified
 * paths.
 *
 * @param mountSecretsBootstrap a utility actually handling mounting of the secrets
 */
private[spark] class InitContainerMountSecretsStep(
    mountSecretsBootstrap: MountSecretsBootstrap) extends InitContainerConfigurationStep {

  override def configureInitContainer(initContainerSpec: InitContainerSpec) : InitContainerSpec = {
    val (podWithSecretsMounted, initContainerWithSecretsMounted) =
      mountSecretsBootstrap.mountSecrets(
        initContainerSpec.driverPod,
        initContainerSpec.initContainer)
    initContainerSpec.copy(
      driverPod = podWithSecretsMounted,
      initContainer = initContainerWithSecretsMounted
    )
  }
}
