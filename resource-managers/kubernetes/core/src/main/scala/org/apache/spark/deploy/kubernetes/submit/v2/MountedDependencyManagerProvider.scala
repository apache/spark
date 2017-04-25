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
package org.apache.spark.deploy.kubernetes.submit.v2

import org.apache.spark.{SecurityManager => SparkSecurityManager, SparkConf}
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.rest.kubernetes.v2.RetrofitClientFactoryImpl

private[spark] trait MountedDependencyManagerProvider {
  def getMountedDependencyManager(
    kubernetesAppId: String,
    stagingServerUri: String,
    podLabels: Map[String, String],
    podNamespace: String,
    sparkJars: Seq[String],
    sparkFiles: Seq[String]): MountedDependencyManager
}

private[spark] class MountedDependencyManagerProviderImpl(sparkConf: SparkConf)
    extends MountedDependencyManagerProvider {
  override def getMountedDependencyManager(
      kubernetesAppId: String,
      stagingServerUri: String,
      podLabels: Map[String, String],
      podNamespace: String,
      sparkJars: Seq[String],
      sparkFiles: Seq[String]): MountedDependencyManager = {
    val resourceStagingServerSslOptions = new SparkSecurityManager(sparkConf)
      .getSSLOptions("kubernetes.resourceStagingServer")
    new MountedDependencyManagerImpl(
      kubernetesAppId,
      podLabels,
      podNamespace,
      stagingServerUri,
      sparkConf.get(INIT_CONTAINER_DOCKER_IMAGE),
      sparkConf.get(DRIVER_LOCAL_JARS_DOWNLOAD_LOCATION),
      sparkConf.get(DRIVER_LOCAL_FILES_DOWNLOAD_LOCATION),
      sparkConf.get(DRIVER_MOUNT_DEPENDENCIES_INIT_TIMEOUT),
      sparkJars,
      sparkFiles,
      resourceStagingServerSslOptions,
      RetrofitClientFactoryImpl)
  }
}
