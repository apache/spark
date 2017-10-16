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
package org.apache.spark.deploy.k8s.submit.submitsteps

import java.nio.file.Paths
import java.util.UUID

import io.fabric8.kubernetes.api.model.{ContainerBuilder, PodBuilder, VolumeBuilder, VolumeMountBuilder}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.constants._

/**
 * Configures local directories that the driver and executors should use for temporary storage.
 *
 * Note that we have different semantics for scratch space in Kubernetes versus the other cluster
 * managers. In Kubernetes, we cannot allow the local directories to resolve to the Java temporary
 * directory. This is because we will mount either emptyDir volumes for both the driver and
 * executors, or hostPath volumes for the executors and an emptyDir for the driver. In either
 * case, the mount paths need to be directories that do not exist in the base container images.
 * But the Java temporary directory is typically a directory like /tmp which exists in most
 * container images.
 *
 * The solution is twofold:
 * - When not using an external shuffle service, a reasonable default is to create a new directory
 *   with a random name and set that to be the value of `spark.local.dir`.
 * - When using the external shuffle service, it is risky to assume that the user intends to mount
 *   the JVM temporary directory into the pod as a hostPath volume. We therefore enforce that
 *   spark.local.dir must be set in dynamic allocation mode so that the user explicitly sets the
 *   paths that have to be mounted.
 */
private[spark] class LocalDirectoryMountConfigurationStep(
    submissionSparkConf: SparkConf,
    randomDirProvider: () => String = () => s"spark-${UUID.randomUUID()}")
    extends DriverConfigurationStep {

  override def configureDriver(driverSpec: KubernetesDriverSpec): KubernetesDriverSpec = {
    val configuredLocalDirs = submissionSparkConf.getOption("spark.local.dir")
    val isUsingExternalShuffle = submissionSparkConf.get(
        org.apache.spark.internal.config.SHUFFLE_SERVICE_ENABLED)
    val resolvedLocalDirsSingleString = if (isUsingExternalShuffle) {
      require(configuredLocalDirs.isDefined, "spark.local.dir must be provided explicitly when" +
          " using the external shuffle service in Kubernetes. These directories should map to" +
          " the paths that are mounted into the external shuffle service pods.")
      configuredLocalDirs.get
    } else {
      // If we don't use the external shuffle service, local directories should be randomized if
      // not provided.
      configuredLocalDirs.getOrElse(s"$GENERATED_LOCAL_DIR_MOUNT_ROOT/${randomDirProvider()}")
    }
    val resolvedLocalDirs = resolvedLocalDirsSingleString.split(",")
    // It's worth noting that we always use an emptyDir volume for the directories on the driver,
    // because the driver does not need a hostPath to share its scratch space with any other pod.
    // The driver itself will decide on whether to use a hostPath volume or an emptyDir volume for
    // these directories on the executors. (see ExecutorPodFactory and
    // KubernetesExternalClusterManager)
    val localDirVolumes = resolvedLocalDirs.zipWithIndex.map { case (dir, index) =>
      new VolumeBuilder()
        .withName(s"spark-local-dir-$index-${Paths.get(dir).getFileName.toString}")
        .withNewEmptyDir().endEmptyDir()
        .build()
    }
    val localDirVolumeMounts = localDirVolumes.zip(resolvedLocalDirs).map {
      case (volume, path) =>
        new VolumeMountBuilder()
          .withName(volume.getName)
          .withMountPath(path)
          .build()
    }
    val resolvedDriverSparkConf = driverSpec.driverSparkConf.clone().set(
        "spark.local.dir", resolvedLocalDirsSingleString)
    driverSpec.copy(
      driverPod = new PodBuilder(driverSpec.driverPod)
        .editSpec()
          .addToVolumes(localDirVolumes: _*)
          .endSpec()
        .build(),
      driverContainer = new ContainerBuilder(driverSpec.driverContainer)
        .addToVolumeMounts(localDirVolumeMounts: _*)
        .build(),
      driverSparkConf = resolvedDriverSparkConf
    )
  }
}
