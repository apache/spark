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

import java.nio.file.Paths

import io.fabric8.kubernetes.api.model.{Volume, VolumeBuilder, VolumeMount, VolumeMountBuilder}

import org.apache.spark.SparkConf
import org.apache.spark.util.Utils

private[spark] trait ExecutorLocalDirVolumeProvider {
  def getExecutorLocalDirVolumesWithMounts: Seq[(Volume, VolumeMount)]
}

private[spark] class ExecutorLocalDirVolumeProviderImpl(
    sparkConf: SparkConf,
    kubernetesExternalShuffleManager: Option[KubernetesExternalShuffleManager])
    extends ExecutorLocalDirVolumeProvider {
  override def getExecutorLocalDirVolumesWithMounts: Seq[(Volume, VolumeMount)] = {
    kubernetesExternalShuffleManager.map(_.getExecutorShuffleDirVolumesWithMounts)
        .getOrElse {
          // If we're not using the external shuffle manager, we should use emptyDir volumes for
          // shuffle directories since it's important for disk I/O for these directories to be
          // performant. If the user has not provided a local directory, instead of using the
          // Java temporary directory, we create one instead, because we want to avoid
          // mounting an emptyDir which overlaps with an existing path in the Docker image.
          // Java's temporary directory path is typically /tmp or a similar path, which is
          // likely to exist in most images.
          val resolvedLocalDirs = Utils.getConfiguredLocalDirs(sparkConf)
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
          localDirVolumes.zip(localDirVolumeMounts)
        }
  }
}
