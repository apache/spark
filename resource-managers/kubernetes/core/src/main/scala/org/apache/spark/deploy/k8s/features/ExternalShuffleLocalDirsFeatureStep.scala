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
package org.apache.spark.deploy.k8s.features

import io.fabric8.kubernetes.api.model.{Volume, VolumeBuilder, VolumeMount, VolumeMountBuilder}
import org.apache.commons.io.FilenameUtils

import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesRoleSpecificConf}
import org.apache.spark.util.Utils

class ExternalShuffleLocalDirsFeatureStep(
    conf: KubernetesConf[_ <: KubernetesRoleSpecificConf]) extends LocalDirsFeatureStep(conf) {

  private val resolvedLocalDirs = Utils.getConfiguredLocalDirs(conf.sparkConf)

  override def getDirVolumesWithMounts(): Seq[(Volume, VolumeMount)] = {
    // TODO: Using hostPath for the local directory will also make it such that the
    // other uses of the local directory - broadcasting and caching - will also write
    // to the directory that the shuffle service is aware of. It would be better for
    // these directories to be separate so that the lifetime of the non-shuffle scratch
    // space is tied to an emptyDir instead of the hostPath. This requires a change in
    // core Spark as well.
    resolvedLocalDirs.zipWithIndex.map {
      case (shuffleDir, shuffleDirIndex) =>
        val volumeName = s"$shuffleDirIndex-${FilenameUtils.getBaseName(shuffleDir)}"
        val volume = new VolumeBuilder()
          .withName(volumeName)
          .withNewHostPath(shuffleDir)
          .build()
        val volumeMount = new VolumeMountBuilder()
          .withName(volumeName)
          .withMountPath(shuffleDir)
          .build()
        (volume, volumeMount)
    }
  }
}
