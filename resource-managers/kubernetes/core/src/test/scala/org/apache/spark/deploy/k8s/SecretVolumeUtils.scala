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

import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model.{Container, Pod}

private[spark] object SecretVolumeUtils {

  def podHasVolume(pod: Pod, volumeName: String): Boolean = {
    pod.getSpec.getVolumes.asScala.exists { volume =>
      volume.getName == volumeName
    }
  }

  def containerHasVolume(container: Container, volumeName: String, mountPath: String): Boolean = {
    container.getVolumeMounts.asScala.exists { volumeMount =>
      volumeMount.getName == volumeName && volumeMount.getMountPath == mountPath
    }
  }
}
