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

import io.fabric8.kubernetes.api.model.{VolumeBuilder, VolumeMountBuilder}
import org.mockito.Mockito.{verify, verifyNoMoreInteractions, when}
import org.scalatest.mock.MockitoSugar.mock

import org.apache.spark.{SparkConf, SparkFunSuite}

class ExecutorLocalDirVolumeProviderSuite extends SparkFunSuite {

  test("Delegates to the external shuffle manager implementation if present.") {
    val externalShuffleManager = mock[KubernetesExternalShuffleManager]
    val mockVolume = new VolumeBuilder()
      .withName("local-dir")
      .withNewHostPath("/tmp/spark-local-dirs")
      .build()
    val mockVolumeMount = new VolumeMountBuilder()
      .withName("local-dir")
      .withMountPath("/tmp/spark-local-dirs-mount")
      .build()
    when(externalShuffleManager.getExecutorShuffleDirVolumesWithMounts)
        .thenReturn(Seq((mockVolume, mockVolumeMount)))
    val volumeProvider = new ExecutorLocalDirVolumeProviderImpl(
        new SparkConf(false), Some(externalShuffleManager))
    assert(volumeProvider.getExecutorLocalDirVolumesWithMounts ===
        Seq((mockVolume, mockVolumeMount)))
    verify(externalShuffleManager).getExecutorShuffleDirVolumesWithMounts
    verifyNoMoreInteractions(externalShuffleManager)
  }

  test("Provides EmptyDir volumes for each local dir in spark.local.dirs.") {
    val localDirs = Seq("/tmp/test-local-dir-1", "/tmp/test-local-dir-2")
    val sparkConf = new SparkConf(false).set("spark.local.dir", localDirs.mkString(","))
    val volumeProvider = new ExecutorLocalDirVolumeProviderImpl(sparkConf, None)
    val localDirVolumesWithMounts = volumeProvider.getExecutorLocalDirVolumesWithMounts
    assert(localDirVolumesWithMounts.size === 2)
    localDirVolumesWithMounts.zip(localDirs).zipWithIndex.foreach {
      case (((localDirVolume, localDirVolumeMount), expectedDirMountPath), index) =>
        val dirName = expectedDirMountPath.substring(
            expectedDirMountPath.lastIndexOf('/') + 1, expectedDirMountPath.length)
        assert(localDirVolume.getName === s"spark-local-dir-$index-$dirName")
        assert(localDirVolume.getEmptyDir != null)
        assert(localDirVolumeMount.getName === localDirVolume.getName)
        assert(localDirVolumeMount.getMountPath === expectedDirMountPath)
      case unknown => throw new IllegalArgumentException("Unexpected object: $unknown")
    }
  }
}
