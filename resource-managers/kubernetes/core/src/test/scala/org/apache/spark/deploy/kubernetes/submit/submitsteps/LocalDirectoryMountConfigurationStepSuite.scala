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
package org.apache.spark.deploy.kubernetes.submit.submitsteps

import java.nio.file.Paths

import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.constants._
import org.apache.spark.deploy.k8s.submit.submitsteps.{KubernetesDriverSpec, LocalDirectoryMountConfigurationStep}

private[spark] class LocalDirectoryMountConfigurationStepSuite extends SparkFunSuite {

  test("When using the external shuffle service, the local directories must be provided.") {
    val sparkConf = new SparkConf(false)
        .set(org.apache.spark.internal.config.SHUFFLE_SERVICE_ENABLED, true)
    val configurationStep = new LocalDirectoryMountConfigurationStep(sparkConf)
    try {
      configurationStep.configureDriver(KubernetesDriverSpec.initialSpec(sparkConf))
      fail("The configuration step should have failed without local dirs.")
    } catch {
      case e: Throwable =>
        assert(e.getMessage === "requirement failed: spark.local.dir must be provided explicitly" +
          " when using the external shuffle service in Kubernetes. These directories should map" +
          " to the paths that are mounted into the external shuffle service pods.")
    }
  }

  test("When not using the external shuffle service, a random directory should be set" +
      " for local dirs if one is not provided.") {
    val sparkConf = new SparkConf(false)
        .set(org.apache.spark.internal.config.SHUFFLE_SERVICE_ENABLED, false)
    val configurationStep = new LocalDirectoryMountConfigurationStep(
        sparkConf, () => "local-dir")
    val resolvedDriverSpec = configurationStep.configureDriver(
        KubernetesDriverSpec.initialSpec(sparkConf))
    testLocalDirsMatch(resolvedDriverSpec, Seq(s"$GENERATED_LOCAL_DIR_MOUNT_ROOT/local-dir"))
  }

  test("When not using the external shuffle service, provided local dirs should be mounted as" +
      "  emptyDirs.") {
    val sparkConf = new SparkConf(false)
        .set(org.apache.spark.internal.config.SHUFFLE_SERVICE_ENABLED, false)
        .set("spark.local.dir", "/mnt/tmp/spark-local,/var/tmp/spark-local")
    val configurationStep = new LocalDirectoryMountConfigurationStep(
        sparkConf)
    val resolvedDriverSpec = configurationStep.configureDriver(
        KubernetesDriverSpec.initialSpec(sparkConf))
    testLocalDirsMatch(resolvedDriverSpec, Seq("/mnt/tmp/spark-local", "/var/tmp/spark-local"))
  }

  private def testLocalDirsMatch(
      resolvedDriverSpec: KubernetesDriverSpec, expectedLocalDirs: Seq[String]): Unit = {
    assert(resolvedDriverSpec.driverSparkConf.get("spark.local.dir").split(",") ===
        expectedLocalDirs)
    expectedLocalDirs
        .zip(resolvedDriverSpec.driverPod.getSpec.getVolumes.asScala)
        .zipWithIndex
        .foreach {
      case ((dir, volume), index) =>
        assert(volume.getEmptyDir != null)
        val fileName = Paths.get(dir).getFileName.toString
        assert(volume.getName === s"spark-local-dir-$index-$fileName")
    }

    expectedLocalDirs
      .zip(resolvedDriverSpec.driverContainer.getVolumeMounts.asScala)
      .zipWithIndex
      .foreach {
        case ((dir, volumeMount), index) =>
          val fileName = Paths.get(dir).getFileName.toString
          assert(volumeMount.getName === s"spark-local-dir-$index-$fileName")
          assert(volumeMount.getMountPath === dir)
      }
  }
}
