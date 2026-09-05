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

import java.io.File
import java.nio.file.Files

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s._
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.util.Utils

class KerberosConfExecutorFeatureStepSuite extends SparkFunSuite {
  import SecretVolumeUtils._

  test("SPARK-50758: mount krb5 ConfigMap when KRB_CONFIG_MAP_NAME is set") {
    val executorSparkConf = new SparkConf(false).set(KRB_CONFIG_MAP_NAME, "testCM")
    val executorConf = KubernetesTestConf.createExecutorConf(sparkConf = executorSparkConf)
    val initial = SparkPod.initialPod()
    val executorPod = new KerberosConfExecutorFeatureStep(executorConf).configurePod(initial)
    checkPod(executorPod, hasKrb5 = true)
  }

  test("SPARK-50758: no-op when KRB_CONFIG_MAP_NAME is not set") {
    val executorConf = KubernetesTestConf.createExecutorConf(sparkConf = new SparkConf(false))
    val initial = SparkPod.initialPod()
    val executorPod = new KerberosConfExecutorFeatureStep(executorConf).configurePod(initial)
    checkPod(executorPod, hasKrb5 = false)
  }

  test("SPARK-50758: mount krb5 ConfigMap when driver step publishes its name") {
    val tmpDir = Utils.createTempDir()
    val krbConf = File.createTempFile("krb5", ".conf", tmpDir)
    Files.writeString(krbConf.toPath, "some data")

    Seq(
      // (sparkConf, expectMount)
      (new SparkConf(false).set(KUBERNETES_KERBEROS_KRB5_CONFIG_MAP, "userCM"), true),
      (new SparkConf(false).set(KUBERNETES_KERBEROS_KRB5_FILE, krbConf.getAbsolutePath), true),
      (new SparkConf(false), false)
    ).foreach { case (driverSparkConf, expectMount) =>

      val driverConf = KubernetesTestConf.createDriverConf(sparkConf = driverSparkConf)
      val driverStep = new KerberosConfDriverFeatureStep(driverConf)

      val executorSparkConf = new SparkConf(false)
      val additionalProps = driverStep.getAdditionalPodSystemProperties()
      if (expectMount) {
        assert(additionalProps.contains(KRB_CONFIG_MAP_NAME),
          s"Driver step must publish $KRB_CONFIG_MAP_NAME when krb5 conf is provided")
        additionalProps.foreach { case (k, v) => executorSparkConf.set(k, v) }
      } else {
        assert(!additionalProps.contains(KRB_CONFIG_MAP_NAME))
      }

      val executorConf = KubernetesTestConf.createExecutorConf(sparkConf = executorSparkConf)
      val initial = SparkPod.initialPod()
      val executorPod = new KerberosConfExecutorFeatureStep(executorConf).configurePod(initial)
      checkPod(executorPod, expectMount)
    }
  }

  private def checkPod(pod: SparkPod, hasKrb5: Boolean): Unit = {
    val mountPath = KRB_FILE_DIR_PATH + "/krb5.conf"
    if (hasKrb5) {
      assert(podHasVolume(pod.pod, KRB_FILE_VOLUME))
      assert(containerHasVolume(pod.container, KRB_FILE_VOLUME, mountPath))
    } else {
      assert(!podHasVolume(pod.pod, KRB_FILE_VOLUME))
      assert(!containerHasVolume(pod.container, KRB_FILE_VOLUME, mountPath))
    }
  }
}
