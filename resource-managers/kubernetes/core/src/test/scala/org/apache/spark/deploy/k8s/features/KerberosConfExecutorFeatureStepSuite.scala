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
import java.nio.charset.StandardCharsets.UTF_8

import com.google.common.io.Files

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s._
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.util.Utils

class KerberosConfExecutorFeatureStepSuite extends SparkFunSuite  {
  import SecretVolumeUtils._

  test("SPARK-50758: mounts the krb5 config map on the executor pod") {
    val tmpDir = Utils.createTempDir()
    val krbConf = File.createTempFile("krb5", ".conf", tmpDir)
    Files.write("some data", krbConf, UTF_8)

    Seq(
      new SparkConf(false)
        .set(KUBERNETES_KERBEROS_KRB5_CONFIG_MAP, "testConfigMap"),
      new SparkConf(false)
        .set(KUBERNETES_KERBEROS_KRB5_FILE, krbConf.getAbsolutePath()),
      new SparkConf(false)).foreach { sparkConf =>

      val driverConf = KubernetesTestConf.createDriverConf(sparkConf = sparkConf)
      val driverStep = new KerberosConfDriverFeatureStep(driverConf)

      val additionalPodSystemProperties = driverStep.getAdditionalPodSystemProperties()
      val executorSparkConf = new SparkConf(false)
      if (hasKerberosConf(driverConf)) {
        assert(additionalPodSystemProperties.contains(Constants.KRB_CONFIG_MAP_NAME))
        additionalPodSystemProperties.foreach { case (key, value) =>
          executorSparkConf.set(key, value)
        }
      } else {
        assert(additionalPodSystemProperties.isEmpty)
      }

      val executorConf = KubernetesTestConf.createExecutorConf(sparkConf = executorSparkConf)
      val executorStep = new KerberosConfExecutorFeatureStep(executorConf)
      val executorPod = executorStep.configurePod(SparkPod.initialPod())

      checkPod(executorPod, hasKerberosConf(driverConf))
    }
  }

  private def hasKerberosConf(conf: KubernetesConf): Boolean = {
    val krb5File = conf.get(KUBERNETES_KERBEROS_KRB5_FILE)
    val krb5CMap = conf.get(KUBERNETES_KERBEROS_KRB5_CONFIG_MAP)
    krb5CMap.isDefined | krb5File.isDefined
  }

  private def checkPod(pod: SparkPod, hasKerberosConf: Boolean): Unit = {
    if (hasKerberosConf) {
      assert(podHasVolume(pod.pod, KRB_FILE_VOLUME))
      assert(containerHasVolume(pod.container, KRB_FILE_VOLUME, KRB_FILE_DIR_PATH + "/krb5.conf"))
    } else {
      assert(!podHasVolume(pod.pod, KRB_FILE_VOLUME))
      assert(!containerHasVolume(pod.container, KRB_FILE_VOLUME, KRB_FILE_DIR_PATH + "/krb5.conf"))
    }
  }

}
