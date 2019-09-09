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
import org.apache.spark.deploy.k8s.{KubernetesTestConf, SparkPod}
import org.apache.spark.deploy.k8s.Config.KUBERNETES_KERBEROS_KRB5_FILE
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.SecretVolumeUtils._
import org.apache.spark.util.Utils

class KerberosConfExecutorFeatureStepSuite extends SparkFunSuite {

  test("executor pod should mount with krb5 if driver pod is configured") {
    val tmpDir = Utils.createTempDir()
    val krbConf = File.createTempFile("krb5", ".conf", tmpDir)
    Files.write("some data", krbConf, UTF_8)
    val conf = new SparkConf().set(KUBERNETES_KERBEROS_KRB5_FILE, krbConf.getAbsolutePath)
    val executorConf = KubernetesTestConf.createExecutorConf(conf)
    val step = new KerberosConfExecutorFeatureStep(executorConf)
    val pod = SparkPod.initialPod()
    val newPod = step.configurePod(pod)
    assert(podHasVolume(newPod.pod, KRB_FILE_VOLUME))
    assert(containerHasVolume(newPod.container, KRB_FILE_VOLUME, KRB_FILE_FULL_NAME))
  }
}
