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

import io.fabric8.kubernetes.client.KubernetesClient

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.k8s._
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.resource.ResourceProfile

class KubernetesExecutorBuilderSuite extends PodBuilderSuite {

  override protected def templateFileConf: ConfigEntry[_] = {
    Config.KUBERNETES_EXECUTOR_PODTEMPLATE_FILE
  }

  override protected def buildPod(sparkConf: SparkConf, client: KubernetesClient): SparkPod = {
    sparkConf.set("spark.driver.host", "https://driver.host.com")
    val conf = KubernetesTestConf.createExecutorConf(sparkConf = sparkConf)
    val secMgr = new SecurityManager(sparkConf)
    val defaultProfile = ResourceProfile.getOrCreateDefaultProfile(sparkConf)
    new KubernetesExecutorBuilder().buildFromFeatures(conf, secMgr, client, defaultProfile).pod
  }

}
