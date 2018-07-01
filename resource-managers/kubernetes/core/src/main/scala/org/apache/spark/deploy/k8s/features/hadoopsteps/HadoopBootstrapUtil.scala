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
package org.apache.spark.deploy.k8s.features.hadoopsteps

import io.fabric8.kubernetes.api.model.ContainerBuilder
import io.fabric8.kubernetes.api.model.PodBuilder

import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.SparkPod

private[spark] object HadoopBootstrapUtil {

   /**
    * Mounting the DT secret for both the Driver and the executors
    *
    * @param dtSecretName Name of the secret that stores the Delegation Token
    * @param dtSecretItemKey Name of the Item Key storing the Delegation Token
    * @param userName Name of the SparkUser to set SPARK_USER
    * @param pod Input pod to be appended to
    * @return a modified SparkPod
    */
  def bootstrapKerberosPod(
      dtSecretName: String,
      dtSecretItemKey: String,
      userName: String,
      pod: SparkPod) : SparkPod = {
      val kerberizedPod = new PodBuilder(pod.pod)
        .editOrNewSpec()
          .addNewVolume()
            .withName(SPARK_APP_HADOOP_SECRET_VOLUME_NAME)
            .withNewSecret()
              .withSecretName(dtSecretName)
              .endSecret()
            .endVolume()
          .endSpec()
        .build()
      val kerberizedContainer = new ContainerBuilder(pod.container)
        .addNewVolumeMount()
          .withName(SPARK_APP_HADOOP_SECRET_VOLUME_NAME)
          .withMountPath(SPARK_APP_HADOOP_CREDENTIALS_BASE_DIR)
          .endVolumeMount()
        .addNewEnv()
          .withName(ENV_HADOOP_TOKEN_FILE_LOCATION)
          .withValue(s"$SPARK_APP_HADOOP_CREDENTIALS_BASE_DIR/$dtSecretItemKey")
          .endEnv()
        .addNewEnv()
          .withName(ENV_SPARK_USER)
          .withValue(userName)
          .endEnv()
        .build()
    SparkPod(kerberizedPod, kerberizedContainer)
  }

   /**
    * setting ENV_SPARK_USER when HADOOP_FILES are detected
    *
    * @param sparkUserName Name of the SPARK_USER
    * @param pod Input pod to be appended to
    * @return a modified SparkPod
    */
  def bootstrapSparkUserPod(
     sparkUserName: String,
     pod: SparkPod) : SparkPod = {
     val envModifiedContainer = new ContainerBuilder(pod.container)
       .addNewEnv()
         .withName(ENV_SPARK_USER)
         .withValue(sparkUserName)
         .endEnv()
       .build()
    SparkPod(pod.pod, envModifiedContainer)
  }
}
