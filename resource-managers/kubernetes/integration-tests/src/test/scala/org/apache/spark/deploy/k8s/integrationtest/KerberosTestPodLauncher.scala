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
package org.apache.spark.deploy.k8s.integrationtest

import java.io.{File, FileInputStream}
import java.lang.Boolean

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.builder.Predicate
import io.fabric8.kubernetes.api.model.{ContainerBuilder, KeyToPathBuilder}
import io.fabric8.kubernetes.api.model.extensions.{Deployment, DeploymentBuilder}
import io.fabric8.kubernetes.client.KubernetesClient

 /**
  * This class is responsible for launching a pod that runs spark-submit to simulate
  * the necessary global environmental variables and files expected for a Kerberos task.
  * In this test we specify HADOOP_CONF_DIR and ensure that for any arbitrary namespace
  * the krb5.conf, core-site.xml, and hdfs-site.xml are resolved accordingly.
  */
private[spark] class KerberosTestPodLauncher(
  kubernetesClient: KubernetesClient,
  namespace: String) {
   private val kerberosFiles = Seq("krb5.conf", "core-site.xml", "hdfs-site.xml")
   private val KRB_VOLUME = "krb5-conf"
   private val KRB_FILE_DIR = "/tmp"
   private val KRB_CONFIG_MAP_NAME = "krb-config-map"
   private val HADOOP_CONF_DIR_PATH = "/opt/spark/hconf"
   private val keyPaths = kerberosFiles.map(file =>
     new KeyToPathBuilder()
       .withKey(file)
       .withPath(file)
       .build()).toList
   def startKerberosTest(
     resource: String,
     className: String,
     appLabel: String,
     yamlLocation: String): Unit = {
     kubernetesClient.load(new FileInputStream(new File(yamlLocation)))
       .get().get(0) match {
       case deployment: Deployment =>
         val deploymentWithEnv: Deployment = new DeploymentBuilder(deployment)
         .editSpec()
           .editTemplate()
             .editSpec()
             .addNewVolume()
               .withName(KRB_VOLUME)
               .withNewConfigMap()
                 .withName(KRB_CONFIG_MAP_NAME)
                 .withItems(keyPaths.asJava)
                 .endConfigMap()
               .endVolume()
               .editMatchingContainer(new ContainerNameEqualityPredicate(
                 deployment.getMetadata.getName))
                 .addNewEnv()
                   .withName("NAMESPACE")
                   .withValue(namespace)
                   .endEnv()
                 .addNewEnv()
                   .withName("MASTER_URL")
                   .withValue(kubernetesClient.getMasterUrl.toString)
                 .endEnv()
                 .addNewEnv()
                   .withName("SUBMIT_RESOURCE")
                   .withValue(resource)
                  .endEnv()
                 .addNewEnv()
                   .withName("CLASS_NAME")
                   .withValue(className)
                   .endEnv()
                 .addNewEnv()
                   .withName("HADOOP_CONF_DIR")
                   .withValue(HADOOP_CONF_DIR_PATH)
                   .endEnv()
                 .addNewEnv()
                   .withName("APP_LOCATOR_LABEL")
                   .withValue(appLabel)
                   .endEnv()
                 .addNewEnv()
                   .withName("SPARK_PRINT_LAUNCH_COMMAND")
                   .withValue("true")
                   .endEnv()
                 .addNewEnv()
                   .withName("TMP_KRB_LOC")
                   .withValue(s"$KRB_FILE_DIR/${kerberosFiles.head}")
                   .endEnv()
                 .addNewEnv()
                   .withName("TMP_CORE_LOC")
                   .withValue(s"$KRB_FILE_DIR/${kerberosFiles(1)}")
                   .endEnv()
                 .addNewEnv()
                   .withName("TMP_HDFS_LOC")
                   .withValue(s"$KRB_FILE_DIR/${kerberosFiles(2)}")
                   .endEnv()
                 .addNewVolumeMount()
                   .withName(KRB_VOLUME)
                   .withMountPath(KRB_FILE_DIR)
                   .endVolumeMount()
                 .endContainer()
               .endSpec()
             .endTemplate()
           .endSpec()
         .build()
         kubernetesClient.extensions().deployments()
           .inNamespace(namespace).create(deploymentWithEnv)}
  }
}

private[spark] class ContainerNameEqualityPredicate(containerName: String)
  extends Predicate[ContainerBuilder] {
  override def apply(item: ContainerBuilder): Boolean = {
    item.getName == containerName
  }
}