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
package org.apache.spark.deploy.k8s.integrationtest.kerberos

import java.io.{File, FileInputStream}

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.api.model.extensions.{Deployment, DeploymentBuilder}
import io.fabric8.kubernetes.client.KubernetesClient
import org.apache.commons.io.FileUtils.readFileToString

import org.apache.spark.deploy.k8s.integrationtest.ContainerNameEqualityPredicate

 /**
  * This class is responsible for handling all Utils and Constants necessary for testing
  */
private[spark] class KerberosUtils(
  kubernetesClient: KubernetesClient,
  namespace: String) {
  def getClient: KubernetesClient = kubernetesClient
  def getNamespace: String = namespace
  def yamlLocation(loc: String): String = s"kerberos-yml/$loc.yml"
  def loadFromYaml(resource: String): FileInputStream =
    new FileInputStream(new File(yamlLocation(resource)))
  private val regex = "REPLACE_ME".r
  private val regexDP = "# default_ccache_name = MEMORY".r
  private val defaultCacheDP = "default_ccache_name = KRBCONF"
  private def locationResolver(loc: String) = s"test-data/hadoop-conf/$loc"
  private val kerberosFiles = Seq("krb5.conf", "core-site.xml", "hdfs-site.xml")
  private val kerberosConfTupList =
    kerberosFiles.map { file =>
      (file, regex.replaceAllIn(readFileToString(new File(locationResolver(file))), namespace))} ++
      Seq(("krb5-dp.conf", regexDP.replaceAllIn(regex.replaceAllIn(readFileToString(
        new File(locationResolver("krb5.conf"))), namespace), defaultCacheDP)))
  private val KRB_VOLUME = "krb5-conf"
  private val KRB_FILE_DIR = "/tmp"
  private val KRB_CONFIG_MAP_NAME = "krb-config-map"
  private val PV_LABELS = Map("job" -> "kerberostest")
  private val keyPaths: Seq[KeyToPath] = (kerberosFiles ++ Seq("krb5-dp.conf"))
    .map(file =>
    new KeyToPathBuilder()
      .withKey(file)
      .withPath(file)
      .build()).toList
  private def createPVTemplate(name: String, pathType: String) : PersistentVolume =
    new PersistentVolumeBuilder()
      .withNewMetadata()
        .withName(name)
        .withLabels(Map(
          "type" -> "local",
          "job" -> "kerberostest").asJava)
        .endMetadata()
      .withNewSpec()
        .withCapacity(Map("storage" -> new Quantity("1Gi")).asJava)
        .withAccessModes("ReadWriteOnce")
        .withHostPath(
          new HostPathVolumeSource(s"/tmp/$namespace/$pathType"))
        .endSpec()
      .build()
   private val pvNN = "nn-hadoop"
   private val pvKT = "server-keytab"
   private val persistentVolumeMap: Map[String, PersistentVolume] = Map(
     pvNN -> createPVTemplate(pvNN, "nn"),
     pvKT -> createPVTemplate(pvKT, "keytab"))
  private def buildKerberosPV(pvType: String) = {
    KerberosStorage(
      kubernetesClient.load(loadFromYaml(pvType))
        .get().get(0).asInstanceOf[PersistentVolumeClaim],
      persistentVolumeMap(pvType))
  }
  def getNNStorage: KerberosStorage = buildKerberosPV(pvNN)
  def getKTStorage: KerberosStorage = buildKerberosPV(pvKT)
  def getLabels: Map[String, String] = PV_LABELS
  def getKeyPaths: Seq[KeyToPath] = keyPaths
  def getConfigMap: ConfigMap = new ConfigMapBuilder()
    .withNewMetadata()
    .withName(KRB_CONFIG_MAP_NAME)
    .endMetadata()
    .addToData(kerberosConfTupList.toMap.asJava)
    .build()
  private val kdcNode = Seq("kerberos-deployment", "kerberos-service")
  private val nnNode = Seq("nn-deployment", "nn-service")
  private val dnNode = Seq("dn1-deployment", "dn1-service")
  private val dataPopulator = Seq("data-populator-deployment", "data-populator-service")
  private def buildKerberosDeployment(seqPair: Seq[String]) = {
    val deployment =
      kubernetesClient.load(loadFromYaml(seqPair.head)).get().get(0).asInstanceOf[Deployment]
    KerberosDeployment(
      new DeploymentBuilder(deployment)
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
                .withName("TMP_KRB_LOC")
                .withValue(s"$KRB_FILE_DIR/${kerberosFiles.head}")
                .endEnv()
              .addNewEnv()
                .withName("TMP_KRB_DP_LOC")
                .withValue(s"$KRB_FILE_DIR/krb5-dp.conf")
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
      .build(),
      kubernetesClient.load(loadFromYaml(seqPair(1))).get().get(0).asInstanceOf[Service]
    )
  }
  def getKDC: KerberosDeployment = buildKerberosDeployment(kdcNode)
  def getNN: KerberosDeployment = buildKerberosDeployment(nnNode)
  def getDN: KerberosDeployment = buildKerberosDeployment(dnNode)
  def getDP: KerberosDeployment = buildKerberosDeployment(dataPopulator)
}
