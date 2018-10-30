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
import io.fabric8.kubernetes.api.model.apps.{StatefulSet, StatefulSetBuilder}
import io.fabric8.kubernetes.api.model.batch.{Job, JobBuilder}
import io.fabric8.kubernetes.client.KubernetesClient
import org.apache.commons.io.FileUtils.readFileToString

import org.apache.spark.deploy.k8s.integrationtest.KubernetesSuite.KERBEROS_LABEL

/**
 * This class is responsible for handling all Utils and Constants necessary for testing
 */
private[spark] class KerberosUtils(
    sparkImage: String,
    hadoopImage: String,
    kerberosImage: String,
    serviceAccountName: String,
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
  private val KRB_FILE_DIR = "/mnt"
  private val KRB_CONFIG_MAP_NAME = "krb-config-map"
  private val keyPaths: Seq[KeyToPath] = (kerberosFiles ++ Seq("krb5-dp.conf"))
    .map { file =>
      new KeyToPathBuilder()
        .withKey(file)
        .withPath(file)
        .build()
    }.toList
  private def createPVTemplate(name: String, pathType: String) : PersistentVolume =
    new PersistentVolumeBuilder()
      .withNewMetadata()
        .withName(name)
        .withLabels(Map(
          "type" -> "local",
          "job" -> "kerberostest").asJava)
        .endMetadata()
      .withNewSpec()
        .withStorageClassName(name)
        .withCapacity(Map("storage" -> new Quantity("200Mi")).asJava)
        .withAccessModes("ReadWriteMany")
        .withHostPath(new HostPathVolumeSource(s"$KRB_FILE_DIR/$namespace/$pathType", ""))
        .endSpec()
      .build()
  private def createPVCTemplate(name: String) : PersistentVolumeClaim =
    new PersistentVolumeClaimBuilder()
      .withNewMetadata()
      .withName(name)
      .withLabels(Map(
        "job" -> "kerberostest").asJava)
      .endMetadata()
      .withNewSpec()
        .withStorageClassName(name)
          .withVolumeName(name)
          .withAccessModes("ReadWriteMany")
          .withNewResources()
            .withRequests(Map("storage" -> new Quantity("200Mi")).asJava)
          .endResources()
        .endSpec()
      .build()
  private val pvNN = "nn-hadoop"
  private val pvKT = "server-keytab"
  private val persistentVolumeMap: Map[String, PersistentVolume] = Map(
    pvNN -> createPVTemplate(pvNN, "nn"),
    pvKT -> createPVTemplate(pvKT, "keytab"))

  private def buildKerberosPV(pvType: String) = {
    PVStorage(pvType, createPVCTemplate(pvType), persistentVolumeMap(pvType))
  }

  def getNNStorage: PVStorage = buildKerberosPV(pvNN)
  def getKTStorage: PVStorage = buildKerberosPV(pvKT)
  def getLabels: Map[String, String] = KERBEROS_LABEL
  def getKeyPaths: Seq[KeyToPath] = keyPaths
  def getConfigMap: ConfigMapStorage =
    ConfigMapStorage(
      new ConfigMapBuilder()
        .withNewMetadata()
          .withName(KRB_CONFIG_MAP_NAME)
          .endMetadata()
        .addToData(kerberosConfTupList.toMap.asJava)
        .build()
    )
  private val kdcNode = Seq("kerberos-set", "kerberos-service")
  private val nnNode = Seq("nn-set", "nn-service")
  private val dnNode = Seq("dn1-set", "dn1-service")
  private val dataPopulator = "data-populator-job"
  private val hadoopContainerEnvs = Seq(
    new EnvVarBuilder()
      .withName("NAMESPACE")
      .withValue(namespace)
      .build(),
    new EnvVarBuilder()
      .withName("TMP_KRB_LOC")
      .withValue(s"$KRB_FILE_DIR/${kerberosFiles.head}")
      .build(),
    new EnvVarBuilder()
      .withName("TMP_KRB_DP_LOC")
      .withValue(s"$KRB_FILE_DIR/krb5-dp.conf")
      .build(),
    new EnvVarBuilder()
      .withName("TMP_CORE_LOC")
      .withValue(s"$KRB_FILE_DIR/${kerberosFiles(1)}")
      .build(),
    new EnvVarBuilder()
      .withName("TMP_HDFS_LOC")
      .withValue(s"$KRB_FILE_DIR/${kerberosFiles(2)}")
      .build()
  ).asJava
  private val krbVolume =
    new VolumeBuilder()
      .withName(KRB_VOLUME)
      .withNewConfigMap()
        .withName(KRB_CONFIG_MAP_NAME)
        .withItems(keyPaths.asJava)
        .endConfigMap()
      .build()
  private val krbVolumeMount =
    new VolumeMountBuilder()
      .withName(KRB_VOLUME)
      .withMountPath(KRB_FILE_DIR)
      .build()

  private def buildHadoopClusterDeployment(name: String, seqPair: Seq[String]) = {
    val statefulSet =
      kubernetesClient.load(loadFromYaml(seqPair.head)).get().get(0).asInstanceOf[StatefulSet]
    ServiceStorage(
      name,
      new StatefulSetBuilder(statefulSet)
        .editSpec()
          .editTemplate()
            .editSpec()
              .addNewVolumeLike(krbVolume).endVolume()
            .editMatchingContainer(new ContainerNameEqualityPredicate(
              statefulSet.getMetadata.getName))
              .addAllToEnv(hadoopContainerEnvs)
              .addNewVolumeMountLike(krbVolumeMount).endVolumeMount()
              .withImage(hadoopImage)
              .endContainer()
            .endSpec()
          .endTemplate()
        .endSpec()
      .build(),
      kubernetesClient.load(loadFromYaml(seqPair(1))).get().get(0).asInstanceOf[Service] )
  }
  private def buildDP(yamlLocation: String): JobStorage = {
    val job = kubernetesClient.load(loadFromYaml(yamlLocation)).get().get(0).asInstanceOf[Job]
    JobStorage(
      new JobBuilder(job)
        .editSpec()
            .editTemplate()
              .editSpec()
                .addNewVolumeLike(krbVolume).endVolume()
              .editMatchingContainer(new ContainerNameEqualityPredicate(
                job.getMetadata.getName))
                .addAllToEnv(hadoopContainerEnvs)
                .addNewVolumeMountLike(krbVolumeMount).endVolumeMount()
                .withImage(hadoopImage)
                .endContainer()
              .endSpec()
            .endTemplate()
          .endSpec()
        .build()
    )
  }
  def getKDC: ServiceStorage = buildHadoopClusterDeployment("kerberos", kdcNode)
  def getNN: ServiceStorage = buildHadoopClusterDeployment("nn", nnNode)
  def getDN: ServiceStorage = buildHadoopClusterDeployment("dn1", dnNode)
  def getDP: JobStorage = buildDP(dataPopulator)
  private val HADOOP_CONF_DIR_PATH = "/opt/spark/hconf"
  private val krb5TestkeyPaths =
    kerberosFiles.map { file =>
    new KeyToPathBuilder()
      .withKey(file)
      .withPath(file)
      .build()
    }.toList

  def getKerberosTest(
    resource: String,
    className: String,
    appLabel: String,
    yamlLocation: String): JobStorage = {
    val job = kubernetesClient.load(loadFromYaml(yamlLocation)).get().get(0).asInstanceOf[Job]
    JobStorage(
      new JobBuilder(job)
        .editSpec()
          .editTemplate()
            .editOrNewMetadata()
              .addToLabels(Map("name" -> "kerberos-test").asJava)
              .endMetadata()
            .editSpec()
              .withServiceAccountName(serviceAccountName)
              .addNewVolume()
              .withName(KRB_VOLUME)
              .withNewConfigMap()
                .withName(KRB_CONFIG_MAP_NAME)
                .withItems(krb5TestkeyPaths.asJava)
                .endConfigMap()
              .endVolume()
              .editMatchingContainer(new ContainerNameEqualityPredicate(
                job.getMetadata.getName))
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
                .addNewEnv()
                  .withName("BASE_SPARK_IMAGE")
                  .withValue(sparkImage)
                  .endEnv()
                .addNewVolumeMount()
                  .withName(KRB_VOLUME)
                  .withMountPath(KRB_FILE_DIR)
                  .endVolumeMount()
                .withImage(kerberosImage)
              .endContainer()
              .endSpec()
            .endTemplate()
          .endSpec()
        .build())
  }
}
