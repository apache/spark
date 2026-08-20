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

package org.apache.spark.deploy.k8s.submit

import java.io.{File, StringWriter}
import java.nio.charset.MalformedInputException
import java.util.{List => JList, Map => JMap}
import java.util.Properties

import scala.collection.mutable
import scala.io.{Codec, Source}
import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model.{ConfigMap, ConfigMapBuilder, KeyToPath}

import org.apache.spark.SparkConf
import org.apache.spark.annotation.{DeveloperApi, Since, Stable}
import org.apache.spark.deploy.k8s.{Config, Constants, KubernetesUtils}
import org.apache.spark.deploy.k8s.Config.{KUBERNETES_DNS_SUBDOMAIN_NAME_MAX_LENGTH, KUBERNETES_NAMESPACE}
import org.apache.spark.deploy.k8s.Constants.ENV_SPARK_CONF_DIR
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{PATH, PATHS}
import org.apache.spark.util.ArrayImplicits._

/**
 * :: DeveloperApi ::
 *
 * A utility class used for K8s operations internally and Spark K8s operator.
 */
@Stable
@DeveloperApi
@Since("3.1.0")
object KubernetesClientUtils extends Logging {

  // Config map name can be KUBERNETES_DNS_SUBDOMAIN_NAME_MAX_LENGTH chars at max.
  @Since("3.3.0")
  def configMapName(prefix: String): String = {
    val suffix = "-conf-map"
    s"${prefix.take(KUBERNETES_DNS_SUBDOMAIN_NAME_MAX_LENGTH - suffix.length)}$suffix"
  }

  @Since("3.1.0")
  val configMapNameExecutor: String = configMapName(s"spark-exec-${KubernetesUtils.uniqueID()}")

  @Since("3.1.0")
  val configMapNameDriver: String = configMapName(s"spark-drv-${KubernetesUtils.uniqueID()}")

  private def buildStringFromPropertiesMap(configMapName: String,
      propertiesMap: Map[String, String]): String = {
    val properties = new Properties()
    propertiesMap.foreach { case (k, v) =>
      properties.setProperty(k, v)
    }
    val propertiesWriter = new StringWriter()
    properties.store(propertiesWriter,
      s"Java properties built from Kubernetes config map with name: $configMapName")
    propertiesWriter.toString
  }

  /**
   * Build, file -> 'file's content' map of all the selected files in SPARK_CONF_DIR.
   * (Java-friendly)
   */
  @Since("4.1.0")
  def buildSparkConfDirFilesMapJava(
      configMapName: String,
      sparkConf: SparkConf,
      resolvedPropertiesMap: JMap[String, String]): JMap[String, String] = synchronized {
    buildSparkConfDirFilesMap(configMapName, sparkConf, resolvedPropertiesMap.asScala.toMap).asJava
  }

  /**
   * Build, file -> 'file's content' map of all the selected files in SPARK_CONF_DIR.
   */
  @Since("3.1.1")
  def buildSparkConfDirFilesMap(
      configMapName: String,
      sparkConf: SparkConf,
      resolvedPropertiesMap: Map[String, String]): Map[String, String] = synchronized {
    val loadedConfFilesMap = KubernetesClientUtils.loadSparkConfDirFiles(sparkConf)
    // Add resolved spark conf to the loaded configuration files map.
    if (resolvedPropertiesMap.nonEmpty) {
      val resolvedProperties: String = KubernetesClientUtils
        .buildStringFromPropertiesMap(configMapName, resolvedPropertiesMap)
      loadedConfFilesMap ++ Map(Constants.SPARK_CONF_FILE_NAME -> resolvedProperties)
    } else {
      loadedConfFilesMap
    }
  }

  @Since("4.1.0")
  def buildKeyToPathObjectsJava(confFilesMap: JMap[String, String]): JList[KeyToPath] = {
    buildKeyToPathObjects(confFilesMap.asScala.toMap).asJava
  }

  @Since("3.1.0")
  def buildKeyToPathObjects(confFilesMap: Map[String, String]): Seq[KeyToPath] = {
    confFilesMap.map {
      case (fileName: String, _: String) =>
        val filePermissionMode = 420  // 420 is decimal for octal literal 0644.
        new KeyToPath(fileName, filePermissionMode, fileName)
    }.toList.sortBy(x => x.getKey) // List is sorted to make mocking based tests work
  }

  /**
   * Build a ConfigMap that will hold the content for environment variable SPARK_CONF_DIR
   * on remote pods. (Java-friendly)
   */
  @Since("4.1.0")
  def buildConfigMapJava(configMapName: String, confFileMap: JMap[String, String],
      withLabels: JMap[String, String]): ConfigMap = {
    buildConfigMap(configMapName, confFileMap.asScala.toMap, withLabels.asScala.toMap)
  }

  /**
   * Build a Config Map that will hold the content for environment variable SPARK_CONF_DIR
   * on remote pods.
   */
  @Since("3.1.0")
  def buildConfigMap(configMapName: String, confFileMap: Map[String, String],
      withLabels: Map[String, String] = Map()): ConfigMap = {
    val configMapNameSpace =
      confFileMap.getOrElse(KUBERNETES_NAMESPACE.key, KUBERNETES_NAMESPACE.defaultValueString)
    new ConfigMapBuilder()
      .withNewMetadata()
        .withName(configMapName)
        .withNamespace(configMapNameSpace)
        .withLabels(withLabels.asJava)
        .endMetadata()
      .withImmutable(true)
      .addToData(confFileMap.asJava)
      .build()
  }

  /**
   * Validate the total size of a config map's data against
   * `spark.kubernetes.configMap.maxSize`.
   *
   * Kubernetes rejects ConfigMaps whose serialized size exceeds a server-side limit
   * (by default ~1MiB, see https://etcd.io/docs/v3.4.0/dev-guide/limit/). Rather than
   * silently dropping configuration files to stay under that limit -- which can leave
   * an application running with missing or unexpected configuration -- callers should
   * invoke this method before building the config map so that Spark fails fast with a
   * clear error instead.
   */
  @Since("5.0.0")
  def validateConfigMapSize(confFileMap: Map[String, String], sparkConf: SparkConf): Unit = {
    val maxSize = sparkConf.get(Config.CONFIG_MAP_MAXSIZE)
    val mapSize = confFileMap.map { case (k, v) => k.length + v.length }.sum
    if (mapSize > maxSize) {
      throw new IllegalArgumentException(
        s"Data for config map with size $mapSize bytes exceeds the maximum config map " +
          s"size of $maxSize bytes. Please see config: `${Config.CONFIG_MAP_MAXSIZE.key}` " +
          "for more details.")
    }
  }

  // exposed for testing
  private[submit] def loadSparkConfDirFiles(conf: SparkConf): Map[String, String] = {
    val confDir = Option(conf.getenv(ENV_SPARK_CONF_DIR)).orElse(
      conf.getOption("spark.home").map(dir => s"$dir/conf"))
    if (confDir.isDefined) {
      val confFiles: Seq[File] = listConfFiles(confDir.get)
      val confFileMap = mutable.HashMap[String, String]()
      var source: Source = Source.fromString("") // init with empty source.
      for (file <- confFiles) {
        try {
          source = Source.fromFile(file)(Codec.UTF8)
          val (fileName, fileContent) = file.getName -> source.mkString
          confFileMap.put(fileName, fileContent)
        } catch {
          case e: MalformedInputException =>
            logWarning(log"Unable to read a non UTF-8 encoded file " +
              log"${MDC(PATH, file.getAbsolutePath)}. Skipping...", e)
        } finally {
          source.close()
        }
      }
      if (confFileMap.nonEmpty) {
        logInfo(log"Spark configuration files loaded from ${MDC(PATH, confDir)} : " +
          log"${MDC(PATHS, confFileMap.keys.mkString(","))}")
      }
      confFileMap.toMap
    } else {
      Map.empty[String, String]
    }
  }

  private def listConfFiles(confDir: String): Seq[File] = {
    // At the moment configmaps do not support storing binary content
    // (i.e. skip jar,tar,gzip,zip).
    def testIfBinary(f: File): Boolean = f.getName.matches(".*\\.(gz|zip|jar|tar)")

    // We exclude all the template files and user provided spark conf or properties,
    // Spark properties are resolved in a different step.
    def testIfSparkConfOrTemplates(f: File) = f.getName.matches(".*\\.template") ||
      f.getName.matches("spark.*(conf|properties)")

    val fileFilter = (f: File) => {
      f.isFile && !testIfBinary(f) && !testIfSparkConfOrTemplates(f)
    }
    val confFiles: Seq[File] = {
      val dir = new File(confDir)
      if (dir.isDirectory) {
        dir.listFiles.filter(x => fileFilter(x)).toImmutableArraySeq
      } else {
        Nil
      }
    }
    confFiles
  }
}
