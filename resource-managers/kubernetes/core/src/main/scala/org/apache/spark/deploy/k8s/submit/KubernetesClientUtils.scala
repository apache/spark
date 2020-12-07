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
import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.immutable.SortedSet
import scala.collection.mutable
import scala.io.{Codec, Source}

import io.fabric8.kubernetes.api.model.{ConfigMap, ConfigMapBuilder, KeyToPath}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.k8s.{Config, Constants, KubernetesUtils}
import org.apache.spark.deploy.k8s.Constants.ENV_SPARK_CONF_DIR
import org.apache.spark.internal.Logging

private[spark] object KubernetesClientUtils extends Logging {

  // Config map name can be 63 chars at max.
  def configMapName(prefix: String): String = s"${prefix.take(54)}-conf-map"

  val configMapNameExecutor: String = configMapName(s"spark-exec-${KubernetesUtils.uniqueID()}")

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

  object StringLengthOrdering extends Ordering[(String, String)] {
    override def compare(x: (String, String), y: (String, String)): Int = {
      // compare based on file length and break the tie with string comparison of keys
      // (i.e. file names).
      (x._1.length + x._2.length).compare(y._1.length + y._2.length) * 10 +
        x._1.compareTo(y._1)
    }
  }

  def truncateToSize(seq: Seq[(String, String)], maxSize: Long): Map[String, String] = {
    // First order the entries in order of their size.
    // Skip entries if the resulting Map size exceeds maxSize.
    val ordering: Ordering[(String, String)] = StringLengthOrdering
    val sortedSet = SortedSet[(String, String)](seq: _*)(ordering)
    var i: Int = 0
    val map = mutable.HashMap[String, String]()
    for (item <- sortedSet) {
      i += item._1.length + item._2.length
      if (i < maxSize) {
        map.put(item._1, item._2)
      }
    }
    map.toMap
  }
  /**
   * Build, file -> 'file's content' map of all the selected files in SPARK_CONF_DIR.
   */
  def buildSparkConfDirFilesMap(configMapName: String,
      sparkConf: SparkConf, resolvedPropertiesMap: Map[String, String]): Map[String, String] = {
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

  def buildKeyToPathObjects(confFilesMap: Map[String, String]): Seq[KeyToPath] = {
    confFilesMap.map {
      case (fileName: String, _: String) =>
        val filePermissionMode = 420  // 420 is decimal for octal literal 0644.
        new KeyToPath(fileName, filePermissionMode, fileName)
    }.toList.sortBy(x => x.getKey) // List is sorted to make mocking based tests work
  }

  /**
   * Build a Config Map that will hold the content for environment variable SPARK_CONF_DIR
   * on remote pods.
   */
  def buildConfigMap(configMapName: String, confFileMap: Map[String, String],
      withLabels: Map[String, String] = Map()): ConfigMap = {
    new ConfigMapBuilder()
      .withNewMetadata()
        .withName(configMapName)
        .withLabels(withLabels.asJava)
        .endMetadata()
      .addToData(confFileMap.asJava)
      .build()
  }

  private def loadSparkConfDirFiles(conf: SparkConf): Map[String, String] = {
    val confDir = Option(conf.getenv(ENV_SPARK_CONF_DIR)).orElse(
      conf.getOption("spark.home").map(dir => s"$dir/conf"))
    if (confDir.isDefined) {
      val confFiles = listConfFiles(confDir.get, conf.get(Config.CONFIG_MAP_MAXSIZE))
      val filesSeq: Seq[Option[(String, String)]] = confFiles.map { file =>
        try {
          val source = Source.fromFile(file)(Codec.UTF8)
          val mapping = Some(file.getName -> source.mkString)
          source.close()
          mapping
        } catch {
          case e: MalformedInputException =>
            logWarning(s"Unable to read a non UTF-8 encoded file ${file.getAbsolutePath}.", e)
            None
        }
      }
      val truncatedMap = truncateToSize(filesSeq.flatten, conf.get(Config.CONFIG_MAP_MAXSIZE))
      if (truncatedMap.nonEmpty) {
        logInfo(s"Spark configuration files loaded from $confDir :" +
          s" ${truncatedMap.keys.mkString(",")}")
      }
      truncatedMap
    } else {
      Map.empty[String, String]
    }
  }

  private def listConfFiles(confDir: String, maxSize: Long): Seq[File] = {
    // At the moment configmaps do not support storing binary content.
    // configMaps do not allow for size greater than 1.5 MiB(configurable).
    // https://etcd.io/docs/v3.4.0/dev-guide/limit/
    // We exclude all the template files and user provided spark conf or properties,
    // and binary files (e.g. jars and zip). Spark properties are resolved in a different step.
    def testIfTooLargeOrBinary(f: File): Boolean = (f.length() + f.getName.length > maxSize) ||
      f.getName.matches(".*\\.(gz|zip|jar|tar)") ||
      f.getName.matches(".*\\.template") ||
      f.getName.matches("spark.*(conf|properties)")

    val fileFilter = (f: File) => {
      f.isFile && !testIfTooLargeOrBinary(f)
    }
    val confFiles: Seq[File] = {
      val dir = new File(confDir)
      if (dir.isDirectory) {
        dir.listFiles.filter(x => fileFilter(x)).toSeq
      } else {
        Nil
      }
    }
    confFiles
  }
}
