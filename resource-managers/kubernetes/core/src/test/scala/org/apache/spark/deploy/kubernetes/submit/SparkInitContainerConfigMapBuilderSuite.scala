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
package org.apache.spark.deploy.kubernetes.submit

import java.io.StringReader
import java.util.Properties

import com.google.common.collect.Maps
import org.mockito.Mockito.{verify, when}
import org.scalatest.BeforeAndAfter
import org.scalatest.mock.MockitoSugar._
import scala.collection.JavaConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.kubernetes.config._

class SparkInitContainerConfigMapBuilderSuite extends SparkFunSuite with BeforeAndAfter {

  private val JARS = Seq(
    "hdfs://localhost:9000/app/jars/jar1.jar",
    "file:///app/jars/jar2.jar",
    "http://localhost:9000/app/jars/jar3.jar",
    "local:///app/jars/jar4.jar")
  private val FILES = Seq(
    "hdfs://localhost:9000/app/files/file1.txt",
    "file:///app/files/file2.txt",
    "http://localhost:9000/app/files/file3.txt",
    "local:///app/files/file4.txt")
  private val JARS_DOWNLOAD_PATH = "/var/data/jars"
  private val FILES_DOWNLOAD_PATH = "/var/data/files"
  private val CONFIG_MAP_NAME = "config-map"
  private val CONFIG_MAP_KEY = "config-map-key"

  test("Config map without submitted dependencies sets remote download configurations") {
    val configMap = new SparkInitContainerConfigMapBuilderImpl(
      JARS,
      FILES,
      JARS_DOWNLOAD_PATH,
      FILES_DOWNLOAD_PATH,
      CONFIG_MAP_NAME,
      CONFIG_MAP_KEY,
      None).build()
    assert(configMap.getMetadata.getName === CONFIG_MAP_NAME)
    val maybeConfigValue = configMap.getData.asScala.get(CONFIG_MAP_KEY)
    assert(maybeConfigValue.isDefined)
    maybeConfigValue.foreach { configValue =>
      val propertiesStringReader = new StringReader(configValue)
      val properties = new Properties()
      properties.load(propertiesStringReader)
      val propertiesMap = Maps.fromProperties(properties).asScala
      val remoteJarsString = propertiesMap.get(INIT_CONTAINER_REMOTE_JARS.key)
      assert(remoteJarsString.isDefined)
      val remoteJars = remoteJarsString.map(_.split(",")).toSet.flatten
      assert(remoteJars ===
        Set("hdfs://localhost:9000/app/jars/jar1.jar", "http://localhost:9000/app/jars/jar3.jar"))
      val remoteFilesString = propertiesMap.get(INIT_CONTAINER_REMOTE_FILES.key)
      assert(remoteFilesString.isDefined)
      val remoteFiles = remoteFilesString.map(_.split(",")).toSet.flatten
      assert(remoteFiles ===
        Set("hdfs://localhost:9000/app/files/file1.txt",
          "http://localhost:9000/app/files/file3.txt"))
      assert(propertiesMap(INIT_CONTAINER_JARS_DOWNLOAD_LOCATION.key) === JARS_DOWNLOAD_PATH)
      assert(propertiesMap(INIT_CONTAINER_FILES_DOWNLOAD_LOCATION.key) === FILES_DOWNLOAD_PATH)
    }
  }

  test("Config map with submitted dependencies adds configurations from plugin") {
    val submittedDependenciesPlugin = mock[SubmittedDependencyInitContainerConfigPlugin]
    when(submittedDependenciesPlugin.configurationsToFetchSubmittedDependencies())
      .thenReturn(Map("customConf" -> "customConfValue"))
    val configMap = new SparkInitContainerConfigMapBuilderImpl(
      JARS,
      FILES,
      JARS_DOWNLOAD_PATH,
      FILES_DOWNLOAD_PATH,
      CONFIG_MAP_NAME,
      CONFIG_MAP_KEY,
      Some(submittedDependenciesPlugin)).build()
    val configValue = configMap.getData.asScala(CONFIG_MAP_KEY)
    val propertiesStringReader = new StringReader(configValue)
    val properties = new Properties()
    properties.load(propertiesStringReader)
    val propertiesMap = Maps.fromProperties(properties).asScala
    assert(propertiesMap("customConf") === "customConfValue")
    verify(submittedDependenciesPlugin).configurationsToFetchSubmittedDependencies()
  }
}
