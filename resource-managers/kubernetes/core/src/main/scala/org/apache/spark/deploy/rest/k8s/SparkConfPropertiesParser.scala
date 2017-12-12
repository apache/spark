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
package org.apache.spark.deploy.rest.k8s

import java.io.{File, FileInputStream}
import java.util.Properties

import scala.collection.JavaConverters._

import com.google.common.collect.Maps

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.{ConfigReader, SparkConfigProvider}
import org.apache.spark.util.Utils

private[spark] object SparkConfPropertiesParser {

  def getSparkConfFromPropertiesFile(propertiesFile: File): SparkConf = {
    val sparkConf = new SparkConf(true)

    if (!propertiesFile.isFile) {
      throw new IllegalArgumentException(s"Server properties file given at" +
        s" ${propertiesFile.getAbsoluteFile} does not exist or is not a file.")
    }

    val properties = new Properties
    Utils.tryWithResource(new FileInputStream(propertiesFile))(properties.load)
    val propertiesMap = Maps.fromProperties(properties)
    val configReader = new ConfigReader(new SparkConfigProvider(propertiesMap))
    propertiesMap.asScala.keys.foreach { key =>
      configReader.get(key).foreach(sparkConf.set(key, _))
    }

    sparkConf
  }
}
