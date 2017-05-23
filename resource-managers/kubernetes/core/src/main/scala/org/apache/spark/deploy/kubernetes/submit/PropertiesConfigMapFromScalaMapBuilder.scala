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

import java.io.StringWriter
import java.util.Properties

import io.fabric8.kubernetes.api.model.{ConfigMap, ConfigMapBuilder}

/**
 * Creates a config map from a map object, with a single given key
 * and writing the map in a {@link java.util.Properties} format.
 */
private[spark] object PropertiesConfigMapFromScalaMapBuilder {

  def buildConfigMap(
      configMapName: String,
      configMapKey: String,
      config: Map[String, String]): ConfigMap = {
    val properties = new Properties()
    config.foreach { case (key, value) => properties.setProperty(key, value) }
    val propertiesWriter = new StringWriter()
    properties.store(propertiesWriter,
      s"Java properties built from Kubernetes config map with name: $configMapName" +
        " and config map key: $configMapKey")
    new ConfigMapBuilder()
      .withNewMetadata()
        .withName(configMapName)
        .endMetadata()
      .addToData(configMapKey, propertiesWriter.toString)
      .build()
  }
}
