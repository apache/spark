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

package org.apache.spark.deploy.yarn

import java.net.URLClassLoader

import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.spark._
import org.apache.spark.internal.config._
import org.apache.spark.network.shuffledb.DBBackend
import org.apache.spark.network.yarn.{YarnShuffleService, YarnTestAccessor}
import org.apache.spark.tags.ExtendedYarnTest

/**
 * SPARK-34828: Integration test for the external shuffle service with an alternate name and
 * configs (by using a configuration overlay)
 */
abstract class YarnShuffleAlternateNameConfigSuite extends YarnShuffleIntegrationSuite {

  private[this] val shuffleServiceName = "custom_shuffle_service_name"

  override def newYarnConfig(): YarnConfiguration = {
    val yarnConfig = super.newYarnConfig()
    yarnConfig.set(YarnConfiguration.NM_AUX_SERVICES, shuffleServiceName)
    yarnConfig.set(YarnConfiguration.NM_AUX_SERVICE_FMT.format(shuffleServiceName),
      classOf[YarnShuffleService].getCanonicalName)
    val overlayConf = new YarnConfiguration()
    // Enable authentication in the base NodeManager conf but not in the client. This would break
    // shuffle, unless the shuffle service conf overlay overrides to turn off authentication.
    overlayConf.setBoolean(NETWORK_AUTH_ENABLED.key, true)
    // Add the authentication conf to a separate config object used as an overlay rather than
    // setting it directly. This is necessary because a config overlay will override previous
    // config overlays, but not configs which were set directly on the config object.
    yarnConfig.addResource(overlayConf)
    yarnConfig
  }

  override protected def extraSparkConf(): Map[String, String] =
    super.extraSparkConf() ++ Map(SHUFFLE_SERVICE_NAME.key -> shuffleServiceName)

  override def beforeAll(): Unit = {
    val configFileContent =
      s"""<?xml version="1.0" encoding="UTF-8"?>
         |<configuration>
         |  <property>
         |    <name>${NETWORK_AUTH_ENABLED.key}</name>
         |    <value>false</value>
         |  </property>
         |</configuration>
         |""".stripMargin
    val jarFile = TestUtils.createJarWithFiles(Map(
      YarnTestAccessor.getShuffleServiceConfOverlayResourceName -> configFileContent
    ))
    // Configure a custom classloader which includes the conf overlay as a resource
    val oldClassLoader = Thread.currentThread().getContextClassLoader
    Thread.currentThread().setContextClassLoader(new URLClassLoader(Array(jarFile)))
    try {
      super.beforeAll()
    } finally {
      Thread.currentThread().setContextClassLoader(oldClassLoader)
    }
  }
}
@ExtendedYarnTest
class YarnShuffleAlternateNameConfigWithLevelDBBackendSuite
  extends YarnShuffleAlternateNameConfigSuite {
  override protected def dbBackend: DBBackend = DBBackend.LEVELDB
}
