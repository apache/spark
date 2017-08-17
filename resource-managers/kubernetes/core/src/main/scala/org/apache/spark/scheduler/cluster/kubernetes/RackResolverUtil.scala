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
package org.apache.spark.scheduler.cluster.kubernetes

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.CommonConfigurationKeysPublic
import org.apache.hadoop.net.{NetworkTopology, ScriptBasedMapping, TableMapping}
import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}

/**
 * Finds rack names that cluster nodes belong to in order to support HDFS rack locality.
 */
private[kubernetes] trait RackResolverUtil {

  def isConfigured() : Boolean

  def resolveRack(hadoopConfiguration: Configuration, host: String): Option[String]
}

private[kubernetes] class RackResolverUtilImpl(hadoopConfiguration: Configuration)
    extends RackResolverUtil {

  val scriptPlugin : String = classOf[ScriptBasedMapping].getCanonicalName
  val tablePlugin : String = classOf[TableMapping].getCanonicalName
  val isResolverConfigured : Boolean = checkConfigured(hadoopConfiguration)

  // RackResolver logs an INFO message whenever it resolves a rack, which is way too often.
  if (Logger.getLogger(classOf[RackResolver]).getLevel == null) {
    Logger.getLogger(classOf[RackResolver]).setLevel(Level.WARN)
  }

  override def isConfigured() : Boolean = isResolverConfigured

  def checkConfigured(hadoopConfiguration: Configuration): Boolean = {
    val plugin = hadoopConfiguration.get(
      CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY, scriptPlugin)
    val scriptName = hadoopConfiguration.get(
      CommonConfigurationKeysPublic.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY, "")
    val tableName = hadoopConfiguration.get(
      CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, "")
    plugin == scriptPlugin && scriptName.nonEmpty ||
      plugin == tablePlugin && tableName.nonEmpty ||
      plugin != scriptPlugin && plugin != tablePlugin
  }

  override def resolveRack(hadoopConfiguration: Configuration, host: String): Option[String] = {
    val rack = Option(RackResolver.resolve(hadoopConfiguration, host).getNetworkLocation)
    if (rack.nonEmpty && rack.get != NetworkTopology.DEFAULT_RACK) {
      rack
    } else {
      None
    }
  }
}
