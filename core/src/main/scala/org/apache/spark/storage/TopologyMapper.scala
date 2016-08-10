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

package org.apache.spark.storage

import java.io.{File, FileInputStream}
import java.util.Properties

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.SparkConf
import org.apache.spark.util.Utils

/**
 * ::DeveloperApi::
 * TopologyMapper provides topology information for a given host
 * @param conf SparkConf to get required properties, if needed
 */
@DeveloperApi
abstract class TopologyMapper(conf: SparkConf) {
  /**
   * Gets the topology information given the host name
   *
   * @param hostname Hostname
   * @return topology information for the given hostname. One can use a 'topology delimiter'
   *         to make this topology information nested.
   *         For example : ‘/myrack/myhost’, where ‘/’ is the topology delimiter,
   *         ‘myrack’ is the topology identifier, and ‘myhost’ is the individual host.
   *         This function only returns the topology information without the hostname.
   */
  def getTopologyForHost(hostname: String): String
}

@DeveloperApi
class DefaultTopologyMapper(conf: SparkConf) extends TopologyMapper(conf) with Logging {
  override def getTopologyForHost(hostname: String): String = {
    logDebug(s"Got a request for $hostname")
    "DefaultRack"
  }
}

/**
 * A simple file based topology mapper. This expects topology information provided as a
 * [[java.util.Properties]] file. The name of the file is obtained from SparkConf property
 * `spark.replication.topologyawareness.topologyFile`. To use this topology mapper, set the
 * `spark.replication.topologyawareness.topologyMapper` property to
 * [[org.apache.spark.storage.FileBasedTopologyMapper]]
 * @param conf SparkConf object
 */
@DeveloperApi
class FileBasedTopologyMapper(conf: SparkConf) extends TopologyMapper(conf) with Logging {
  val topologyFile = conf.getOption("spark.replication.topologyawareness.topologyfile")
  require(topologyFile.isDefined, "Please provide topology file for FileBasedTopologyMapper.")
  val topologyMap = Utils.getPropertiesFromFile(topologyFile.get)

  override def getTopologyForHost(hostname: String): String = {
    val topology = topologyMap.get(hostname)
    if (topology.isDefined) {
      logDebug(s"$hostname -> ${topology.get}")
      topology.get
    } else {
      logWarning(s"$hostname does not have any topology information")
      ""
    }
  }
}

