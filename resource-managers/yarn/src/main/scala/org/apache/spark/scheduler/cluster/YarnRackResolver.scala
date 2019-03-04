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

package org.apache.spark.scheduler.cluster

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.google.common.base.Strings
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.CommonConfigurationKeysPublic
import org.apache.hadoop.net._
import org.apache.hadoop.util.ReflectionUtils

import org.apache.spark.internal.Logging

/**
 * Added in SPARK-27038. Before using the higher Hadoop version which applied YARN-9332,
 * we construct [[YarnRackResolver]] instead of [[org.apache.hadoop.yarn.util.RackResolver]]
 * to revolve the rack info.
 */
object YarnRackResolver extends Logging {
  private var dnsToSwitchMapping: DNSToSwitchMapping = _
  private var initCalled = false
  // advisory count of arguments for rack script
  private val ADVISORY_MINIMUM_NUMBER_SCRIPT_ARGS = 10000

  def init(conf: Configuration): Unit = {
    if (!initCalled) {
      initCalled = true
      val dnsToSwitchMappingClass =
        conf.getClass(CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
          classOf[ScriptBasedMapping], classOf[DNSToSwitchMapping])
      if (classOf[ScriptBasedMapping].isAssignableFrom(dnsToSwitchMappingClass)) {
        val numArgs = conf.getInt(CommonConfigurationKeysPublic.NET_TOPOLOGY_SCRIPT_NUMBER_ARGS_KEY,
          CommonConfigurationKeysPublic.NET_TOPOLOGY_SCRIPT_NUMBER_ARGS_DEFAULT)
        if (numArgs < ADVISORY_MINIMUM_NUMBER_SCRIPT_ARGS) {
          logWarning(s"Increasing the value of" +
            s" ${CommonConfigurationKeysPublic.NET_TOPOLOGY_SCRIPT_NUMBER_ARGS_KEY} could reduce" +
            s" the time of rack resolving when submits a stage with a mass of tasks." +
            s" Current number is $numArgs")
        }
      }
      try {
        val newInstance = ReflectionUtils.newInstance(dnsToSwitchMappingClass, conf)
          .asInstanceOf[DNSToSwitchMapping]
        dnsToSwitchMapping = newInstance match {
          case _: CachedDNSToSwitchMapping => newInstance
          case _ => new CachedDNSToSwitchMapping(newInstance)
        }
      } catch {
        case e: Exception =>
          throw new RuntimeException(e)
      }
    }
  }

  def resolveRacks(conf: Configuration, hostNames: List[String]): List[Node] = {
    init(conf)
    val nodes = new ArrayBuffer[Node]
    val rNameList = dnsToSwitchMapping.resolve(hostNames.toList.asJava).asScala
    if (rNameList == null || rNameList.isEmpty) {
      hostNames.foreach(nodes += new NodeBase(_, NetworkTopology.DEFAULT_RACK))
      logInfo(s"Got an error when resolve hostNames. " +
        s"Falling back to ${NetworkTopology.DEFAULT_RACK} for all")
    } else {
      for ((hostName, rName) <- hostNames.zip(rNameList)) {
        if (Strings.isNullOrEmpty(rName)) {
          // fallback to use default rack
          nodes += new NodeBase(hostName, NetworkTopology.DEFAULT_RACK)
          logDebug(s"Could not resolve $hostName. " +
            s"Falling back to ${NetworkTopology.DEFAULT_RACK}")
        } else {
          nodes += new NodeBase(hostName, rName)
        }
      }
    }
    nodes.toList
  }
}
