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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.google.common.base.Strings
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.CommonConfigurationKeysPublic
import org.apache.hadoop.net._
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}

import org.apache.spark.internal.Logging

/**
 * Wrapper around YARN's [[RackResolver]]. This allows Spark tests to easily override the
 * default behavior, since YARN's class self-initializes the first time it's called, and
 * future calls all use the initial configuration.
 */
private[spark] class SparkRackResolver {

  // RackResolver logs an INFO message whenever it resolves a rack, which is way too often.
  if (Logger.getLogger(classOf[RackResolver]).getLevel == null) {
    Logger.getLogger(classOf[RackResolver]).setLevel(Level.WARN)
  }

  def resolve(conf: Configuration, hostName: String): String = {
    RackResolver.resolve(conf, hostName).getNetworkLocation()
  }

  /**
   * Added in SPARK-27038.
   * This should be changed to `RackResolver.resolve(conf, hostNames)`
   * in hadoop releases with YARN-9332.
   */
  def resolve(conf: Configuration, hostNames: List[String]): List[Node] = {
    SparkRackResolver.resolve(conf, hostNames)
  }
}

/**
 * Utility to resolve the rack for hosts in an efficient manner.
 * It will cache the rack for individual hosts to avoid
 * repeatedly performing the same expensive lookup.
 *
 * Its logic refers [[org.apache.hadoop.yarn.util.RackResolver]] and enhanced.
 * This will be unnecessary in hadoop releases with YARN-9332.
 * With that, we could just directly use [[org.apache.hadoop.yarn.util.RackResolver]].
 * In the meantime, this is a re-implementation for spark's use.
 */
object SparkRackResolver extends Logging {
  private var dnsToSwitchMapping: DNSToSwitchMapping = _
  private var initCalled = false

  private def init(conf: Configuration): Unit = {
    if (!initCalled) {
      initCalled = true
      val dnsToSwitchMappingClass =
        conf.getClass(CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
          classOf[ScriptBasedMapping], classOf[DNSToSwitchMapping])
      if (classOf[ScriptBasedMapping].isAssignableFrom(dnsToSwitchMappingClass)) {
        val numArgs = conf.getInt(CommonConfigurationKeysPublic.NET_TOPOLOGY_SCRIPT_NUMBER_ARGS_KEY,
          CommonConfigurationKeysPublic.NET_TOPOLOGY_SCRIPT_NUMBER_ARGS_DEFAULT)
        logInfo(s"Setting spark.hadoop.net.topology.script.number.args with a higher value " +
          s"may reduce the time of rack resolving when submits a stage with a mass of tasks. " +
          s"Current number is $numArgs")
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

  def resolve(conf: Configuration, hostNames: List[String]): List[Node] = {
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
