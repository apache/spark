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

package org.apache.spark

import java.io.File

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.util.Utils.executeAndGetOutput

/**
 * Discovers resources (GPUs/FPGAs/etc).
 * This class find resources by running and parses the output of the user specified script
 * from the config spark.{driver/executor}.{resourceType}.discoveryScript.
 * The output of the script it runs is expected to be a String that is in the format of
 * count:unit:comma-separated list of addresses, where the list of addresses is
 * specific for that resource type. The user is responsible for interpreting the address.
 */
private[spark] object ResourceDiscoverer extends Logging {

  def findResources(sparkconf: SparkConf, isDriver: Boolean): Map[String, ResourceInformation] = {
    val prefix = if (isDriver) {
      SPARK_DRIVER_RESOURCE_PREFIX
    } else {
      SPARK_EXECUTOR_RESOURCE_PREFIX
    }
    // get unique resource types
    val resourceTypes = sparkconf.getAllWithPrefix(prefix).map(x => x._1.split('.')(0)).toSet
    resourceTypes.map{ rtype => {
      val rInfo = getResourceAddrsForType(sparkconf, prefix, rtype)
      (rtype -> rInfo)
    }}.toMap
  }

  private def getResourceAddrsForType(sparkconf: SparkConf,
        prefix: String, resourceType: String): ResourceInformation = {
    val discoveryConf = prefix + resourceType + SPARK_RESOURCE_DISCOVERY_SCRIPT_POSTFIX
    val script = sparkconf.getOption(discoveryConf)
    val result = if (script.nonEmpty) {
      val scriptFile = new File(script.get)
      // check that script exists and try to execute
      if (scriptFile.exists()) {
        try {
          val output = executeAndGetOutput(Seq(script.get), new File("."))
          parseResourceTypeString(resourceType, output)
        } catch {
          case e @ (_: SparkException | _: NumberFormatException) =>
            throw new SparkException(s"Error running the resource discovery script: $scriptFile" +
              s" for $resourceType", e)
        }
      } else {
        throw new SparkException(s"Resource script: $scriptFile to discover $resourceType" +
          s" doesn't exist!")
      }
    } else {
      throw new SparkException(s"User is expecting to use $resourceType resources but " +
        s"didn't specify a script via conf: $discoveryConf, to find them!")
    }
    result
  }

  // this parses a resource information string in the format:
  // count:unit:comma-separated list of addresses
  // The units and addresses are optional. The idea being if the user has something like
  // memory you don't have addresses to assign out.
  def parseResourceTypeString(rtype: String, rInfoStr: String): ResourceInformation = {
    // format should be: count:unit:addr1,addr2,addr3
    val singleResourceType = rInfoStr.split(':')
    if (singleResourceType.size < 3) {
      throw new SparkException("Format of the resourceAddrs parameter is invalid," +
        " please specify all of count, unit, and addresses in the format:" +
        " count:unit:addr1,addr2,addr3")
    }
    // format should be: addr1,addr2,addr3
    val splitAddrs = singleResourceType(2).split(',').map(_.trim())
    val retAddrs = if (splitAddrs.size == 1 && splitAddrs(0).isEmpty()) {
      Array.empty[String]
    } else {
      splitAddrs
    }
    new ResourceInformation(rtype, singleResourceType(1), singleResourceType(0).toLong, retAddrs)
  }
}
