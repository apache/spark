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

import com.fasterxml.jackson.core.JsonParseException
import org.json4s.{DefaultFormats, MappingException}
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.util.Utils.executeAndGetOutput

/**
 * Discovers information about resources (GPUs/FPGAs/etc). It currently only supports
 * resources that have addresses.
 * This class finds resources by running and parsing the output of the user specified script
 * from the config spark.{driver/executor}.resource.{resourceName}.discoveryScript.
 * The output of the script it runs is expected to be JSON in the format of the
 * ResourceInformation class.
 *
 * For example:  {"name": "gpu", "addresses": ["0","1"]}
 */
private[spark] object ResourceDiscoverer extends Logging {

  private implicit val formats = DefaultFormats

  /**
   * This function will discover information about a set of resources by using the
   * user specified script (spark.{driver/executor}.resource.{resourceName}.discoveryScript).
   * It optionally takes a set of resource names or if that isn't specified
   * it uses the config prefix passed in to look at the executor or driver configs
   * to get the resource names. Then for each resource it will run the discovery script
   * and get the ResourceInformation about it.
   *
   * @param sparkConf SparkConf
   * @param confPrefix Driver or Executor resource prefix
   * @param resourceNamesOpt Optionally specify resource names. If not set uses the resource
   *                  configs based on confPrefix passed in to get the resource names.
   * @return Map of resource name to ResourceInformation
   */
  def discoverResourcesInformation(
      sparkConf: SparkConf,
      confPrefix: String,
      resourceNamesOpt: Option[Set[String]] = None
      ): Map[String, ResourceInformation] = {
    val resourceNames = resourceNamesOpt.getOrElse(
      // get unique resource names by grabbing first part config with multiple periods,
      // ie resourceName.count, grab resourceName part
      SparkConf.getBaseOfConfigs(sparkConf.getAllWithPrefix(confPrefix))
    )
    resourceNames.map { rName => {
      val rInfo = getResourceInfo(sparkConf, confPrefix, rName)
      (rName -> rInfo)
    }}.toMap
  }

  private def getResourceInfo(
      sparkConf: SparkConf,
      confPrefix: String,
      resourceName: String): ResourceInformation = {
    val discoveryConf = confPrefix + resourceName + SPARK_RESOURCE_DISCOVERY_SCRIPT_SUFFIX
    val script = sparkConf.getOption(discoveryConf)
    val result = if (script.nonEmpty) {
      val scriptFile = new File(script.get)
      // check that script exists and try to execute
      if (scriptFile.exists()) {
        try {
          val output = executeAndGetOutput(Seq(script.get), new File("."))
          val parsedJson = parse(output)
          val name = (parsedJson \ "name").extract[String]
          val addresses = (parsedJson \ "addresses").extract[Array[String]]
          if (name != resourceName) {
            throw new SparkException(s"Discovery script: ${script.get} specified via " +
              s"$discoveryConf returned a resource name: $name that doesn't match the " +
              s"config name: $resourceName")
          }
          new ResourceInformation(name, addresses)
        } catch {
          case e @ (_: SparkException | _: MappingException | _: JsonParseException) =>
            throw new SparkException(s"Error running the resource discovery script: $scriptFile" +
              s" for $resourceName", e)
        }
      } else {
        throw new SparkException(s"Resource script: $scriptFile to discover $resourceName" +
          s" doesn't exist!")
      }
    } else {
      throw new SparkException(s"User is expecting to use $resourceName resources but " +
        s"didn't specify a script via conf: $discoveryConf, to find them!")
    }
    result
  }

  /**
   * Make sure the actual resources we have on startup are at least the number the user
   * requested. Note that there is other code in SparkConf that makes sure we have executor configs
   * for each task resource requirement and that they are large enough. This function
   * is used by both driver and executors.
   *
   * @param requiredResources The resources that are required for us to run.
   * @param actualResources The actual resources discovered.
   */
  def checkActualResourcesMeetRequirements(
      requiredResources: Map[String, String],
      actualResources: Map[String, ResourceInformation]): Unit = {
    requiredResources.foreach { case (rName, reqCount) =>
      val actualRInfo = actualResources.get(rName).getOrElse(
        throw new SparkException(s"Resource: $rName required but wasn't discovered on startup"))

      if (actualRInfo.addresses.size < reqCount.toLong) {
        throw new SparkException(s"Resource: $rName, with addresses: " +
          s"${actualRInfo.addresses.mkString(",")} " +
          s"is less than what the user requested: $reqCount)")
      }
    }
  }
}
