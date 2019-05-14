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
 * Discovers resources (GPUs/FPGAs/etc). It currently only supports resources that have
 * addresses.
 * This class finds resources by running and parsing the output of the user specified script
 * from the config spark.{driver/executor}.resource.{resourceType}.discoveryScript.
 * The output of the script it runs is expected to be JSON in the format of the
 * ResourceInformation class.
 *
 * For example:  {"name": "gpu", "addresses": ["0","1"]}
 */
private[spark] object ResourceDiscoverer extends Logging {

  private implicit val formats = DefaultFormats

  def findResources(sparkConf: SparkConf, isDriver: Boolean): Map[String, ResourceInformation] = {
    val prefix = if (isDriver) {
      SPARK_DRIVER_RESOURCE_PREFIX
    } else {
      SPARK_EXECUTOR_RESOURCE_PREFIX
    }
    // get unique resource types by grabbing first part config with multiple periods,
    // ie resourceType.count, grab resourceType part
    val resourceNames = sparkConf.getAllWithPrefix(prefix).map { case (k, _) =>
      k.split('.').head
    }.toSet
    resourceNames.map { rName => {
      val rInfo = getResourceInfoForType(sparkConf, prefix, rName)
      (rName -> rInfo)
    }}.toMap
  }

  private def getResourceInfoForType(
      sparkConf: SparkConf,
      prefix: String,
      resourceType: String): ResourceInformation = {
    val discoveryConf = prefix + resourceType + SPARK_RESOURCE_DISCOVERY_SCRIPT_POSTFIX
    val script = sparkConf.getOption(discoveryConf)
    val result = if (script.nonEmpty) {
      val scriptFile = new File(script.get)
      // check that script exists and try to execute
      if (scriptFile.exists()) {
        try {
          val output = executeAndGetOutput(Seq(script.get), new File("."))
          val parsedJson = parse(output)
          val name = (parsedJson \ "name").extract[String]
          val addresses = (parsedJson \ "addresses").extract[Array[String]].toArray
          new ResourceInformation(name, addresses)
        } catch {
          case e @ (_: SparkException | _: MappingException | _: JsonParseException) =>
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
}
