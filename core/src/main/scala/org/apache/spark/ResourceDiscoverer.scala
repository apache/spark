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
import org.json4s.jackson.JsonMethods._

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.util.Utils.executeAndGetOutput

/**
 * Discovers resources (GPUs/FPGAs/etc).
 * This class find resources by running and parses the output of the user specified script
 * from the config spark.{driver/executor}.{resourceType}.discoveryScript.
 * The output of the script it runs is expected to be JSON in the format of the
 * ResourceInformation class, with addresses being optional.
 *
 * For example:  {"name": "gpu","count":2, "units":"", "addresses": ["0","1"]}
 */
private[spark] object ResourceDiscoverer extends Logging {

  private implicit val formats = DefaultFormats

  def findResources(sparkconf: SparkConf, isDriver: Boolean): Map[String, ResourceInformation] = {
    val prefix = if (isDriver) {
      SPARK_DRIVER_RESOURCE_PREFIX
    } else {
      SPARK_EXECUTOR_RESOURCE_PREFIX
    }
    // get unique resource types by grabbing first part config with multiple periods,
    // ie resourceType.count, grab resourceType part
    val resourceTypes = sparkconf.getAllWithPrefix(prefix).map { case (k, _) =>
      k.split('.').head
    }.toSet
    resourceTypes.map { rtype => {
      val rInfo = getResourceAddrsForType(sparkconf, prefix, rtype)
      (rtype -> rInfo)
    }}.toMap
  }

  private def getResourceAddrsForType(
      sparkconf: SparkConf,
      prefix: String,
      resourceType: String): ResourceInformation = {
    val discoveryConf = prefix + resourceType + SPARK_RESOURCE_DISCOVERY_SCRIPT_POSTFIX
    val script = sparkconf.getOption(discoveryConf)
    val result = if (script.nonEmpty) {
      val scriptFile = new File(script.get)
      // check that script exists and try to execute
      if (scriptFile.exists()) {
        try {
          val output = executeAndGetOutput(Seq(script.get), new File("."))
          val parsedJson = parse(output)
          parsedJson.extract[ResourceInformation]
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
