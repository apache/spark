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

import scala.collection.mutable

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.yarn.config._

private object ResourceRequestValidator {
  private val ERROR_PREFIX: String = "Error:"

  /**
   * Validates sparkConf and throws a SparkException if a standard resource (memory or cores)
   * is defined with the property spark.yarn.x.resource.y<br>
   *
   * Example of an invalid config:<br>
   * - spark.yarn.driver.resource.memory=2g<br>
   *
   * Please note that if multiple resources are defined like described above,
   * the error messages will be concatenated.<br>
   * Example of such a config:<br>
   * - spark.yarn.driver.resource.memory=2g<br>
   * - spark.yarn.executor.resource.cores=2<br>
   * Then the following two error messages will be printed:<br>
   * - "memory cannot be requested with config spark.yarn.driver.resource.memory,
   * please use config spark.driver.memory instead!<br>
   * - "cores cannot be requested with config spark.yarn.executor.resource.cores,
   * please use config spark.executor.cores instead!<br>
   *
   * @param sparkConf
   */
  def validateResources(sparkConf: SparkConf): Unit = {
    val resourceDefinitions = Seq[(String, String)](
      (AM_MEMORY.key, YARN_AM_RESOURCE_TYPES_PREFIX + "memory"),
      (AM_CORES.key, YARN_AM_RESOURCE_TYPES_PREFIX + "cores"),
      ("spark.driver.memory", YARN_DRIVER_RESOURCE_TYPES_PREFIX + "memory"),
      (DRIVER_CORES.key, YARN_DRIVER_RESOURCE_TYPES_PREFIX + "cores"),
      ("spark.executor.memory", YARN_EXECUTOR_RESOURCE_TYPES_PREFIX + "memory"),
      (EXECUTOR_CORES.key, YARN_EXECUTOR_RESOURCE_TYPES_PREFIX + "cores"))
    val errorMessage = new mutable.StringBuilder()

    resourceDefinitions.foreach { case (sparkName, resourceRequest) =>
      if (sparkConf.contains(resourceRequest)) {
        errorMessage.append(s"$ERROR_PREFIX Do not use $resourceRequest, " +
            s"please use $sparkName instead!\n")
      }
    }

    // throw exception after loop
    if (errorMessage.nonEmpty) {
      throw new SparkException(errorMessage.toString())
    }
  }
}
