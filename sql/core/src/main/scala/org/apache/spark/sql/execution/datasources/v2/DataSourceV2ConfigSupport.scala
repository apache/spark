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

package org.apache.spark.sql.execution.datasources.v2

import java.util.regex.Pattern

import scala.collection.JavaConverters._
import scala.collection.immutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.v2.ConfigSupport

private[sql] object DataSourceV2ConfigSupport extends Logging {

  /**
   * Helper method to propagate session configs with config key that matches at least one of the
   * given prefixes to the corresponding data source options.
   *
   * @param cs the session config propagate help class
   * @param source the data source format
   * @param conf the session conf
   * @return an immutable map that contains all the session configs that should be propagated to
   *         the data source.
   */
  def withSessionConfig(
      cs: ConfigSupport,
      source: String,
      conf: SQLConf): immutable.Map[String, String] = {
    val prefixes = cs.getConfigPrefixes
    require(prefixes != null, "The config key-prefixes cann't be null.")
    val mapping = cs.getConfigMapping.asScala
    val validOptions = cs.getValidOptions
    require(validOptions != null, "The valid options list cann't be null.")

    val pattern = Pattern.compile(s"spark\\.sql(\\.$source)?\\.(.*)")
    val filteredConfigs = conf.getAllConfs.filterKeys { confKey =>
      prefixes.asScala.exists(confKey.startsWith(_))
    }
    val convertedConfigs = filteredConfigs.map{ entry =>
      val newKey = mapping.get(entry._1).getOrElse {
        val m = pattern.matcher(entry._1)
        if (m.matches()) {
          m.group(2)
        } else {
          // Unable to recognize the session config key.
          logWarning(s"Unrecognizable session config name ${entry._1}.")
          entry._1
        }
      }
      (newKey, entry._2)
    }
    if (validOptions.size == 0) {
      convertedConfigs
    } else {
      // Check whether all the valid options are propagated.
      validOptions.asScala.foreach { optionName =>
        if (!convertedConfigs.keySet.contains(optionName)) {
          logWarning(s"Data source option '$optionName' is required, but not propagated from " +
            "session config, please check the config settings.")
        }
      }

      // Filter the valid options.
      convertedConfigs.filterKeys { optionName =>
        validOptions.contains(optionName)
      }
    }
  }
}
