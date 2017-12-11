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
   * Helper method that turns session configs with config keys that start with
   * `spark.datasource.$name` into k/v pairs, the k/v pairs will be used to create data source
   * options.
   * A session config `spark.datasource.$name.xxx -> yyy` will be transformed into
   * `xxx -> yyy`.
   *
   * @param name the data source name
   * @param conf the session conf
   * @return an immutable map that contains all the extracted and transformed k/v pairs.
   */
  def withSessionConfig(
      name: String,
      conf: SQLConf): immutable.Map[String, String] = {
    require(name != null, "The data source name can't be null.")

    val pattern = Pattern.compile(s"spark\\.datasource\\.$name\\.(.*)")
    val filteredConfigs = conf.getAllConfs.filterKeys { confKey =>
      confKey.startsWith(s"spark.datasource.$name")
    }
    filteredConfigs.map { entry =>
      val m = pattern.matcher(entry._1)
      require(m.matches() && m.groupCount() > 0, s"Fail in matching ${entry._1} with $pattern.")
      (m.group(1), entry._2)
    }
  }
}
