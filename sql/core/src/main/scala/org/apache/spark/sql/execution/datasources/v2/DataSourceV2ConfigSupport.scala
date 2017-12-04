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

import scala.collection.JavaConverters._
import scala.collection.immutable

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.v2.ConfigSupport

private[sql] object DataSourceV2ConfigSupport {

  /**
   * Helper method to filter session configs with config key that matches at least one of the given
   * prefixes.
   *
   * @param cs the config key-prefixes that should be filtered.
   * @param conf the session conf
   * @return an immutable map that contains all the session configs that should be propagated to
   *         the data source.
   */
  def withSessionConfig(
      cs: ConfigSupport,
      conf: SQLConf): immutable.Map[String, String] = {
    val prefixes = cs.getConfigPrefixes
    require(prefixes != null, "The config key-prefixes cann't be null.")
    conf.getAllConfs.filterKeys { confKey =>
      prefixes.asScala.exists(confKey.startsWith(_))
    }
  }
}
