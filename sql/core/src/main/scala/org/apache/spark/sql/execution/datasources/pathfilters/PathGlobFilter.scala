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


package org.apache.spark.sql.execution.datasources.pathfilters

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, GlobFilter}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

class PathGlobFilter(sparkSession: SparkSession,
                     conf: Configuration,
                     options: CaseInsensitiveMap[String])
    extends GlobFilter(options.get("pathGlobFilter").get)
    with FileIndexFilter {

  override def accept(fileStatus: FileStatus): Boolean =
    accept(fileStatus.getPath)
  override def strategy(): String = "pathGlobFilter"
}
case object PathGlobFilter extends PathFilterObject {
  def get(sparkSession: SparkSession,
          configuration: Configuration,
          options: CaseInsensitiveMap[String]): PathGlobFilter = {
    new PathGlobFilter(sparkSession, configuration, options)
  }
  def strategy(): String = "pathglobfilter"
}
