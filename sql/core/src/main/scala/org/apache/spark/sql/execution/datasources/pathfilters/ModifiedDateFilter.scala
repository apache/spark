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

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.SparkSession

/**
  * SPARK-31962 - Provide modifiedAfter and modifiedBefore options when
  * filtering from a batch-based file data source.
  *
  * Example Usages
  * Load all CSV files modified after date:
  * spark.read.format("csv").option("modifiedAfter","2020-06-15T05:00:00").load()
  *
  * Load all CSV files modified before date:
  * spark.read.format("csv").option("modifiedBefore","2020-06-15T05:00:00").load()
  *
  * Load all CSV files modified between two dates:
  * spark.read.format("csv").option("modifiedAfter","2019-01-15T05:00:00")
  * .option("modifiedBefore","2020-06-15T05:00:00").load()
  *
  * @param sparkSession SparkSession
  * @param hadoopConf Hadoop Configuration object
  * @param options Map containing options
  */
abstract class ModifiedDateFilter(sparkSession: SparkSession,
                                  hadoopConf: Configuration,
                                  options: Map[String, String])
    extends PathFilterStrategy(sparkSession, hadoopConf, options)
    with FileIndexFilter {
  protected val formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME
  protected lazy val parsedDate: LocalDateTime = {
    LocalDateTime.parse(options.get("timestamp").toString, formatter)
  }
  protected lazy val seconds: Long = {
    parsedDate.toEpochSecond(ZoneOffset.UTC) * 1000
  }

  def accept(fileStatus: FileStatus): Boolean
  def accept(path: Path): Boolean
  def strategy(): String
}
