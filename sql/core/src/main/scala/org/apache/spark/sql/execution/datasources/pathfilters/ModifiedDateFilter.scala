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
SPARK-31962 - Provide option to load files after a specified
date when reading from a folder path.  When specifying the
modifiedDateFilter option in yyyy-mm-ddThh:mm:ss format,
all files having modification dates before this date will not be returned
when loading from a folder path.
Ex:
spark.read
.option("header", "true")
.option("delimiter", "\t")
.option("modifiedDateFilter", "2020-05-01T12:00:00")
.format("csv")
.load("/mnt/Deltas")
 * @param sparkSession SparkSession
 * @param hadoopConf Hadoop Configuration object
 * @param timestamp string representing timestamp
 */
class ModifiedDateFilter(sparkSession: SparkSession, hadoopConf: Configuration, timestamp: String)
    extends FileIndexFilter {
  private val formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME
  private val parsedDate = LocalDateTime.parse(timestamp, formatter)
  private val afterDateSeconds = parsedDate.toEpochSecond(ZoneOffset.UTC) * 1000

  override def accept(fileStatus: FileStatus): Boolean = {
    (fileStatus.getModificationTime - afterDateSeconds) > 0
  }
  override def accept(path: Path): Boolean = true
}
