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

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.PathFilter

import scala.collection.parallel.mutable.ParArray

/**
 * SPARK-31962 - Provide option to load files after a specified
 * date when reading from a folder path.
*/
object FilesModifiedAfterDateOption {
	def accept(
		  parameters: Map[String, String],
		  sparkSession: SparkSession,
		  hadoopConf: Configuration): ParArray[PathFilter] = {
		var afterDateSeconds = 0L
		val option = "filesModifiedAfterDate"
		val filesModifiedAfterDate = parameters.get(option)
		val hasModifiedDateOption = filesModifiedAfterDate.isDefined

		if (hasModifiedDateOption) {
			filesModifiedAfterDate.foreach(fileDate => {
				val formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME
				val locateDate = LocalDateTime.parse(fileDate, formatter)
				afterDateSeconds = locateDate.toEpochSecond(ZoneOffset.UTC) * 1000
			})
			val fileFilter = new PathFilterIgnoreOldFiles(
				sparkSession,
				hadoopConf,
				afterDateSeconds)

			return ParArray[PathFilter](fileFilter)
		}
		ParArray[PathFilter]()
	}
}
