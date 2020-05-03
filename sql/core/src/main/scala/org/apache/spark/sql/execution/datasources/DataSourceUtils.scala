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

package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.fs.Path
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization

import org.apache.spark.sql.{SPARK_LEGACY_DATETIME, SPARK_VERSION_METADATA_KEY}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils


object DataSourceUtils {
  /**
   * The key to use for storing partitionBy columns as options.
   */
  val PARTITIONING_COLUMNS_KEY = "__partition_columns"

  /**
   * Utility methods for converting partitionBy columns to options and back.
   */
  private implicit val formats = Serialization.formats(NoTypeHints)

  def encodePartitioningColumns(columns: Seq[String]): String = {
    Serialization.write(columns)
  }

  def decodePartitioningColumns(str: String): Seq[String] = {
    Serialization.read[Seq[String]](str)
  }

  /**
   * Verify if the schema is supported in datasource. This verification should be done
   * in a driver side.
   */
  def verifySchema(format: FileFormat, schema: StructType): Unit = {
    schema.foreach { field =>
      if (!format.supportDataType(field.dataType)) {
        throw new AnalysisException(
          s"$format data source does not support ${field.dataType.catalogString} data type.")
      }
    }
  }

  // SPARK-24626: Metadata files and temporary files should not be
  // counted as data files, so that they shouldn't participate in tasks like
  // location size calculation.
  private[sql] def isDataPath(path: Path): Boolean = isDataFile(path.getName)

  private[sql] def isDataFile(fileName: String) =
    !(fileName.startsWith("_") || fileName.startsWith("."))

  def needRebaseDateTime(lookupFileMeta: String => String): Option[Boolean] = {
    if (Utils.isTesting && SQLConf.get.getConfString("spark.test.forceNoRebase", "") == "true") {
      return Some(false)
    }
    // If there is no version, we return None and let the caller side to decide.
    Option(lookupFileMeta(SPARK_VERSION_METADATA_KEY)).map { version =>
      // Files written by Spark 2.4 and earlier follow the legacy hybrid calendar and we need to
      // rebase the datetime values.
      // Files written by Spark 3.0 and latter may also need the rebase if they were written with
      // the "rebaseInWrite" config enabled.
      version < "3.0.0" || lookupFileMeta(SPARK_LEGACY_DATETIME) != null
    }
  }
}
