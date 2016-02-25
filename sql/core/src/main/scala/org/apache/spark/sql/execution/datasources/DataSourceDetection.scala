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

import scala.util.Try

import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapred.{FileInputFormat, JobConf}

import org.apache.spark.{Logging, SparkException}
import org.apache.spark.sql.SQLContext

private[sql] object DataSourceDetection extends Logging {
  /** A map to detect data sources by the extensions of given files. */
  private val extensionDatasourceMap = Map(
    "csv" -> "csv",
    "json" -> "json",
    "txt" -> "text",
    "parquet" -> "parquet",
    "orc" -> "orc"
  )
  // This `magic number` is to detect the Parquet file format. Parquet files start with the
  // bytes. See https://github.com/Parquet/parquet-format.
  private val parquetMagicNumber: Array[Byte] =
    Array(0x50.toByte, 0x41.toByte, 0x52.toByte, 0x31.toByte)
  private val defaultDataSourceName = "parquet"

  /**
   * This detects the data source by extension of one of the given files. If it fails
   * to detect the source, then, this tries to read the "magic number" of Parquet to decide
   * if it is a Parquet file or not. Since it drops `sqlContext.conf.defaultDataSourceName`
   * option, we assume the default datasource is Parquet.
   */
  def detect(sqlContext: SQLContext, path: String): String = {
    // Detection steps follows:
    //  1. Firstly, this tries to check if one of the given paths has the extension. If it has,
    //   it detects data source based on that.
    //  2. If the path does not have an extension, then it tries to find a single leaf file from
    //   the given path and then it tries to find an extension. If it has, it detects data source
    //   based on that.
    //  3. If this even fails, then it tries to read Parquet "magic number" to decide.
    val rootExtension = FilenameUtils.getExtension(path).toLowerCase
    if (rootExtension.toLowerCase.nonEmpty) {
      extensionDatasourceMap
        .getOrElse(rootExtension,
          throw new SparkException(s"Failed to detect data source for extension" +
            s" [$rootExtension]. Known extensions are" +
            s" ${extensionDatasourceMap.keys.mkString(", ")}. Please provide data source."))
    } else {
      val status = headLeafFile(sqlContext, new Path(path)).getOrElse {
        throw new SparkException(s"Failed to pick a file to detect data source detection. " +
          s"Please provide data source.")
      }
      val targetPath = status.getPath.toString
      val childExtension = FilenameUtils.getExtension(targetPath).toLowerCase
      if (childExtension.isEmpty) {
        if (checkParquetFile(sqlContext, targetPath)) {
          defaultDataSourceName
        } else {
          throw new SparkException(s"Detected data source was [$defaultDataSourceName] but " +
            s"it does not have a file format for this. Please provide data source.")
        }
      } else {
        extensionDatasourceMap
          .getOrElse(childExtension,
            throw new SparkException(s"Failed to detect data source for extension" +
              s" [$childExtension]. Known extensions are" +
              s" ${extensionDatasourceMap.keys.mkString(", ")}. Please provide data source."))
      }
    }
  }

  private def checkParquetFile(sqlContext: SQLContext, path: String): Boolean = {
    val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
    val hdfsPath = new Path(path)
    val fs = hdfsPath.getFileSystem(hadoopConf)
    val buffer = new Array[Byte](parquetMagicNumber.length)
    fs.open(hdfsPath).read(buffer)
    buffer.sameElements(parquetMagicNumber)
  }

  private def headLeafFile(sqlContext: SQLContext, hdfsPath: Path): Option[FileStatus] = {
    val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
    val statuses = {
      val fs = hdfsPath.getFileSystem(hadoopConf)
      val qualified = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
      logInfo(s"Listing $qualified on driver")
      // Dummy jobconf to get to the pathFilter defined in configuration
      val jobConf = new JobConf(hadoopConf, this.getClass())
      val pathFilter = FileInputFormat.getInputPathFilter(jobConf)
      if (pathFilter != null) {
        Try(fs.listStatus(qualified, pathFilter)).getOrElse(Array.empty)
      } else {
        Try(fs.listStatus(qualified)).getOrElse(Array.empty)
      }
    }.filterNot { status =>
      val name = status.getPath.getName
      name.startsWith("_") || name.startsWith(".")
    }
    val (dirs, files) = statuses.partition(_.isDirectory)

    if (files.nonEmpty) {
      Some(files.head)
    } else if (dirs.isEmpty) {
      None
    } else {
      headLeafFile(sqlContext, dirs.head.getPath)
    }
  }
}
