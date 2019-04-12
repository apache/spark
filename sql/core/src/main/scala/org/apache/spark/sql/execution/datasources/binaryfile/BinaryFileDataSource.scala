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

package org.apache.spark.sql.execution.datasources.binaryfile

import org.apache.spark.sql.types._

/**
 * `binaryfile` package implements Spark SQL data source API for loading binary file data
 * as `DataFrame`.
 *
 * The loaded `DataFrame` has two columns, the schema is:
 *  - status: `StructType` (the file status information)
 *  - content: `BinaryType` (binary data of the file content)
 *
 * The schema of "status" column described above is:
 *  - path: `StringType` (the file path)
 *  - modificationTime: `TimestampType` (last modification time of the file)
 *  - len: `LongType` (the file length)
 *
 * To use binary file data source, you need to set "binaryFile" as the format in `DataFrameReader`
 * and optionally specify the data source options, available options include:
 *  - pathGlobFilter: Only include files with path matching the glob pattern.
 *                    The glob pattern keep the same behavior with hadoop API
 *                    `org.apache.hadoop.fs.FileSystem.globStatus(pathPattern)`
 *
 * Example:
 * {{{
 *   // Scala
 *   val df = spark.read.format("binaryFile")
 *     .option("pathGlobFilter", "*.txt")
 *     .load("path/to/fileDir")
 *
 *   // Java
 *   Dataset<Row> df = spark.read().format("binaryFile")
 *     .option("pathGlobFilter", "*.txt")
 *     .load("path/to/fileDir");
 * }}}
 *
 * @note This binary file data source does not support saving dataframe to binary files.
 * @note This class is public for documentation purpose. Please don't use this class directly.
 * Rather, use the data source API as illustrated above.
 */
class BinaryFileDataSource private() {}

object BinaryFileDataSource {

  val fileStatusSchema = StructType(
    StructField("path", StringType, true) ::
      StructField("modificationTime", TimestampType, true) ::
      StructField("len", LongType, true) :: Nil)

  val binaryFileSchema = StructType(
    StructField("status", fileStatusSchema, true) ::
      StructField("content", BinaryType, true) :: Nil)

}
