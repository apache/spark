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
 *  - modification_time: `TimestampType` (last modification time of the file)
 *  - length: `LongType` (the file length)
 */
class BinaryFileDataSource private() {}

object BinaryFileDataSource {

  val fileStatusSchema = StructType(
    StructField("path", StringType, true) ::
      StructField("modification_time", TimestampType, true) ::
      StructField("length", LongType, true) :: Nil)

  val binaryFileSchema = StructType(
    StructField("status", fileStatusSchema, true) ::
      StructField("content", BinaryType, true) :: Nil)

}
