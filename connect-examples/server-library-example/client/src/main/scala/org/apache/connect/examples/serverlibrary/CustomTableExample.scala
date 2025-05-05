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

package org.apache.connect.examples.serverlibrary

import java.nio.file.{Path, Paths}

import com.google.protobuf.Any
import org.apache.spark.connect.proto.Command
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

import org.apache.connect.examples.serverlibrary.proto
import org.apache.connect.examples.serverlibrary.proto.CreateTable.Column.{DataType => ProtoDataType}

object CustomTableExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().remote("sc://localhost").build()

    // Step 1: Create a custom table from existing data
    val tableName = "sample_table"
    val fileName = "dummy_data.data"

    val workingDirectory = System.getProperty("user.dir")
    val fullPath = Paths.get(workingDirectory, s"resources/$fileName")
    val customTable = CustomTable
      .create(spark)
      .name(tableName)
      .path(fullPath.toString)
      .addColumn("id", CustomTable.DataType.Int)
      .addColumn("name", CustomTable.DataType.String)
      .build()

    // Step 2: Verify
    customTable.explain()

    // Step 3: Clone the custom table
    val clonedPath = fullPath.getParent.resolve("cloned_data.data")
    val clonedName = "cloned_table"
    val clonedTable =
      customTable.clone(target = clonedPath.toString, newName = clonedName, replace = true)

    // Step 4: Verify
    clonedTable.explain()
  }
}
