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

import com.google.protobuf.Any
import org.apache.spark.connect.proto.Command
import org.apache.spark.sql.SparkSession

import org.apache.connect.examples.serverlibrary.CustomTable

/**
 * Builder class for constructing a `CustomTable` instance.
 *
 * @param spark The Spark session.
 */
class CustomTableBuilder private[serverlibrary] (spark: SparkSession) {
  import CustomTableBuilder._

  private var name: String = _
  private var path: String = _
  private var columns: Seq[Column] = Seq.empty

  /**
   * Sets the name of the custom table.
   *
   * @param name The name of the table.
   * @return The current `CustomTableBuilder` instance.
   */
  def name(name: String): CustomTableBuilder = {
    this.name = name
    this
  }

  /**
   * Sets the path of the custom table.
   *
   * @param path The path of the table.
   * @return The current `CustomTableBuilder` instance.
   */
  def path(path: String): CustomTableBuilder = {
    this.path = path
    this
  }

  /**
   * Adds a column to the custom table.
   *
   * @param name The name of the column.
   * @param dataType The data type of the column.
   * @return The current `CustomTableBuilder` instance.
   */
  def addColumn(name: String, dataType: CustomTable.DataType.Value): CustomTableBuilder = {
    columns = columns :+ Column(name, dataType)
    this
  }

  /**
   * Builds the `CustomTable` instance.
   *
   * @return A new `CustomTable` instance.
   * @throws IllegalArgumentException if name, path, or columns are not set.
   */
  def build(): CustomTable = {
    require(name != null, "Name must be set")
    require(path != null, "Path must be set")
    require(columns.nonEmpty, "At least one column must be added")

    // Define the table creation proto
    val createTableProtoBuilder = proto.CreateTable
      .newBuilder()
      .setTable(
        proto.CustomTable
          .newBuilder()
          .setPath(path)
          .setName(name)
          .build())

    // Add columns to the table creation proto
    columns.foreach { column =>
      createTableProtoBuilder.addColumns(
        proto.CreateTable.Column
          .newBuilder()
          .setName(column.name)
          .setDataType(CustomTable.DataType.toProto(column.dataType))
          .build())
    }
    val createTableProto = createTableProtoBuilder.build() // Build the CreateTable object

    // Wrap the CreateTable proto in CustomCommand
    val customCommand = proto.CustomCommand
      .newBuilder()
      .setCreateTable(createTableProto)
      .build()

    // Pack the CustomCommand into Any
    val customCommandAny = Any.pack(customCommand)
    // Set the Any as the extension of a Command
    val commandProto = Command
      .newBuilder()
      .setExtension(customCommandAny)
      .build()

    // Execute the command
    spark.execute(commandProto)

    // After the command is executed, create a client-side representation of the table with the
    // leaf node of the client-side dataset being the Scan node for the custom table.
    CustomTable.from(spark, name, path)
  }
}

object CustomTableBuilder {
  /**
   * Case class representing a column in the custom table.
   *
   * @param name The name of the column.
   * @param dataType The data type of the column.
   */
  private case class Column(name: String, dataType: CustomTable.DataType.Value)
}
