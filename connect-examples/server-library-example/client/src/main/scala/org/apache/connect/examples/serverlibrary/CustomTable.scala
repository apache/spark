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
import org.apache.spark.sql.{functions, Column, DataFrame, Dataset, Row, SparkSession}

import org.apache.connect.examples.serverlibrary.proto
import org.apache.connect.examples.serverlibrary.proto.CreateTable.Column.{DataType => ProtoDataType}

/**
 * Represents a custom table with associated DataFrame and metadata.
 *
 * @param df    The underlying DataFrame.
 * @param table The metadata of the custom table.
 */
class CustomTable private (private val df: Dataset[Row], private val table: proto.CustomTable) {

  /**
   * Returns the Spark session associated with the DataFrame.
   */
  private def spark = df.sparkSession

  /**
   * Converts the custom table to a DataFrame.
   *
   * @return The underlying DataFrame.
   */
  def toDF: Dataset[Row] = df

  /**
   * Prints the execution plan of the custom table.
   */
  def explain(): Unit = {
    println(s"Explaining plan for custom table: ${table.getName} with path: ${table.getPath}")
    df.explain("extended")
  }

  /**
   * Clones the custom table to a new location with a new name.
   *
   * @param target  The target path for the cloned table.
   * @param newName The new name for the cloned table.
   * @param replace Whether to replace the target location if it exists.
   * @return A new `CustomTable` instance representing the cloned table.
   */
  def clone(target: String, newName: String, replace: Boolean): CustomTable = {
    val cloneTableProto = proto.CloneTable
      .newBuilder()
      .setTable(
        proto.CustomTable
          .newBuilder()
          .setName(table.getName)
          .setPath(table.getPath))
      .setClone(
        proto.CustomTable
          .newBuilder()
          .setName(newName)
          .setPath(target))
      .setReplace(replace)
      .build()
    val customCommand = proto.CustomCommand
      .newBuilder()
      .setCloneTable(cloneTableProto)
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
    CustomTable.from(spark, newName, target)
  }
}

object CustomTable {
  /**
   * Creates a `CustomTable` from the given Spark session, name, and path.
   *
   * @param spark The Spark session.
   * @param name  The name of the table.
   * @param path  The path of the table.
   * @return A new `CustomTable` instance.
   */
  def from(spark: SparkSession, name: String, path: String): CustomTable = {
    val table = proto.CustomTable
      .newBuilder()
      .setName(name)
      .setPath(path)
      .build()
    val relation = proto.CustomRelation
      .newBuilder()
      .setScan(proto.Scan.newBuilder().setTable(table))
      .build()
    val customRelation = Any.pack(relation)
    val df = spark.newDataFrame(f => f.setExtension(customRelation))
    new CustomTable(df, table)
  }

  /**
   * Creates a new `CustomTableBuilder` instance.
   *
   * @param spark The Spark session.
   * @return A new `CustomTableBuilder` instance.
   */
  def create(spark: SparkSession): CustomTableBuilder = new CustomTableBuilder(spark)

  /**
   * Enumeration for data types.
   */
  object DataType extends Enumeration {
    type DataType = Value
    val Int, String, Float, Boolean = Value

    /**
     * Converts a `DataType` to its corresponding `ProtoDataType`.
     *
     * @param dataType The data type to convert.
     * @return The corresponding `ProtoDataType`.
     */
    def toProto(dataType: DataType): ProtoDataType = {
      dataType match {
        case Int => ProtoDataType.INT
        case String => ProtoDataType.STRING
        case Float => ProtoDataType.FLOAT
        case Boolean => ProtoDataType.BOOLEAN
      }
    }
  }
}
