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

package org.example

import com.google.protobuf.Any
import org.apache.spark.connect.proto.Command
import org.example.proto
import org.example.proto.CreateTable.Column.{DataType => ProtoDataType}
import org.apache.spark.sql.{functions, Column, DataFrame, Dataset, Row, SparkSession}

class CustomTable private (private val df: Dataset[Row], private val table: proto.CustomTable) {

  private def spark = df.sparkSession

  def toDF: Dataset[Row] = df

  def explain(): Unit = {
    println(s"Explaning plan for custom table: ${table.getName} with path: ${table.getPath}")
    df.explain("extended")
  }

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

  def create(spark: SparkSession): CustomTableBuilder = new CustomTableBuilder(spark)

  object DataType extends Enumeration {
    type DataType = Value
    val Int, String, Float, Boolean = Value

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
