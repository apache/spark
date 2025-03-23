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

import scala.collection.JavaConverters._

import com.google.protobuf.Any
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connect.plugin.CommandPlugin
import org.apache.spark.sql.types.{StringType, IntegerType, FloatType, DoubleType, BooleanType, LongType, StructType, StructField, DataType}

import org.apache.connect.examples.serverlibrary.CustomTable
import org.apache.connect.examples.serverlibrary.proto
import org.apache.connect.examples.serverlibrary.proto.CreateTable.Column.{DataType => ProtoDataType}

/**
 * Commands are distinct actions that can be executed. Unlike relations, which focus on the
 * transformation and nesting of output data, commands represent singular operations that perform
 * specific tasks on the data.
 * In this example, the `CustomCommandPlugin` handles operations related to creating and duplicating
 * custom tables.
 */
class CustomCommandPlugin extends CommandPlugin with CustomPluginBase {

  /**
   * Processes the raw byte array containing the command.
   *
   * @param raw The raw byte array of the command.
   * @param planner The SparkConnectPlanner instance.
   * @return True if the command was processed, false otherwise.
   */
  override def process(raw: Array[Byte], planner: SparkConnectPlanner): Boolean = {
    val command = Any.parseFrom(raw)
    if (command.is(classOf[proto.CustomCommand])) {
      processInternal(command.unpack(classOf[proto.CustomCommand]), planner)
      true
    } else {
      false
    }
  }

  /**
   * Processes the unpacked CustomCommand.
   *
   * @param command The unpacked CustomCommand.
   * @param planner The SparkConnectPlanner instance.
   */
  private def processInternal(
      command: proto.CustomCommand,
      planner: SparkConnectPlanner): Unit = {
    command.getCommandTypeCase match {
      case proto.CustomCommand.CommandTypeCase.CREATE_TABLE =>
        processCreateTable(planner, command.getCreateTable)
      case proto.CustomCommand.CommandTypeCase.CLONE_TABLE =>
        processCloneTable(planner, command.getCloneTable)
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported command type: ${command.getCommandTypeCase}")
    }
  }

  /**
   * Processes the CreateTable command.
   *
   * @param planner The SparkConnectPlanner instance.
   * @param createTable The CreateTable message.
   */
  private def processCreateTable(
      planner: SparkConnectPlanner,
      createTable: proto.CreateTable): Unit = {
    val tableName = createTable.getTable.getName
    val tablePath = createTable.getTable.getPath

    // Convert the list of columns from the protobuf message to a Spark schema
    val schema = StructType(createTable.getColumnsList.asScala.toSeq.map { column =>
      StructField(
        column.getName,
        protoDataTypeToSparkType(column.getDataType),
        nullable = true // Assuming all columns are nullable for simplicity
      )
    })

    // Create the table using the CustomTable utility
    CustomTable.createTable(tableName, tablePath, planner.session, schema)
  }

  /**
   * Converts a protobuf DataType to a Spark DataType.
   *
   * @param protoType The protobuf DataType.
   * @return The corresponding Spark DataType.
   */
  private def protoDataTypeToSparkType(protoType: ProtoDataType): DataType = {
    protoType match {
      case ProtoDataType.INT => IntegerType
      case ProtoDataType.STRING => StringType
      case ProtoDataType.FLOAT => FloatType
      case ProtoDataType.BOOLEAN => BooleanType
      case _ =>
        throw new IllegalArgumentException(s"Unsupported or unknown data type: ${protoType}")
    }
  }

  /**
   * Processes the CloneTable command.
   *
   * @param planner The SparkConnectPlanner instance.
   * @param msg The CloneTable message.
   */
  private def processCloneTable(planner: SparkConnectPlanner, msg: proto.CloneTable): Unit = {
    val sourceTable = getCustomTable(msg.getTable)
    CustomTable.cloneTable(
      sourceTable,
      msg.getClone.getName,
      msg.getClone.getPath,
      msg.getReplace)
  }
}
