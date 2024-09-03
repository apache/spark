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

package org.apache.spark.sql.internal

import scala.jdk.CollectionConverters._

import org.apache.spark.annotation.Experimental
import org.apache.spark.connect.proto
import org.apache.spark.sql.{Column, DataFrameWriterV2, Dataset}

/**
 * Interface used to write a [[org.apache.spark.sql.Dataset]] to external storage using the v2
 * API.
 *
 * @since 3.4.0
 */
@Experimental
final class DataFrameWriterV2Impl[T] private[sql] (table: String, ds: Dataset[T])
    extends DataFrameWriterV2[T] {
  import ds.sparkSession.RichColumn

  private val builder = proto.WriteOperationV2
    .newBuilder()
    .setInput(ds.plan.getRoot)
    .setTableName(table)

  /** @inheritdoc */
  override def using(provider: String): this.type = {
    builder.setProvider(provider)
    this
  }

  /** @inheritdoc */
  override def option(key: String, value: String): this.type = {
    builder.putOptions(key, value)
    this
  }

  /** @inheritdoc */
  override def options(options: scala.collection.Map[String, String]): this.type = {
    builder.putAllOptions(options.asJava)
    this
  }

  /** @inheritdoc */
  override def options(options: java.util.Map[String, String]): this.type = {
    builder.putAllOptions(options)
    this
  }

  /** @inheritdoc */
  override def tableProperty(property: String, value: String): this.type = {
    builder.putTableProperties(property, value)
    this
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  override def partitionedBy(column: Column, columns: Column*): this.type = {
    builder.addAllPartitioningColumns((column +: columns).map(_.expr).asJava)
    this
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  override def clusterBy(colName: String, colNames: String*): this.type = {
    builder.addAllClusteringColumns((colName +: colNames).asJava)
    this
  }

  /** @inheritdoc */
  override def create(): Unit = {
    executeWriteOperation(proto.WriteOperationV2.Mode.MODE_CREATE)
  }

  /** @inheritdoc */
  override def replace(): Unit = {
    executeWriteOperation(proto.WriteOperationV2.Mode.MODE_REPLACE)
  }

  /** @inheritdoc */
  override def createOrReplace(): Unit = {
    executeWriteOperation(proto.WriteOperationV2.Mode.MODE_CREATE_OR_REPLACE)
  }

  /** @inheritdoc */
  def append(): Unit = {
    executeWriteOperation(proto.WriteOperationV2.Mode.MODE_APPEND)
  }

  /** @inheritdoc */
  def overwrite(condition: Column): Unit = {
    builder.setOverwriteCondition(condition.expr)
    executeWriteOperation(proto.WriteOperationV2.Mode.MODE_OVERWRITE)
  }

  /** @inheritdoc */
  def overwritePartitions(): Unit = {
    executeWriteOperation(proto.WriteOperationV2.Mode.MODE_OVERWRITE_PARTITIONS)
  }

  private def executeWriteOperation(mode: proto.WriteOperationV2.Mode): Unit = {
    val command = proto.Command
      .newBuilder()
      .setWriteOperationV2(builder.setMode(mode))
      .build()
    ds.sparkSession.execute(command)
  }
}
