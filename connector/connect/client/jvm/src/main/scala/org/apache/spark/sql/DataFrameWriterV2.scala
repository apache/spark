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

package org.apache.spark.sql

import scala.jdk.CollectionConverters._

import org.apache.spark.annotation.Experimental
import org.apache.spark.connect.proto

/**
 * Interface used to write a [[org.apache.spark.sql.Dataset]] to external storage using the v2
 * API.
 *
 * @since 3.4.0
 */
@Experimental
final class DataFrameWriterV2[T] private[sql] (table: String, ds: Dataset[T])
    extends api.DataFrameWriterV2[T] {
  import ds.sparkSession.RichColumn

  private var partitioning: Option[Seq[proto.Expression]] = None

  private var clustering: Option[Seq[String]] = None

  private var overwriteCondition: Option[proto.Expression] = None

  /** @inheritdoc */
  override def using(provider: String): this.type = super.using(provider)

  /** @inheritdoc */
  override def option(key: String, value: String): this.type =
    super.option(key, value)

  /** @inheritdoc */
  override def options(options: scala.collection.Map[String, String]): this.type =
    super.options(options)

  /** @inheritdoc */
  override def options(options: java.util.Map[String, String]): this.type =
    super.options(options)

  /** @inheritdoc */
  override def tableProperty(property: String, value: String): this.type =
    super.tableProperty(property, value)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def partitionedBy(column: Column, columns: Column*): this.type = {
    val asTransforms = (column +: columns).map(_.expr)
    this.partitioning = Some(asTransforms)
    this
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  override def clusterBy(colName: String, colNames: String*): this.type = {
    this.clustering = Some(colName +: colNames)
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
    overwriteCondition = Some(condition.expr)
    executeWriteOperation(proto.WriteOperationV2.Mode.MODE_OVERWRITE)
  }

  /** @inheritdoc */
  def overwritePartitions(): Unit = {
    executeWriteOperation(proto.WriteOperationV2.Mode.MODE_OVERWRITE_PARTITIONS)
  }

  private def executeWriteOperation(mode: proto.WriteOperationV2.Mode): Unit = {
    val builder = proto.WriteOperationV2.newBuilder()

    builder.setInput(ds.plan.getRoot)
    builder.setTableName(table)
    provider.foreach(builder.setProvider)

    partitioning.foreach(columns => builder.addAllPartitioningColumns(columns.asJava))
    clustering.foreach(columns => builder.addAllClusteringColumns(columns.asJava))

    options.foreach { case (k, v) =>
      builder.putOptions(k, v)
    }
    properties.foreach { case (k, v) =>
      builder.putTableProperties(k, v)
    }

    builder.setMode(mode)

    overwriteCondition.foreach(builder.setOverwriteCondition)

    ds.sparkSession.execute(proto.Command.newBuilder().setWriteOperationV2(builder).build())
  }
}
