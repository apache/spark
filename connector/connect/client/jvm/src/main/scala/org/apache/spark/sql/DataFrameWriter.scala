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

import org.apache.spark.annotation.Stable
import org.apache.spark.connect.proto

/**
 * Interface used to write a [[Dataset]] to external storage systems (e.g. file systems, key-value
 * stores, etc). Use `Dataset.write` to access this.
 *
 * @since 3.4.0
 */
@Stable
final class DataFrameWriter[T] private[sql] (ds: Dataset[T]) extends api.DataFrameWriter {

  /** @inheritdoc */
  override def mode(saveMode: SaveMode): this.type = super.mode(saveMode)

  /** @inheritdoc */
  override def mode(saveMode: String): this.type = super.mode(saveMode)

  /** @inheritdoc */
  override def format(source: String): this.type = super.format(source)

  /** @inheritdoc */
  override def option(key: String, value: String): this.type = super.option(key, value)

  /** @inheritdoc */
  override def option(key: String, value: Boolean): this.type = super.option(key, value)

  /** @inheritdoc */
  override def option(key: String, value: Long): this.type = super.option(key, value)

  /** @inheritdoc */
  override def option(key: String, value: Double): this.type = super.option(key, value)

  /** @inheritdoc */
  override def options(options: scala.collection.Map[String, String]): this.type =
    super.options(options)

  /** @inheritdoc */
  override def options(options: java.util.Map[String, String]): this.type =
    super.options(options)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def partitionBy(colNames: String*): this.type = super.partitionBy(colNames: _*)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def bucketBy(numBuckets: Int, colName: String, colNames: String*): this.type =
    super.bucketBy(numBuckets, colName, colNames: _*)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def sortBy(colName: String, colNames: String*): this.type =
    super.sortBy(colName, colNames: _*)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def clusterBy(colName: String, colNames: String*): this.type =
    super.clusterBy(colName, colNames: _*)

  /** @inheritdoc */
  def save(path: String): Unit = {
    saveInternal(Some(path))
  }

  /** @inheritdoc */
  def save(): Unit = saveInternal(None)

  private def saveInternal(path: Option[String]): Unit = {
    executeWriteOperation(builder => path.foreach(builder.setPath))
  }

  private def executeWriteOperation(f: proto.WriteOperation.Builder => Unit): Unit = {
    val builder = proto.WriteOperation.newBuilder()

    builder.setInput(ds.plan.getRoot)

    // Set path or table
    f(builder)

    // Cannot both be set
    require(!(builder.hasPath && builder.hasTable))

    builder.setMode(mode match {
      case SaveMode.Append => proto.WriteOperation.SaveMode.SAVE_MODE_APPEND
      case SaveMode.Overwrite => proto.WriteOperation.SaveMode.SAVE_MODE_OVERWRITE
      case SaveMode.Ignore => proto.WriteOperation.SaveMode.SAVE_MODE_IGNORE
      case SaveMode.ErrorIfExists => proto.WriteOperation.SaveMode.SAVE_MODE_ERROR_IF_EXISTS
    })

    if (source.nonEmpty) {
      builder.setSource(source)
    }
    sortColumnNames.foreach(names => builder.addAllSortColumnNames(names.asJava))
    partitioningColumns.foreach(cols => builder.addAllPartitioningColumns(cols.asJava))
    clusteringColumns.foreach(cols => builder.addAllClusteringColumns(cols.asJava))

    numBuckets.foreach(n => {
      val bucketBuilder = proto.WriteOperation.BucketBy.newBuilder()
      bucketBuilder.setNumBuckets(n)
      bucketColumnNames.foreach(names => bucketBuilder.addAllBucketColumnNames(names.asJava))
      builder.setBucketBy(bucketBuilder)
    })

    extraOptions.foreach { case (k, v) =>
      builder.putOptions(k, v)
    }

    ds.sparkSession.execute(proto.Command.newBuilder().setWriteOperation(builder).build())
  }

  /** @inheritdoc */
  def insertInto(tableName: String): Unit = {
    executeWriteOperation(builder => {
      builder.setTable(
        proto.WriteOperation.SaveTable
          .newBuilder()
          .setTableName(tableName)
          .setSaveMethod(
            proto.WriteOperation.SaveTable.TableSaveMethod.TABLE_SAVE_METHOD_INSERT_INTO))
    })
  }

  /** @inheritdoc */
  def saveAsTable(tableName: String): Unit = {
    executeWriteOperation(builder => {
      builder.setTable(
        proto.WriteOperation.SaveTable
          .newBuilder()
          .setTableName(tableName)
          .setSaveMethod(
            proto.WriteOperation.SaveTable.TableSaveMethod.TABLE_SAVE_METHOD_SAVE_AS_TABLE))
    })
  }
}
