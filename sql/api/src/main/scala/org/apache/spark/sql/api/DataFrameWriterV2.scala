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
package org.apache.spark.sql.api

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import _root_.java.util

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.{Column, CreateTableWriter}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException

/**
 * Interface used to write a [[org.apache.spark.sql.api.Dataset]] to external storage
 * using the v2 API.
 *
 * @since 3.0.0
 */
@Experimental
abstract class DataFrameWriterV2[T] extends CreateTableWriter[T] {
  protected var provider: Option[String] = None

  protected val options = new mutable.HashMap[String, String]()

  protected  val properties = new mutable.HashMap[String, String]()

  /** @inheritdoc */
  override def using(provider: String): this.type = {
    this.provider = Some(provider)
    this
  }

  /** @inheritdoc */
  override def option(key: String, value: String): this.type = {
    this.options.put(key, value)
    this
  }

  /** @inheritdoc */
  override def options(options: scala.collection.Map[String, String]): this.type = {
    options.foreach {
      case (key, value) =>
        this.options.put(key, value)
    }
    this
  }

  /** @inheritdoc */
  override def options(options: util.Map[String, String]): this.type = {
    this.options(options.asScala)
    this
  }

  /** @inheritdoc */
  override def tableProperty(property: String, value: String): this.type = {
    this.properties.put(property, value)
    this
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  override def partitionedBy(column: Column, columns: Column*): this.type

  /** @inheritdoc */
  @scala.annotation.varargs
  override def clusterBy(colName: String, colNames: String*): this.type

  /**
   * Append the contents of the data frame to the output table.
   *
   * If the output table does not exist, this operation will fail with
   * [[org.apache.spark.sql.catalyst.analysis.NoSuchTableException]]. The data frame will be
   * validated to ensure it is compatible with the existing table.
   *
   * @throws org.apache.spark.sql.catalyst.analysis.NoSuchTableException If the table does not exist
   */
  @throws(classOf[NoSuchTableException])
  def append(): Unit

  /**
   * Overwrite rows matching the given filter condition with the contents of the data frame in
   * the output table.
   *
   * If the output table does not exist, this operation will fail with
   * [[org.apache.spark.sql.catalyst.analysis.NoSuchTableException]].
   * The data frame will be validated to ensure it is compatible with the existing table.
   *
   * @throws org.apache.spark.sql.catalyst.analysis.NoSuchTableException If the table does not exist
   */
  @throws(classOf[NoSuchTableException])
  def overwrite(condition: Column): Unit

  /**
   * Overwrite all partition for which the data frame contains at least one row with the contents
   * of the data frame in the output table.
   *
   * This operation is equivalent to Hive's `INSERT OVERWRITE ... PARTITION`, which replaces
   * partitions dynamically depending on the contents of the data frame.
   *
   * If the output table does not exist, this operation will fail with
   * [[org.apache.spark.sql.catalyst.analysis.NoSuchTableException]]. The data frame will be
   * validated to ensure it is compatible with the existing table.
   *
   * @throws org.apache.spark.sql.catalyst.analysis.NoSuchTableException If the table does not exist
   */
  @throws(classOf[NoSuchTableException])
  def overwritePartitions(): Unit
}
