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

import java.util

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.analysis.{CannotReplaceMissingTableException, NoSuchTableException, TableAlreadyExistsException}

/**
 * Interface used to write a [[org.apache.spark.sql.api.Dataset]] to external storage using the v2
 * API.
 *
 * @since 3.0.0
 */
@Experimental
abstract class DataFrameWriterV2[T] extends CreateTableWriter[T] {

  /** @inheritdoc */
  override def using(provider: String): this.type

  /** @inheritdoc */
  override def option(key: String, value: Boolean): this.type = option(key, value.toString)

  /** @inheritdoc */
  override def option(key: String, value: Long): this.type = option(key, value.toString)

  /** @inheritdoc */
  override def option(key: String, value: Double): this.type = option(key, value.toString)

  /** @inheritdoc */
  override def option(key: String, value: String): this.type

  /** @inheritdoc */
  override def options(options: scala.collection.Map[String, String]): this.type

  /** @inheritdoc */
  override def options(options: util.Map[String, String]): this.type

  /** @inheritdoc */
  override def tableProperty(property: String, value: String): this.type

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
   * @throws org.apache.spark.sql.catalyst.analysis.NoSuchTableException
   *   If the table does not exist
   */
  @throws(classOf[NoSuchTableException])
  def append(): Unit

  /**
   * Overwrite rows matching the given filter condition with the contents of the data frame in the
   * output table.
   *
   * If the output table does not exist, this operation will fail with
   * [[org.apache.spark.sql.catalyst.analysis.NoSuchTableException]]. The data frame will be
   * validated to ensure it is compatible with the existing table.
   *
   * @throws org.apache.spark.sql.catalyst.analysis.NoSuchTableException
   *   If the table does not exist
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
   * @throws org.apache.spark.sql.catalyst.analysis.NoSuchTableException
   *   If the table does not exist
   */
  @throws(classOf[NoSuchTableException])
  def overwritePartitions(): Unit
}

/**
 * Configuration methods common to create/replace operations and insert/overwrite operations.
 * @tparam R
 *   builder type to return
 * @since 3.0.0
 */
trait WriteConfigMethods[R] {

  /**
   * Add a write option.
   *
   * @since 3.0.0
   */
  def option(key: String, value: String): R

  /**
   * Add a boolean output option.
   *
   * @since 3.0.0
   */
  def option(key: String, value: Boolean): R = option(key, value.toString)

  /**
   * Add a long output option.
   *
   * @since 3.0.0
   */
  def option(key: String, value: Long): R = option(key, value.toString)

  /**
   * Add a double output option.
   *
   * @since 3.0.0
   */
  def option(key: String, value: Double): R = option(key, value.toString)

  /**
   * Add write options from a Scala Map.
   *
   * @since 3.0.0
   */
  def options(options: scala.collection.Map[String, String]): R

  /**
   * Add write options from a Java Map.
   *
   * @since 3.0.0
   */
  def options(options: java.util.Map[String, String]): R
}

/**
 * Trait to restrict calls to create and replace operations.
 *
 * @since 3.0.0
 */
trait CreateTableWriter[T] extends WriteConfigMethods[CreateTableWriter[T]] {

  /**
   * Create a new table from the contents of the data frame.
   *
   * The new table's schema, partition layout, properties, and other configuration will be based
   * on the configuration set on this writer.
   *
   * If the output table exists, this operation will fail with
   * [[org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException]].
   *
   * @throws org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
   *   If the table already exists
   */
  @throws(classOf[TableAlreadyExistsException])
  def create(): Unit

  /**
   * Replace an existing table with the contents of the data frame.
   *
   * The existing table's schema, partition layout, properties, and other configuration will be
   * replaced with the contents of the data frame and the configuration set on this writer.
   *
   * If the output table does not exist, this operation will fail with
   * [[org.apache.spark.sql.catalyst.analysis.CannotReplaceMissingTableException]].
   *
   * @throws org.apache.spark.sql.catalyst.analysis.CannotReplaceMissingTableException
   *   If the table does not exist
   */
  @throws(classOf[CannotReplaceMissingTableException])
  def replace(): Unit

  /**
   * Create a new table or replace an existing table with the contents of the data frame.
   *
   * The output table's schema, partition layout, properties, and other configuration will be
   * based on the contents of the data frame and the configuration set on this writer. If the
   * table exists, its configuration and data will be replaced.
   */
  def createOrReplace(): Unit

  /**
   * Partition the output table created by `create`, `createOrReplace`, or `replace` using the
   * given columns or transforms.
   *
   * When specified, the table data will be stored by these values for efficient reads.
   *
   * For example, when a table is partitioned by day, it may be stored in a directory layout like:
   * <ul> <li>`table/day=2019-06-01/`</li> <li>`table/day=2019-06-02/`</li> </ul>
   *
   * Partitioning is one of the most widely used techniques to optimize physical data layout. It
   * provides a coarse-grained index for skipping unnecessary data reads when queries have
   * predicates on the partitioned columns. In order for partitioning to work well, the number of
   * distinct values in each column should typically be less than tens of thousands.
   *
   * @since 3.0.0
   */
  @scala.annotation.varargs
  def partitionedBy(column: Column, columns: Column*): CreateTableWriter[T]

  /**
   * Clusters the output by the given columns on the storage. The rows with matching values in the
   * specified clustering columns will be consolidated within the same group.
   *
   * For instance, if you cluster a dataset by date, the data sharing the same date will be stored
   * together in a file. This arrangement improves query efficiency when you apply selective
   * filters to these clustering columns, thanks to data skipping.
   *
   * @since 4.0.0
   */
  @scala.annotation.varargs
  def clusterBy(colName: String, colNames: String*): CreateTableWriter[T]

  /**
   * Specifies a provider for the underlying output data source. Spark's default catalog supports
   * "parquet", "json", etc.
   *
   * @since 3.0.0
   */
  def using(provider: String): CreateTableWriter[T]

  /**
   * Add a table property.
   */
  def tableProperty(property: String, value: String): CreateTableWriter[T]
}
