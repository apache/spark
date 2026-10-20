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

import org.apache.spark.annotation.Experimental

/**
 * Builds a BIN BY operation on a Dataset.
 *
 * @since 4.4.0
 */
@Experimental
final class BinByBuilder private[sql] (dataset: Dataset[_]) {

  /**
   * Sets the timestamp columns for each row's half-open range. Both columns must have the same
   * timestamp type.
   *
   * @since 4.4.0
   */
  def range(rangeStart: Column, rangeEnd: Column): BinByWithRange = {
    new BinByWithRange(dataset = dataset, rangeStart = rangeStart, rangeEnd = rangeEnd)
  }
}

/**
 * A BIN BY builder with its range columns set.
 *
 * @since 4.4.0
 */
@Experimental
final class BinByWithRange private[sql] (
    dataset: Dataset[_],
    rangeStart: Column,
    rangeEnd: Column) {

  /**
   * Sets the bin width to a positive, constant day-time interval expression.
   *
   * @since 4.4.0
   */
  def binWidth(width: Column): BinByWithWidth = {
    new BinByWithWidth(
      dataset,
      BinBySpec(rangeStart = rangeStart, rangeEnd = rangeEnd, binWidth = width))
  }
}

/**
 * A BIN BY builder with its range and bin width set.
 *
 * @since 4.4.0
 */
@Experimental
final class BinByWithWidth private[sql] (dataset: Dataset[_], spec: BinBySpec) {

  /**
   * Sets a constant alignment origin with the same timestamp type as the range columns. The
   * default origin is 1970-01-01 00:00:00, in the session time zone for TIMESTAMP.
   *
   * @since 4.4.0
   */
  def alignTo(origin: Column): BinByWithWidth = {
    new BinByWithWidth(dataset, spec.copy(origin = Some(origin)))
  }

  /**
   * Sets the FLOAT or DOUBLE columns to scale by each bin's overlap ratio. At least one column is
   * required.
   *
   * @since 4.4.0
   */
  @scala.annotation.varargs
  def distributeUniform(cols: Column*): BinByWithDistribute = {
    new BinByWithDistribute(dataset, spec.copy(distributeColumns = cols.toVector))
  }
}

/**
 * A BIN BY builder that can produce a DataFrame, with optional output column names. Each rename
 * returns an independent builder.
 *
 * @since 4.4.0
 */
@Experimental
final class BinByWithDistribute private[sql] (dataset: Dataset[_], spec: BinBySpec) {

  /**
   * Sets the name of the bin-start column. The default is `bin_start`.
   *
   * @since 4.4.0
   */
  def renameBinStart(name: String): BinByWithDistribute = {
    require(name != null, "Column name cannot be null")

    new BinByWithDistribute(dataset, spec.copy(binStartName = Some(name)))
  }

  /**
   * Sets the name of the bin-end column. The default is `bin_end`.
   *
   * @since 4.4.0
   */
  def renameBinEnd(name: String): BinByWithDistribute = {
    require(name != null, "Column name cannot be null")

    new BinByWithDistribute(dataset, spec.copy(binEndName = Some(name)))
  }

  /**
   * Sets the name of the overlap-ratio column. The default is `bin_distribute_ratio`.
   *
   * @since 4.4.0
   */
  def renameBinDistributeRatio(name: String): BinByWithDistribute = {
    require(name != null, "Column name cannot be null")

    new BinByWithDistribute(dataset, spec.copy(binDistributeRatioName = Some(name)))
  }

  /**
   * Returns the DataFrame with one row for each bin that overlaps an input range.
   *
   * @since 4.4.0
   */
  def build(): DataFrame = dataset.binBy(spec)
}

private[sql] case class BinBySpec(
    rangeStart: Column,
    rangeEnd: Column,
    binWidth: Column,
    origin: Option[Column] = None,
    distributeColumns: Seq[Column] = Seq.empty,
    binStartName: Option[String] = None,
    binEndName: Option[String] = None,
    binDistributeRatioName: Option[String] = None)
