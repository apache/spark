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

package org.apache.spark.sql.execution.command

import java.util.NoSuchElementException

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}


/**
 * Command that runs
 * {{{
 *   set key = value;
 *   set -v;
 *   set;
 * }}}
 */
case class SetCommand(kv: Option[(String, Option[String])]) extends RunnableCommand with Logging {

  private def keyValueOutput: Seq[Attribute] = {
    val schema = StructType(
      StructField("key", StringType, nullable = false) ::
        StructField("value", StringType, nullable = false) :: Nil)
    schema.toAttributes
  }

  private val (_output, runFunc): (Seq[Attribute], SQLContext => Seq[Row]) = kv match {
    // Configures the deprecated "mapred.reduce.tasks" property.
    case Some((SQLConf.Deprecated.MAPRED_REDUCE_TASKS, Some(value))) =>
      val runFunc = (sqlContext: SQLContext) => {
        logWarning(
          s"Property ${SQLConf.Deprecated.MAPRED_REDUCE_TASKS} is deprecated, " +
            s"automatically converted to ${SQLConf.SHUFFLE_PARTITIONS.key} instead.")
        if (value.toInt < 1) {
          val msg =
            s"Setting negative ${SQLConf.Deprecated.MAPRED_REDUCE_TASKS} for automatically " +
              "determining the number of reducers is not supported."
          throw new IllegalArgumentException(msg)
        } else {
          sqlContext.setConf(SQLConf.SHUFFLE_PARTITIONS.key, value)
          Seq(Row(SQLConf.SHUFFLE_PARTITIONS.key, value))
        }
      }
      (keyValueOutput, runFunc)

    case Some((SQLConf.Deprecated.EXTERNAL_SORT, Some(value))) =>
      val runFunc = (sqlContext: SQLContext) => {
        logWarning(
          s"Property ${SQLConf.Deprecated.EXTERNAL_SORT} is deprecated and will be ignored. " +
            s"External sort will continue to be used.")
        Seq(Row(SQLConf.Deprecated.EXTERNAL_SORT, "true"))
      }
      (keyValueOutput, runFunc)

    case Some((SQLConf.Deprecated.USE_SQL_AGGREGATE2, Some(value))) =>
      val runFunc = (sqlContext: SQLContext) => {
        logWarning(
          s"Property ${SQLConf.Deprecated.USE_SQL_AGGREGATE2} is deprecated and " +
            s"will be ignored. ${SQLConf.Deprecated.USE_SQL_AGGREGATE2} will " +
            s"continue to be true.")
        Seq(Row(SQLConf.Deprecated.USE_SQL_AGGREGATE2, "true"))
      }
      (keyValueOutput, runFunc)

    case Some((SQLConf.Deprecated.TUNGSTEN_ENABLED, Some(value))) =>
      val runFunc = (sqlContext: SQLContext) => {
        logWarning(
          s"Property ${SQLConf.Deprecated.TUNGSTEN_ENABLED} is deprecated and " +
            s"will be ignored. Tungsten will continue to be used.")
        Seq(Row(SQLConf.Deprecated.TUNGSTEN_ENABLED, "true"))
      }
      (keyValueOutput, runFunc)

    case Some((SQLConf.Deprecated.CODEGEN_ENABLED, Some(value))) =>
      val runFunc = (sqlContext: SQLContext) => {
        logWarning(
          s"Property ${SQLConf.Deprecated.CODEGEN_ENABLED} is deprecated and " +
            s"will be ignored. Codegen will continue to be used.")
        Seq(Row(SQLConf.Deprecated.CODEGEN_ENABLED, "true"))
      }
      (keyValueOutput, runFunc)

    case Some((SQLConf.Deprecated.UNSAFE_ENABLED, Some(value))) =>
      val runFunc = (sqlContext: SQLContext) => {
        logWarning(
          s"Property ${SQLConf.Deprecated.UNSAFE_ENABLED} is deprecated and " +
            s"will be ignored. Unsafe mode will continue to be used.")
        Seq(Row(SQLConf.Deprecated.UNSAFE_ENABLED, "true"))
      }
      (keyValueOutput, runFunc)

    case Some((SQLConf.Deprecated.SORTMERGE_JOIN, Some(value))) =>
      val runFunc = (sqlContext: SQLContext) => {
        logWarning(
          s"Property ${SQLConf.Deprecated.SORTMERGE_JOIN} is deprecated and " +
            s"will be ignored. Sort merge join will continue to be used.")
        Seq(Row(SQLConf.Deprecated.SORTMERGE_JOIN, "true"))
      }
      (keyValueOutput, runFunc)

    case Some((SQLConf.Deprecated.PARQUET_UNSAFE_ROW_RECORD_READER_ENABLED, Some(value))) =>
      val runFunc = (sqlContext: SQLContext) => {
        logWarning(
          s"Property ${SQLConf.Deprecated.PARQUET_UNSAFE_ROW_RECORD_READER_ENABLED} is " +
            s"deprecated and will be ignored. Vectorized parquet reader will be used instead.")
        Seq(Row(SQLConf.PARQUET_VECTORIZED_READER_ENABLED, "true"))
      }
      (keyValueOutput, runFunc)

    // Configures a single property.
    case Some((key, Some(value))) =>
      val runFunc = (sqlContext: SQLContext) => {
        sqlContext.setConf(key, value)
        Seq(Row(key, value))
      }
      (keyValueOutput, runFunc)

    // (In Hive, "SET" returns all changed properties while "SET -v" returns all properties.)
    // Queries all key-value pairs that are set in the SQLConf of the sqlContext.
    case None =>
      val runFunc = (sqlContext: SQLContext) => {
        sqlContext.getAllConfs.map { case (k, v) => Row(k, v) }.toSeq
      }
      (keyValueOutput, runFunc)

    // Queries all properties along with their default values and docs that are defined in the
    // SQLConf of the sqlContext.
    case Some(("-v", None)) =>
      val runFunc = (sqlContext: SQLContext) => {
        sqlContext.conf.getAllDefinedConfs.map { case (key, defaultValue, doc) =>
          Row(key, defaultValue, doc)
        }
      }
      val schema = StructType(
        StructField("key", StringType, nullable = false) ::
          StructField("default", StringType, nullable = false) ::
          StructField("meaning", StringType, nullable = false) :: Nil)
      (schema.toAttributes, runFunc)

    // Queries the deprecated "mapred.reduce.tasks" property.
    case Some((SQLConf.Deprecated.MAPRED_REDUCE_TASKS, None)) =>
      val runFunc = (sqlContext: SQLContext) => {
        logWarning(
          s"Property ${SQLConf.Deprecated.MAPRED_REDUCE_TASKS} is deprecated, " +
            s"showing ${SQLConf.SHUFFLE_PARTITIONS.key} instead.")
        Seq(Row(SQLConf.SHUFFLE_PARTITIONS.key, sqlContext.conf.numShufflePartitions.toString))
      }
      (keyValueOutput, runFunc)

    // Queries a single property.
    case Some((key, None)) =>
      val runFunc = (sqlContext: SQLContext) => {
        val value =
          try sqlContext.getConf(key) catch {
            case _: NoSuchElementException => "<undefined>"
          }
        Seq(Row(key, value))
      }
      (keyValueOutput, runFunc)
  }

  override val output: Seq[Attribute] = _output

  override def run(sqlContext: SQLContext): Seq[Row] = runFunc(sqlContext)

}
