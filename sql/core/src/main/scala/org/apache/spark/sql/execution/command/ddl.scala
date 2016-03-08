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

import org.apache.spark.Logging
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.datasources.BucketSpec
import org.apache.spark.sql.types._


/**
 * A DDL command expected to be run in the underlying system without Spark parsing the
 * query text.
 */
abstract class NativeDDLCommands(val sql: String) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    sqlContext.runNativeSql(sql)
  }

  override val output: Seq[Attribute] = {
    Seq(AttributeReference("result", StringType, nullable = false)())
  }

}

case class CreateDatabase(
    databaseName: String,
    allowExisting: Boolean,
    path: Option[String],
    comment: Option[String],
    props: Map[String, String])(sql: String)
  extends NativeDDLCommands(sql) with Logging

case class CreateFunction(
    functionName: String,
    alias: String,
    resourcesMap: Map[String, String],
    isTemp: Boolean)(sql: String)
  extends NativeDDLCommands(sql) with Logging

case class AlterTableRename(
    tableName: TableIdentifier,
    renameTableName: TableIdentifier)(sql: String)
  extends NativeDDLCommands(sql) with Logging

case class AlterTableSetProperties(
    tableName: TableIdentifier,
    setProperties: Map[String, Option[String]])(sql: String)
  extends NativeDDLCommands(sql) with Logging

case class AlterTableDropProperties(
    tableName: TableIdentifier,
    dropProperties: Map[String, Option[String]],
    allowExisting: Boolean)(sql: String)
  extends NativeDDLCommands(sql) with Logging

case class AlterTableSerDeProperties(
    tableName: TableIdentifier,
    serdeClassName: Option[String],
    serdeProperties: Option[Map[String, Option[String]]],
    partition: Option[Map[String, Option[String]]])(sql: String)
  extends NativeDDLCommands(sql) with Logging

case class AlterTableStoreProperties(
    tableName: TableIdentifier,
    buckets: Option[BucketSpec],
    noClustered: Boolean,
    noSorted: Boolean)(sql: String)
  extends NativeDDLCommands(sql) with Logging

case class AlterTableSkewed(
    tableName: TableIdentifier,
    skewedCols: Seq[String],
    skewedValues: Seq[Seq[String]],
    storedAsDirs: Boolean,
    notSkewed: Boolean,
    // TODO: what??
    notStoredAsDirs: Boolean)(sql: String)
  extends NativeDDLCommands(sql) with Logging

case class AlterTableSkewedLocation(
    tableName: TableIdentifier,
    skewedMap: Map[Seq[String], String])(sql: String)
  extends NativeDDLCommands(sql) with Logging

case class AlterTableAddPartition(
    tableName: TableIdentifier,
    partitionsAndLocs: Seq[(Map[String, Option[String]], Option[String])],
    allowExisting: Boolean)(sql: String)
  extends NativeDDLCommands(sql) with Logging

case class AlterTableRenamePartition(
    tableName: TableIdentifier,
    oldPartition: Map[String, Option[String]],
    newPartition: Map[String, Option[String]])(sql: String)
  extends NativeDDLCommands(sql) with Logging

case class AlterTableExchangePartition(
    tableName: TableIdentifier,
    fromTableName: TableIdentifier,
    partition: Map[String, Option[String]])(sql: String)
  extends NativeDDLCommands(sql) with Logging

case class AlterTableDropPartition(
    tableName: TableIdentifier,
    partitions: Seq[Seq[(String, String, String)]],
    allowExisting: Boolean,
    purge: Boolean,
    replication: Option[(String, Boolean)])(sql: String)
  extends NativeDDLCommands(sql) with Logging

case class AlterTableArchivePartition(
    tableName: TableIdentifier,
    partition: Map[String, Option[String]])(sql: String)
  extends NativeDDLCommands(sql) with Logging

case class AlterTableUnarchivePartition(
    tableName: TableIdentifier,
    partition: Map[String, Option[String]])(sql: String)
  extends NativeDDLCommands(sql) with Logging

case class AlterTableSetFileFormat(
    tableName: TableIdentifier,
    partition: Option[Map[String, Option[String]]],
    fileFormat: Option[Seq[String]],
    genericFormat: Option[String])(sql: String)
  extends NativeDDLCommands(sql) with Logging

case class AlterTableSetLocation(
    tableName: TableIdentifier,
    partition: Option[Map[String, Option[String]]],
    location: String)(sql: String)
  extends NativeDDLCommands(sql) with Logging

case class AlterTableTouch(
    tableName: TableIdentifier,
    partition: Option[Map[String, Option[String]]])(sql: String)
  extends NativeDDLCommands(sql) with Logging

case class AlterTableCompact(
    tableName: TableIdentifier,
    partition: Option[Map[String, Option[String]]],
    compactType: String)(sql: String)
  extends NativeDDLCommands(sql) with Logging

case class AlterTableMerge(
    tableName: TableIdentifier,
    partition: Option[Map[String, Option[String]]])(sql: String)
  extends NativeDDLCommands(sql) with Logging

case class AlterTableChangeCol(
    tableName: TableIdentifier,
    partition: Option[Map[String, Option[String]]],
    oldColName: String,
    newColName: String,
    dataType: DataType,
    comment: Option[String],
    afterPos: Boolean,
    afterPosCol: Option[String],
    restrict: Boolean,
    cascade: Boolean)(sql: String)
  extends NativeDDLCommands(sql) with Logging

case class AlterTableAddCol(
    tableName: TableIdentifier,
    partition: Option[Map[String, Option[String]]],
    columns: StructType,
    restrict: Boolean,
    cascade: Boolean)(sql: String)
  extends NativeDDLCommands(sql) with Logging

case class AlterTableReplaceCol(
    tableName: TableIdentifier,
    partition: Option[Map[String, Option[String]]],
    columns: StructType,
    restrict: Boolean,
    cascade: Boolean)(sql: String)
  extends NativeDDLCommands(sql) with Logging
