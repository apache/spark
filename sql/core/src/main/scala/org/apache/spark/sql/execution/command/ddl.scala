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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.ExternalCatalog.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.datasources.BucketSpec
import org.apache.spark.sql.types._


// Note: The definition of these commands are based on the ones described in
// https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL

/**
 * A DDL command expected to be parsed and run in an underlying system instead of in Spark.
 */
abstract class NativeDDLCommand(val sql: String) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    sqlContext.runNativeSql(sql)
  }

  override val output: Seq[Attribute] = {
    Seq(AttributeReference("result", StringType, nullable = false)())
  }

}

case class CreateDatabase(
    databaseName: String,
    ifNotExists: Boolean,
    path: Option[String],
    comment: Option[String],
    props: Map[String, String])(sql: String)
  extends NativeDDLCommand(sql) with Logging

/**
 * Drop Database: Removes a database from the system.
 *
 * 'ifExists':
 * - true, if database_name does't exist, no action
 * - false (default), if database_name does't exist, an error message will be issued
 * 'restric':
 * - true (default), the database cannot be dropped if it is not empty. The inclusive
 * tables must be dropped at first.
 * - false, it is in the Cascade mode. The dependent objects are automatically dropped
 * before dropping database.
 */
case class DropDatabase(
    databaseName: String,
    ifExists: Boolean,
    restrict: Boolean)(sql: String)
  extends NativeDDLCommand(sql) with Logging

/** ALTER DATABASE: add new (key, value) pairs into DBPROPERTIES */
case class AlterDatabaseProperties(
    databaseName: String,
    props: Map[String, String])(sql: String)
  extends NativeDDLCommand(sql) with Logging

/**
 * DESCRIBE DATABASE: shows the name of the database, its comment (if one has been set), and its
 * root location on the filesystem. When extended is true, it also shows the database's properties
 */
case class DescribeDatabase(
    databaseName: String,
    extended: Boolean)(sql: String)
  extends NativeDDLCommand(sql) with Logging

case class CreateFunction(
    databaseName: Option[String],
    functionName: String,
    alias: String,
    resources: Seq[(String, String)],
    isTemp: Boolean)(sql: String)
  extends NativeDDLCommand(sql) with Logging

/**
 * The DDL command that drops a function.
 * ifExists: returns an error if the function doesn't exist, unless this is true.
 * isTemp: indicates if it is a temporary function.
 */
case class DropFunction(
    databaseName: Option[String],
    functionName: String,
    ifExists: Boolean,
    isTemp: Boolean)(sql: String)
  extends NativeDDLCommand(sql) with Logging

/** Rename in ALTER TABLE/VIEW: change the name of a table/view to a different name. */
case class AlterTableAlterViewRename(
    oldName: TableIdentifier,
    newName: TableIdentifier)(sql: String)
  extends NativeDDLCommand(sql) with Logging

/** Set Properties in ALTER TABLE/VIEW: add metadata to a table/view. */
case class AlterTableAlterViewSetProperties(
    tableName: TableIdentifier,
    properties: Map[String, String])(sql: String)
  extends NativeDDLCommand(sql) with Logging

/** Unset Properties in ALTER TABLE/VIEW: remove metadata from a table/view. */
case class AlterTableAlterViewUnsetProperties(
    tableName: TableIdentifier,
    properties: Map[String, String],
    ifExists: Boolean)(sql: String)
  extends NativeDDLCommand(sql) with Logging

case class AlterTableSerDeProperties(
    tableName: TableIdentifier,
    serdeClassName: Option[String],
    serdeProperties: Option[Map[String, String]],
    partition: Option[Map[String, String]])(sql: String)
  extends NativeDDLCommand(sql) with Logging

case class AlterTableStorageProperties(
    tableName: TableIdentifier,
    buckets: BucketSpec)(sql: String)
  extends NativeDDLCommand(sql) with Logging

case class AlterTableNotClustered(
    tableName: TableIdentifier)(sql: String) extends NativeDDLCommand(sql) with Logging

case class AlterTableNotSorted(
    tableName: TableIdentifier)(sql: String) extends NativeDDLCommand(sql) with Logging

case class AlterTableSkewed(
    tableName: TableIdentifier,
    // e.g. (dt, country)
    skewedCols: Seq[String],
    // e.g. ('2008-08-08', 'us), ('2009-09-09', 'uk')
    skewedValues: Seq[Seq[String]],
    storedAsDirs: Boolean)(sql: String)
  extends NativeDDLCommand(sql) with Logging {

  require(skewedValues.forall(_.size == skewedCols.size),
    "number of columns in skewed values do not match number of skewed columns provided")
}

case class AlterTableNotSkewed(
    tableName: TableIdentifier)(sql: String) extends NativeDDLCommand(sql) with Logging

case class AlterTableNotStoredAsDirs(
    tableName: TableIdentifier)(sql: String) extends NativeDDLCommand(sql) with Logging

case class AlterTableSkewedLocation(
    tableName: TableIdentifier,
    skewedMap: Map[String, String])(sql: String)
  extends NativeDDLCommand(sql) with Logging

/**
 * Add Partition in ALTER TABLE/VIEW: add the table/view partitions.
 * 'partitionSpecsAndLocs': the syntax of ALTER VIEW is identical to ALTER TABLE,
 * EXCEPT that it is ILLEGAL to specify a LOCATION clause.
 * An error message will be issued if the partition exists, unless 'ifNotExists' is false.
 */
case class AlterTableAlterViewAddPartition(
    tableName: TableIdentifier,
    partitionSpecsAndLocs: Seq[(TablePartitionSpec, Option[String])],
    ifNotExists: Boolean)(sql: String)
  extends NativeDDLCommand(sql) with Logging

case class AlterTableRenamePartition(
    tableName: TableIdentifier,
    oldPartition: TablePartitionSpec,
    newPartition: TablePartitionSpec)(sql: String)
  extends NativeDDLCommand(sql) with Logging

case class AlterTableExchangePartition(
    fromTableName: TableIdentifier,
    toTableName: TableIdentifier,
    spec: TablePartitionSpec)(sql: String)
  extends NativeDDLCommand(sql) with Logging

/**
 * Drop Partition in ALTER TABLE: to drop a particular partition for a table.
 * This removes the data and metadata for this partition.
 * The data is actually moved to the .Trash/Current directory if Trash is configured,
 * unless 'purge' is true, but the metadata is completely lost.
 * An error message will be issued if the partition does not exist, unless 'ifExists' is false.
 */
case class AlterTableDropPartition(
    tableName: TableIdentifier,
    specs: Seq[TablePartitionSpec],
    ifExists: Boolean,
    purge: Boolean)(sql: String)
  extends NativeDDLCommand(sql) with Logging

/**
 * Drop Partition in ALTER VIEW: drop the related partition metadata for a view.
 * An error message will be issued if the partition does not exist, unless 'ifExists' is false.
 */
case class AlterViewDropPartition(
    viewName: TableIdentifier,
    specs: Seq[TablePartitionSpec],
    ifExists: Boolean)(sql: String)
  extends NativeDDLCommand(sql) with Logging

case class AlterTableArchivePartition(
    tableName: TableIdentifier,
    spec: TablePartitionSpec)(sql: String)
  extends NativeDDLCommand(sql) with Logging

case class AlterTableUnarchivePartition(
    tableName: TableIdentifier,
    spec: TablePartitionSpec)(sql: String)
  extends NativeDDLCommand(sql) with Logging

case class AlterTableSetFileFormat(
    tableName: TableIdentifier,
    partitionSpec: Option[TablePartitionSpec],
    fileFormat: Seq[String],
    genericFormat: Option[String])(sql: String)
  extends NativeDDLCommand(sql) with Logging

case class AlterTableSetLocation(
    tableName: TableIdentifier,
    partitionSpec: Option[TablePartitionSpec],
    location: String)(sql: String)
  extends NativeDDLCommand(sql) with Logging

case class AlterTableTouch(
    tableName: TableIdentifier,
    partitionSpec: Option[TablePartitionSpec])(sql: String)
  extends NativeDDLCommand(sql) with Logging

case class AlterTableCompact(
    tableName: TableIdentifier,
    partitionSpec: Option[TablePartitionSpec],
    compactType: String)(sql: String)
  extends NativeDDLCommand(sql) with Logging

case class AlterTableMerge(
    tableName: TableIdentifier,
    partitionSpec: Option[TablePartitionSpec])(sql: String)
  extends NativeDDLCommand(sql) with Logging

case class AlterTableChangeCol(
    tableName: TableIdentifier,
    partitionSpec: Option[TablePartitionSpec],
    oldColName: String,
    newColName: String,
    dataType: DataType,
    comment: Option[String],
    afterColName: Option[String],
    restrict: Boolean,
    cascade: Boolean)(sql: String)
  extends NativeDDLCommand(sql) with Logging

case class AlterTableAddCol(
    tableName: TableIdentifier,
    partitionSpec: Option[TablePartitionSpec],
    columns: StructType,
    restrict: Boolean,
    cascade: Boolean)(sql: String)
  extends NativeDDLCommand(sql) with Logging

case class AlterTableReplaceCol(
    tableName: TableIdentifier,
    partitionSpec: Option[TablePartitionSpec],
    columns: StructType,
    restrict: Boolean,
    cascade: Boolean)(sql: String)
  extends NativeDDLCommand(sql) with Logging
