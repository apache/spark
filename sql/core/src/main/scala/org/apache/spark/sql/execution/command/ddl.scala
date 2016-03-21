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

case class CreateFunction(
    functionName: String,
    alias: String,
    resources: Seq[(String, String)],
    isTemp: Boolean)(sql: String)
  extends NativeDDLCommand(sql) with Logging

case class AlterTableRename(
    oldName: TableIdentifier,
    newName: TableIdentifier)(sql: String)
  extends NativeDDLCommand(sql) with Logging

case class AlterTableSetProperties(
    tableName: TableIdentifier,
    properties: Map[String, String])(sql: String)
  extends NativeDDLCommand(sql) with Logging

case class AlterTableUnsetProperties(
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

case class AlterTableAddPartition(
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

case class AlterTableDropPartition(
    tableName: TableIdentifier,
    specs: Seq[TablePartitionSpec],
    ifExists: Boolean,
    purge: Boolean)(sql: String)
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
