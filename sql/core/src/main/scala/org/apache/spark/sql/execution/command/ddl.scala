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
import org.apache.spark.sql.{AnalysisException, Row, SQLContext}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogDatabase
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

/**
 * A command for users to create a new database.
 *
 * It will issue an error message when the database with the same name already exists,
 * unless 'ifNotExists' is true.
 * The syntax of using this command in SQL is:
 * {{{
 *    CREATE DATABASE|SCHEMA [IF NOT EXISTS] database_name
 * }}}
 */
case class CreateDatabase(
    databaseName: String,
    ifNotExists: Boolean,
    path: Option[String],
    comment: Option[String],
    props: Map[String, String])
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val catalog = sqlContext.sessionState.catalog
    catalog.createDatabase(
      CatalogDatabase(
        databaseName,
        comment.getOrElse(""),
        path.getOrElse(catalog.getDefaultDBPath(databaseName)),
        props),
      ifNotExists)
    Seq.empty[Row]
  }

  override val output: Seq[Attribute] = Seq.empty
}


/**
 * A command for users to remove a database from the system.
 *
 * 'ifExists':
 * - true, if database_name does't exist, no action
 * - false (default), if database_name does't exist, a warning message will be issued
 * 'cascade':
 * - true, the dependent objects are automatically dropped before dropping database.
 * - false (default), it is in the Restrict mode. The database cannot be dropped if
 * it is not empty. The inclusive tables must be dropped at first.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *    DROP DATABASE [IF EXISTS] database_name [RESTRICT|CASCADE];
 * }}}
 */
case class DropDatabase(
    databaseName: String,
    ifExists: Boolean,
    cascade: Boolean)
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    sqlContext.sessionState.catalog.dropDatabase(databaseName, ifExists, cascade)
    Seq.empty[Row]
  }

  override val output: Seq[Attribute] = Seq.empty
}

/**
 * A command for users to add new (key, value) pairs into DBPROPERTIES
 * If the database does not exist, an error message will be issued to indicate the database
 * does not exist.
 * The syntax of using this command in SQL is:
 * {{{
 *    ALTER (DATABASE|SCHEMA) database_name SET DBPROPERTIES (property_name=property_value, ...)
 * }}}
 */
case class AlterDatabaseProperties(
    databaseName: String,
    props: Map[String, String])
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val catalog = sqlContext.sessionState.catalog
    val db: CatalogDatabase = catalog.getDatabase(databaseName)
    catalog.alterDatabase(db.copy(properties = db.properties ++ props))

    Seq.empty[Row]
  }

  override val output: Seq[Attribute] = Seq.empty
}

/**
 * A command for users to show the name of the database, its comment (if one has been set), and its
 * root location on the filesystem. When extended is true, it also shows the database's properties
 * If the database does not exist, an error message will be issued to indicate the database
 * does not exist.
 * The syntax of using this command in SQL is
 * {{{
 *    DESCRIBE DATABASE [EXTENDED] db_name
 * }}}
 */
case class DescribeDatabase(
    databaseName: String,
    extended: Boolean)
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val dbMetadata: CatalogDatabase = sqlContext.sessionState.catalog.getDatabase(databaseName)
    val result =
      Row("Database Name", dbMetadata.name) ::
        Row("Description", dbMetadata.description) ::
        Row("Location", dbMetadata.locationUri) :: Nil

    if (extended) {
      val properties =
        if (dbMetadata.properties.isEmpty) {
          ""
        } else {
          dbMetadata.properties.toSeq.mkString("(", ", ", ")")
        }
      result :+ Row("Properties", properties)
    } else {
      result
    }
  }

  override val output: Seq[Attribute] = {
    AttributeReference("database_description_item", StringType, nullable = false)() ::
      AttributeReference("database_description_value", StringType, nullable = false)() :: Nil
  }
}

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

/**
 * A command that renames a table/view.
 *
 * The syntax of this command is:
 * {{{
 *    ALTER TABLE table1 RENAME TO table2;
 *    ALTER VIEW view1 RENAME TO view2;
 * }}}
 */
case class AlterTableRename(
    oldName: TableIdentifier,
    newName: TableIdentifier)
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    sqlContext.sessionState.catalog.renameTable(oldName, newName)
    Seq.empty[Row]
  }

}

/**
 * A command that sets table/view properties.
 *
 * The syntax of this command is:
 * {{{
 *   ALTER TABLE table1 SET TBLPROPERTIES ('key1' = 'val1', 'key2' = 'val2', ...);
 *   ALTER VIEW view1 SET TBLPROPERTIES ('key1' = 'val1', 'key2' = 'val2', ...);
 * }}}
 */
case class AlterTableSetProperties(
    tableName: TableIdentifier,
    properties: Map[String, String])
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val catalog = sqlContext.sessionState.catalog
    val table = catalog.getTable(tableName)
    val newTable = table.copy(properties = table.properties ++ properties)
    catalog.alterTable(newTable)
    Seq.empty[Row]
  }

}

/**
 * A command that unsets table/view properties.
 *
 * The syntax of this command is:
 * {{{
 *   ALTER TABLE table1 UNSET TBLPROPERTIES [IF EXISTS] ('key1', 'key2', ...);
 *   ALTER VIEW view1 UNSET TBLPROPERTIES [IF EXISTS] ('key1', 'key2', ...);
 * }}}
 */
case class AlterTableUnsetProperties(
    tableName: TableIdentifier,
    propKeys: Seq[String],
    ifExists: Boolean)
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val catalog = sqlContext.sessionState.catalog
    val table = catalog.getTable(tableName)
    if (!ifExists) {
      propKeys.foreach { k =>
        if (!table.properties.contains(k)) {
          throw new AnalysisException(
            s"attempted to unset non-existent property '$k' in table '$tableName'")
        }
      }
    }
    val newProperties = table.properties.filter { case (k, _) => !propKeys.contains(k) }
    val newTable = table.copy(properties = newProperties)
    catalog.alterTable(newTable)
    Seq.empty[Row]
  }

}

/**
 * A command that sets the serde class and/or serde properties of a table/view.
 *
 * The syntax of this command is:
 * {{{
 *   ALTER TABLE table [PARTITION spec] SET SERDE serde_name [WITH SERDEPROPERTIES props];
 *   ALTER TABLE table [PARTITION spec] SET SERDEPROPERTIES serde_properties;
 * }}}
 */
case class AlterTableSerDeProperties(
    tableName: TableIdentifier,
    serdeClassName: Option[String],
    serdeProperties: Option[Map[String, String]],
    partition: Option[Map[String, String]])
  extends RunnableCommand {

  // should never happen if we parsed things correctly
  require(serdeClassName.isDefined || serdeProperties.isDefined,
    "alter table attempted to set neither serde class name nor serde properties")

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val catalog = sqlContext.sessionState.catalog
    val table = catalog.getTable(tableName)
    val newTable = table.withNewStorage(
      serde = serdeClassName.orElse(table.storage.serde),
      serdeProperties = table.storage.serdeProperties ++ serdeProperties.getOrElse(Map()))
    catalog.alterTable(newTable)
    Seq.empty[Row]
  }

}

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
 * An error message will be issued if the partition exists, unless 'ifNotExists' is true.
 */
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

/**
 * Drop Partition in ALTER TABLE/VIEW: to drop a particular partition for a table/view.
 * This removes the data and metadata for this partition.
 * The data is actually moved to the .Trash/Current directory if Trash is configured,
 * unless 'purge' is true, but the metadata is completely lost.
 * An error message will be issued if the partition does not exist, unless 'ifExists' is true.
 * Note: purge is always false when the target is a view.
 */
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

/**
 * A command that sets the location of a table or a partition.
 *
 * The syntax of this command is:
 * {{{
 *    ALTER TABLE table_name [PARTITION partition_spec] SET LOCATION "loc";
 * }}}
 */
case class AlterTableSetLocation(
    tableName: TableIdentifier,
    partitionSpec: Option[TablePartitionSpec],
    location: String)
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val catalog = sqlContext.sessionState.catalog
    if (partitionSpec.isEmpty) {
      // No partition spec is specified, so we set the location for the table itself
      val table = catalog.getTable(tableName)
      val newTable = table.withNewStorage(locationUri = Some(location))
      catalog.alterTable(newTable)
    } else {
      // Partition spec is specified, so we set the location only for this partition
      val spec = partitionSpec.get
      val part = catalog.getPartition(tableName, spec)
      val newPart = part.copy(storage = part.storage.copy(locationUri = Some(location)))
      catalog.alterPartitions(tableName, Seq(newPart))
    }
    Seq.empty[Row]
  }

}

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
