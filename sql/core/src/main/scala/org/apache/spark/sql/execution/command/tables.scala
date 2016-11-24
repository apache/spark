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

import java.io.File
import java.net.URI
import java.util.Date

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal
import scala.util.Try

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogColumn, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.catalog.CatalogTableType._
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.execution.command.CreateDataSourceTableUtils._
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

case class CreateHiveTableAsSelectLogicalPlan(
    tableDesc: CatalogTable,
    child: LogicalPlan,
    allowExisting: Boolean) extends UnaryNode with Command {

  override def output: Seq[Attribute] = Seq.empty[Attribute]

  override lazy val resolved: Boolean =
    tableDesc.identifier.database.isDefined &&
      tableDesc.schema.nonEmpty &&
      tableDesc.storage.serde.isDefined &&
      tableDesc.storage.inputFormat.isDefined &&
      tableDesc.storage.outputFormat.isDefined &&
      childrenResolved
}

/**
 * A command to create a MANAGED table with the same definition of the given existing table.
 * In the target table definition, the table comment is always empty but the column comments
 * are identical to the ones defined in the source table.
 *
 * The CatalogTable attributes copied from the source table are storage(inputFormat, outputFormat,
 * serde, compressed, properties), schema, provider, partitionColumnNames, bucketSpec.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
 *   LIKE [other_db_name.]existing_table_name
 * }}}
 */
case class CreateTableLikeCommand(
    targetTable: TableIdentifier,
    sourceTable: TableIdentifier,
    ifNotExists: Boolean) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val sourceTableDesc = catalog.getTempViewOrPermanentTableMetadata(sourceTable)

    if (DDLUtils.isDatasourceTable(sourceTableDesc) ||
        sourceTableDesc.tableType == CatalogTableType.VIEW) {
      val outputSchema =
        StructType(sourceTableDesc.schema.map { c =>
          val builder = new MetadataBuilder
          c.comment.map(comment => builder.putString("comment", comment))
          StructField(
            c.name,
            CatalystSqlParser.parseDataType(c.dataType),
            c.nullable,
            metadata = builder.build())
        })
      val (schema, provider) = if (DDLUtils.isDatasourceTable(sourceTableDesc)) {
        (DDLUtils.getSchemaFromTableProperties(sourceTableDesc).getOrElse(outputSchema),
          sourceTableDesc.properties(CreateDataSourceTableUtils.DATASOURCE_PROVIDER))
      } else { // VIEW
        (outputSchema, sparkSession.sessionState.conf.defaultDataSourceName)
      }
      createDataSourceTable(
        sparkSession = sparkSession,
        tableIdent = targetTable,
        userSpecifiedSchema = Some(schema),
        partitionColumns = Array.empty[String],
        bucketSpec = None,
        provider = provider,
        options = Map("path" -> catalog.defaultTablePath(targetTable)),
        isExternal = false)
    } else {
      val newStorage =
        sourceTableDesc.storage.copy(
          locationUri = None,
          serdeProperties = sourceTableDesc.storage.serdeProperties)
      val newTableDesc =
        CatalogTable(
          identifier = targetTable,
          tableType = CatalogTableType.MANAGED,
          storage = newStorage,
          schema = sourceTableDesc.schema,
          partitionColumnNames = sourceTableDesc.partitionColumnNames,
          sortColumnNames = sourceTableDesc.sortColumnNames,
          bucketColumnNames = sourceTableDesc.bucketColumnNames,
          numBuckets = sourceTableDesc.numBuckets)

      catalog.createTable(newTableDesc, ifNotExists)
    }

    Seq.empty[Row]
  }
}


// TODO: move the rest of the table commands from ddl.scala to this file

/**
 * A command to create a table.
 *
 * Note: This is currently used only for creating Hive tables.
 * This is not intended for temporary tables.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [db_name.]table_name
 *   [(col1 data_type [COMMENT col_comment], ...)]
 *   [COMMENT table_comment]
 *   [PARTITIONED BY (col3 data_type [COMMENT col_comment], ...)]
 *   [CLUSTERED BY (col1, ...) [SORTED BY (col1 [ASC|DESC], ...)] INTO num_buckets BUCKETS]
 *   [SKEWED BY (col1, col2, ...) ON ((col_value, col_value, ...), ...)
 *   [STORED AS DIRECTORIES]
 *   [ROW FORMAT row_format]
 *   [STORED AS file_format | STORED BY storage_handler_class [WITH SERDEPROPERTIES (...)]]
 *   [LOCATION path]
 *   [TBLPROPERTIES (property_name=property_value, ...)]
 *   [AS select_statement];
 * }}}
 */
case class CreateTableCommand(table: CatalogTable, ifNotExists: Boolean) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    DDLUtils.verifyTableProperties(table.properties.keys.toSeq, "CREATE TABLE")
    DDLUtils.verifyTableProperties(table.storage.serdeProperties.keys.toSeq, "CREATE TABLE")
    sparkSession.sessionState.catalog.createTable(table, ifNotExists)
    Seq.empty[Row]
  }

}


/**
 * A command that renames a table/view.
 *
 * The syntax of this command is:
 * {{{
 *    ALTER TABLE table1 RENAME TO table2;
 *    ALTER VIEW view1 RENAME TO view2;
 * }}}
 */
case class AlterTableRenameCommand(
    oldName: TableIdentifier,
    newName: TableIdentifier,
    isView: Boolean)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    // If this is a temp view, just rename the view.
    // Otherwise, if this is a real table, we also need to uncache and invalidate the table.
    if (catalog.isTemporaryTable(oldName)) {
      catalog.renameTable(oldName, newName)
    } else {
      val table = catalog.getTableMetadata(oldName)
      DDLUtils.verifyAlterTableType(catalog, table.identifier, isView)
      // If an exception is thrown here we can just assume the table is uncached;
      // this can happen with Hive tables when the underlying catalog is in-memory.
      val wasCached = Try(sparkSession.catalog.isCached(oldName.unquotedString)).getOrElse(false)
      if (wasCached) {
        try {
          sparkSession.catalog.uncacheTable(oldName.unquotedString)
        } catch {
          case NonFatal(e) => log.warn(e.toString, e)
        }
      }
      // For datasource tables, we also need to update the "path" serde property
      if (DDLUtils.isDatasourceTable(table) && table.tableType == CatalogTableType.MANAGED) {
        val newPath = catalog.defaultTablePath(newName)
        val newTable = table.withNewStorage(
          serdeProperties = table.storage.serdeProperties ++ Map("path" -> newPath))
        catalog.alterTable(newTable)
      }
      // Invalidate the table last, otherwise uncaching the table would load the logical plan
      // back into the hive metastore cache
      catalog.refreshTable(oldName)
      catalog.renameTable(oldName, newName)
      if (wasCached) {
        sparkSession.catalog.cacheTable(newName.unquotedString)
      }
    }
    Seq.empty[Row]
  }

}

/**
 * A command that loads data into a Hive table.
 *
 * The syntax of this command is:
 * {{{
 *  LOAD DATA [LOCAL] INPATH 'filepath' [OVERWRITE] INTO TABLE tablename
 *  [PARTITION (partcol1=val1, partcol2=val2 ...)]
 * }}}
 */
case class LoadDataCommand(
    table: TableIdentifier,
    path: String,
    isLocal: Boolean,
    isOverwrite: Boolean,
    partition: Option[TablePartitionSpec]) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val targetTable = catalog.getTableMetadata(table)
    val tableIdentwithDB = targetTable.identifier.quotedString
    if (DDLUtils.isDatasourceTable(targetTable)) {
      throw new AnalysisException(
        s"LOAD DATA is not supported for datasource tables: '$tableIdentwithDB'")
    }
    if (targetTable.partitionColumnNames.nonEmpty) {
      if (partition.isEmpty) {
        throw new AnalysisException(s"LOAD DATA target table '$tableIdentwithDB' is partitioned, " +
          s"but no partition spec is provided")
      }
      if (targetTable.partitionColumnNames.size != partition.get.size) {
        throw new AnalysisException(s"LOAD DATA target table '$tableIdentwithDB' is partitioned, " +
          s"but number of columns in provided partition spec (${partition.get.size}) " +
          s"do not match number of partitioned columns in table " +
          s"(s${targetTable.partitionColumnNames.size})")
      }
      partition.get.keys.foreach { colName =>
        if (!targetTable.partitionColumnNames.contains(colName)) {
          throw new AnalysisException(s"LOAD DATA target table '$tableIdentwithDB' is " +
            s"partitioned, but the specified partition spec refers to a column that is " +
            s"not partitioned: '$colName'")
        }
      }
    } else {
      if (partition.nonEmpty) {
        throw new AnalysisException(s"LOAD DATA target table '$tableIdentwithDB' is not " +
          s"partitioned, but a partition spec was provided.")
      }
    }

    val loadPath =
      if (isLocal) {
        val uri = Utils.resolveURI(path)
        if (!new File(uri.getPath()).exists()) {
          throw new AnalysisException(s"LOAD DATA input path does not exist: $path")
        }
        uri
      } else {
        val uri = new URI(path)
        if (uri.getScheme() != null && uri.getAuthority() != null) {
          uri
        } else {
          // Follow Hive's behavior:
          // If no schema or authority is provided with non-local inpath,
          // we will use hadoop configuration "fs.default.name".
          val defaultFSConf = sparkSession.sessionState.newHadoopConf().get("fs.default.name")
          val defaultFS = if (defaultFSConf == null) {
            new URI("")
          } else {
            new URI(defaultFSConf)
          }

          val scheme = if (uri.getScheme() != null) {
            uri.getScheme()
          } else {
            defaultFS.getScheme()
          }
          val authority = if (uri.getAuthority() != null) {
            uri.getAuthority()
          } else {
            defaultFS.getAuthority()
          }

          if (scheme == null) {
            throw new AnalysisException(
              s"LOAD DATA: URI scheme is required for non-local input paths: '$path'")
          }

          // Follow Hive's behavior:
          // If LOCAL is not specified, and the path is relative,
          // then the path is interpreted relative to "/user/<username>"
          val uriPath = uri.getPath()
          val absolutePath = if (uriPath != null && uriPath.startsWith("/")) {
            uriPath
          } else {
            s"/user/${System.getProperty("user.name")}/$uriPath"
          }
          new URI(scheme, authority, absolutePath, uri.getQuery(), uri.getFragment())
        }
      }

    if (partition.nonEmpty) {
      catalog.loadPartition(
        targetTable.identifier,
        loadPath.toString,
        partition.get,
        isOverwrite,
        holdDDLTime = false,
        inheritTableSpecs = true,
        isSkewedStoreAsSubdir = false)
    } else {
      catalog.loadTable(
        targetTable.identifier,
        loadPath.toString,
        isOverwrite,
        holdDDLTime = false)
    }
    Seq.empty[Row]
  }
}

/**
 * A command to truncate table.
 *
 * The syntax of this command is:
 * {{{
 *   TRUNCATE TABLE tablename [PARTITION (partcol1=val1, partcol2=val2 ...)]
 * }}}
 */
case class TruncateTableCommand(
    tableName: TableIdentifier,
    partitionSpec: Option[TablePartitionSpec]) extends RunnableCommand {

  override def run(spark: SparkSession): Seq[Row] = {
    val catalog = spark.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    val tableIdentwithDB = table.identifier.quotedString
    if (table.tableType == CatalogTableType.EXTERNAL) {
      throw new AnalysisException(
        s"Operation not allowed: TRUNCATE TABLE on external tables: '$tableIdentwithDB'")
    }
    if (table.tableType == CatalogTableType.VIEW) {
      throw new AnalysisException(
        s"Operation not allowed: TRUNCATE TABLE on views: '$tableIdentwithDB'")
    }
    val isDatasourceTable = DDLUtils.isDatasourceTable(table)
    if (isDatasourceTable && partitionSpec.isDefined) {
      throw new AnalysisException(
        s"Operation not allowed: TRUNCATE TABLE ... PARTITION is not supported " +
        s"for tables created using the data sources API: '$tableIdentwithDB'")
    }
    if (table.partitionColumnNames.isEmpty && partitionSpec.isDefined) {
      throw new AnalysisException(
        s"Operation not allowed: TRUNCATE TABLE ... PARTITION is not supported " +
        s"for tables that are not partitioned: '$tableIdentwithDB'")
    }
    val locations =
      if (isDatasourceTable) {
        Seq(table.storage.serdeProperties.get("path"))
      } else if (table.partitionColumnNames.isEmpty) {
        Seq(table.storage.locationUri)
      } else {
        catalog.listPartitions(table.identifier, partitionSpec).map(_.storage.locationUri)
      }
    val hadoopConf = spark.sessionState.newHadoopConf()
    locations.foreach { location =>
      if (location.isDefined) {
        val path = new Path(location.get)
        try {
          val fs = path.getFileSystem(hadoopConf)
          fs.delete(path, true)
          fs.mkdirs(path)
        } catch {
          case NonFatal(e) =>
            throw new AnalysisException(
              s"Failed to truncate table '$tableIdentwithDB' when removing data of the path: " +
                s"$path because of ${e.toString}")
        }
      }
    }
    // After deleting the data, invalidate the table to make sure we don't keep around a stale
    // file relation in the metastore cache.
    spark.sessionState.refreshTable(tableName.unquotedString)
    // Also try to drop the contents of the table from the columnar cache
    try {
      spark.sharedState.cacheManager.uncacheQuery(spark.table(table.identifier))
    } catch {
      case NonFatal(e) =>
        log.warn(s"Exception when attempting to uncache table '$tableIdentwithDB'", e)
    }
    Seq.empty[Row]
  }
}

/**
 * Command that looks like
 * {{{
 *   DESCRIBE [EXTENDED|FORMATTED] table_name;
 * }}}
 */
case class DescribeTableCommand(table: TableIdentifier, isExtended: Boolean, isFormatted: Boolean)
  extends RunnableCommand {

  override val output: Seq[Attribute] = Seq(
    // Column names are based on Hive.
    AttributeReference("col_name", StringType, nullable = false,
      new MetadataBuilder().putString("comment", "name of the column").build())(),
    AttributeReference("data_type", StringType, nullable = false,
      new MetadataBuilder().putString("comment", "data type of the column").build())(),
    AttributeReference("comment", StringType, nullable = true,
      new MetadataBuilder().putString("comment", "comment of the column").build())()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val result = new ArrayBuffer[Row]
    val catalog = sparkSession.sessionState.catalog

    if (catalog.isTemporaryTable(table)) {
      describeSchema(catalog.lookupRelation(table).schema, result)
    } else {
      val metadata = catalog.getTableMetadata(table)

      if (DDLUtils.isDatasourceTable(metadata)) {
        DDLUtils.getSchemaFromTableProperties(metadata) match {
          case Some(userSpecifiedSchema) => describeSchema(userSpecifiedSchema, result)
          case None => describeSchema(catalog.lookupRelation(table).schema, result)
        }
      } else {
        describeSchema(metadata.schema, result)
      }

      if (isExtended) {
        describeExtended(metadata, result)
      } else if (isFormatted) {
        describeFormatted(metadata, result)
      } else {
        describePartitionInfo(metadata, result)
      }
    }

    result
  }

  private def describePartitionInfo(table: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    if (DDLUtils.isDatasourceTable(table)) {
      val userSpecifiedSchema = DDLUtils.getSchemaFromTableProperties(table)
      val partColNames = DDLUtils.getPartitionColumnsFromTableProperties(table)
      for (schema <- userSpecifiedSchema if partColNames.nonEmpty) {
        append(buffer, "# Partition Information", "", "")
        append(buffer, s"# ${output.head.name}", output(1).name, output(2).name)
        describeSchema(StructType(partColNames.map(schema(_))), buffer)
      }
    } else {
      if (table.partitionColumns.nonEmpty) {
        append(buffer, "# Partition Information", "", "")
        append(buffer, s"# ${output.head.name}", output(1).name, output(2).name)
        describeSchema(table.partitionColumns, buffer)
      }
    }
  }

  private def describeExtended(table: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    describePartitionInfo(table, buffer)

    append(buffer, "", "", "")
    append(buffer, "# Detailed Table Information", table.toString, "")
  }

  private def describeFormatted(table: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    describePartitionInfo(table, buffer)

    append(buffer, "", "", "")
    append(buffer, "# Detailed Table Information", "", "")
    append(buffer, "Database:", table.database, "")
    append(buffer, "Owner:", table.owner, "")
    append(buffer, "Create Time:", new Date(table.createTime).toString, "")
    append(buffer, "Last Access Time:", new Date(table.lastAccessTime).toString, "")
    append(buffer, "Location:", table.storage.locationUri.getOrElse(""), "")
    append(buffer, "Table Type:", table.tableType.name, "")

    append(buffer, "Table Parameters:", "", "")
    table.properties.filterNot {
      // Hides schema properties that hold user-defined schema, partition columns, and bucketing
      // information since they are already extracted and shown in other parts.
      case (key, _) => key.startsWith(CreateDataSourceTableUtils.DATASOURCE_SCHEMA)
    }.foreach { case (key, value) =>
      append(buffer, s"  $key", value, "")
    }

    describeStorageInfo(table, buffer)
  }

  private def describeStorageInfo(metadata: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    append(buffer, "", "", "")
    append(buffer, "# Storage Information", "", "")
    metadata.storage.serde.foreach(serdeLib => append(buffer, "SerDe Library:", serdeLib, ""))
    metadata.storage.inputFormat.foreach(format => append(buffer, "InputFormat:", format, ""))
    metadata.storage.outputFormat.foreach(format => append(buffer, "OutputFormat:", format, ""))
    append(buffer, "Compressed:", if (metadata.storage.compressed) "Yes" else "No", "")
    describeBucketingInfo(metadata, buffer)

    append(buffer, "Storage Desc Parameters:", "", "")
    metadata.storage.serdeProperties.foreach { case (key, value) =>
      append(buffer, s"  $key", value, "")
    }
  }

  private def describeBucketingInfo(metadata: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    def appendBucketInfo(numBuckets: Int, bucketColumns: Seq[String], sortColumns: Seq[String]) = {
      append(buffer, "Num Buckets:", numBuckets.toString, "")
      append(buffer, "Bucket Columns:", bucketColumns.mkString("[", ", ", "]"), "")
      append(buffer, "Sort Columns:", sortColumns.mkString("[", ", ", "]"), "")
    }

    DDLUtils.getBucketSpecFromTableProperties(metadata) match {
      case Some(bucketSpec) =>
        appendBucketInfo(
          bucketSpec.numBuckets,
          bucketSpec.bucketColumnNames,
          bucketSpec.sortColumnNames)
      case None =>
        appendBucketInfo(
          metadata.numBuckets,
          metadata.bucketColumnNames,
          metadata.sortColumnNames)
    }
  }

  private def describeSchema(schema: Seq[CatalogColumn], buffer: ArrayBuffer[Row]): Unit = {
    schema.foreach { column =>
      append(buffer, column.name, column.dataType.toLowerCase, column.comment.orNull)
    }
  }

  private def describeSchema(schema: StructType, buffer: ArrayBuffer[Row]): Unit = {
    schema.foreach { column =>
      val comment =
        if (column.metadata.contains("comment")) column.metadata.getString("comment") else null
      append(buffer, column.name, column.dataType.simpleString, comment)
    }
  }

  private def append(
      buffer: ArrayBuffer[Row], column: String, dataType: String, comment: String): Unit = {
    buffer += Row(column, dataType, comment)
  }
}


/**
 * A command for users to get tables in the given database.
 * If a databaseName is not given, the current database will be used.
 * The syntax of using this command in SQL is:
 * {{{
 *   SHOW TABLES [(IN|FROM) database_name] [[LIKE] 'identifier_with_wildcards'];
 * }}}
 */
case class ShowTablesCommand(
    databaseName: Option[String],
    tableIdentifierPattern: Option[String]) extends RunnableCommand {

  // The result of SHOW TABLES has two columns, tableName and isTemporary.
  override val output: Seq[Attribute] = {
    AttributeReference("tableName", StringType, nullable = false)() ::
      AttributeReference("isTemporary", BooleanType, nullable = false)() :: Nil
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // Since we need to return a Seq of rows, we will call getTables directly
    // instead of calling tables in sparkSession.
    val catalog = sparkSession.sessionState.catalog
    val db = databaseName.getOrElse(catalog.getCurrentDatabase)
    val tables =
      tableIdentifierPattern.map(catalog.listTables(db, _)).getOrElse(catalog.listTables(db))
    tables.map { t =>
      val isTemp = t.database.isEmpty
      Row(t.table, isTemp)
    }
  }
}


/**
 * A command for users to list the properties for a table If propertyKey is specified, the value
 * for the propertyKey is returned. If propertyKey is not specified, all the keys and their
 * corresponding values are returned.
 * The syntax of using this command in SQL is:
 * {{{
 *   SHOW TBLPROPERTIES table_name[('propertyKey')];
 * }}}
 */
case class ShowTablePropertiesCommand(table: TableIdentifier, propertyKey: Option[String])
  extends RunnableCommand {

  override val output: Seq[Attribute] = {
    val schema = AttributeReference("value", StringType, nullable = false)() :: Nil
    propertyKey match {
      case None => AttributeReference("key", StringType, nullable = false)() :: schema
      case _ => schema
    }
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog

    if (catalog.isTemporaryTable(table)) {
      Seq.empty[Row]
    } else {
      val catalogTable = sparkSession.sessionState.catalog.getTableMetadata(table)

      propertyKey match {
        case Some(p) =>
          val propValue = catalogTable
            .properties
            .getOrElse(p, s"Table ${catalogTable.qualifiedName} does not have property: $p")
          Seq(Row(propValue))
        case None =>
          catalogTable.properties.map(p => Row(p._1, p._2)).toSeq
      }
    }
  }
}

/**
 * A command to list the column names for a table. This function creates a
 * [[ShowColumnsCommand]] logical plan.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   SHOW COLUMNS (FROM | IN) table_identifier [(FROM | IN) database];
 * }}}
 */
case class ShowColumnsCommand(tableName: TableIdentifier) extends RunnableCommand {
  // The result of SHOW COLUMNS has one column called 'result'
  override val output: Seq[Attribute] = {
    AttributeReference("result", StringType, nullable = false)() :: Nil
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTempViewOrPermanentTableMetadata(tableName)
    table.schema.map { c =>
      Row(c.name)
    }
  }
}

/**
 * A command to list the partition names of a table. If the partition spec is specified,
 * partitions that match the spec are returned. [[AnalysisException]] exception is thrown under
 * the following conditions:
 *
 * 1. If the command is called for a non partitioned table.
 * 2. If the partition spec refers to the columns that are not defined as partitioning columns.
 *
 * This function creates a [[ShowPartitionsCommand]] logical plan
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   SHOW PARTITIONS [db_name.]table_name [PARTITION(partition_spec)]
 * }}}
 */
case class ShowPartitionsCommand(
    tableName: TableIdentifier,
    spec: Option[TablePartitionSpec]) extends RunnableCommand {
  // The result of SHOW PARTITIONS has one column called 'result'
  override val output: Seq[Attribute] = {
    AttributeReference("result", StringType, nullable = false)() :: Nil
  }

  private def getPartName(spec: TablePartitionSpec, partColNames: Seq[String]): String = {
    partColNames.map { name =>
      PartitioningUtils.escapePathName(name) + "=" + PartitioningUtils.escapePathName(spec(name))
    }.mkString(File.separator)
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val table = catalog.getTableMetadata(tableName)
    val tableIdentWithDB = table.identifier.quotedString

    /**
     * Validate and throws an [[AnalysisException]] exception under the following conditions:
     * 1. If the table is not partitioned.
     * 2. If it is a datasource table.
     * 3. If it is a view.
     */
    if (table.tableType == VIEW) {
      throw new AnalysisException(s"SHOW PARTITIONS is not allowed on a view: $tableIdentWithDB")
    }

    if (!DDLUtils.isTablePartitioned(table)) {
      throw new AnalysisException(
        s"SHOW PARTITIONS is not allowed on a table that is not partitioned: $tableIdentWithDB")
    }

    if (DDLUtils.isDatasourceTable(table)) {
      throw new AnalysisException(
        s"SHOW PARTITIONS is not allowed on a datasource table: $tableIdentWithDB")
    }

    /**
     * Validate the partitioning spec by making sure all the referenced columns are
     * defined as partitioning columns in table definition. An AnalysisException exception is
     * thrown if the partitioning spec is invalid.
     */
    if (spec.isDefined) {
      val badColumns = spec.get.keySet.filterNot(table.partitionColumns.map(_.name).contains)
      if (badColumns.nonEmpty) {
        val badCols = badColumns.mkString("[", ", ", "]")
        throw new AnalysisException(
          s"Non-partitioning column(s) $badCols are specified for SHOW PARTITIONS")
      }
    }

    val partNames = catalog.listPartitions(tableName, spec).map { p =>
      getPartName(p.spec, table.partitionColumnNames)
    }

    partNames.map(Row(_))
  }
}

case class ShowCreateTableCommand(table: TableIdentifier) extends RunnableCommand {
  override val output: Seq[Attribute] = Seq(
    AttributeReference("createtab_stmt", StringType, nullable = false)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = sparkSession.sessionState.catalog
    val tableMetadata = catalog.getTableMetadata(table)

    val stmt = if (DDLUtils.isDatasourceTable(tableMetadata)) {
      showCreateDataSourceTable(tableMetadata)
    } else {
      showCreateHiveTable(tableMetadata)
    }

    Seq(Row(stmt))
  }

  private def showCreateHiveTable(metadata: CatalogTable): String = {
    def reportUnsupportedError(features: Seq[String]): Unit = {
      throw new AnalysisException(
        s"Failed to execute SHOW CREATE TABLE against table ${metadata.identifier.quotedString}, " +
          "which is created by Hive and uses the following unsupported feature(s)\n" +
          features.map(" - " + _).mkString("\n")
      )
    }

    if (metadata.unsupportedFeatures.nonEmpty) {
      reportUnsupportedError(metadata.unsupportedFeatures)
    }

    val builder = StringBuilder.newBuilder

    val tableTypeString = metadata.tableType match {
      case EXTERNAL => " EXTERNAL TABLE"
      case VIEW => " VIEW"
      case MANAGED => " TABLE"
    }

    builder ++= s"CREATE$tableTypeString ${table.quotedString}"

    if (metadata.tableType == VIEW) {
      if (metadata.schema.nonEmpty) {
        builder ++= metadata.schema.map(_.name).mkString("(", ", ", ")")
      }
      builder ++= metadata.viewText.mkString(" AS\n", "", "\n")
    } else {
      showHiveTableHeader(metadata, builder)
      showHiveTableNonDataColumns(metadata, builder)
      showHiveTableStorageInfo(metadata, builder)
      showHiveTableProperties(metadata, builder)
    }

    builder.toString()
  }

  private def showHiveTableHeader(metadata: CatalogTable, builder: StringBuilder): Unit = {
    val columns = metadata.schema.filterNot { column =>
      metadata.partitionColumnNames.contains(column.name)
    }.map(columnToDDLFragment)

    if (columns.nonEmpty) {
      builder ++= columns.mkString("(", ", ", ")\n")
    }

    metadata
      .comment
      .map("COMMENT '" + escapeSingleQuotedString(_) + "'\n")
      .foreach(builder.append)
  }

  private def columnToDDLFragment(column: CatalogColumn): String = {
    val comment = column.comment.map(escapeSingleQuotedString).map(" COMMENT '" + _ + "'")
    s"${quoteIdentifier(column.name)} ${column.dataType}${comment.getOrElse("")}"
  }

  private def showHiveTableNonDataColumns(metadata: CatalogTable, builder: StringBuilder): Unit = {
    if (metadata.partitionColumns.nonEmpty) {
      val partCols = metadata.partitionColumns.map(columnToDDLFragment)
      builder ++= partCols.mkString("PARTITIONED BY (", ", ", ")\n")
    }

    if (metadata.bucketColumnNames.nonEmpty) {
      throw new UnsupportedOperationException(
        "Creating Hive table with bucket spec is not supported yet.")
    }
  }

  private def showHiveTableStorageInfo(metadata: CatalogTable, builder: StringBuilder): Unit = {
    val storage = metadata.storage

    storage.serde.foreach { serde =>
      builder ++= s"ROW FORMAT SERDE '$serde'\n"

      val serdeProps = metadata.storage.serdeProperties.map {
        case (key, value) =>
          s"'${escapeSingleQuotedString(key)}' = '${escapeSingleQuotedString(value)}'"
      }

      builder ++= serdeProps.mkString("WITH SERDEPROPERTIES (\n  ", ",\n  ", "\n)\n")
    }

    if (storage.inputFormat.isDefined || storage.outputFormat.isDefined) {
      builder ++= "STORED AS\n"

      storage.inputFormat.foreach { format =>
        builder ++= s"  INPUTFORMAT '${escapeSingleQuotedString(format)}'\n"
      }

      storage.outputFormat.foreach { format =>
        builder ++= s"  OUTPUTFORMAT '${escapeSingleQuotedString(format)}'\n"
      }
    }

    if (metadata.tableType == EXTERNAL) {
      storage.locationUri.foreach { uri =>
        builder ++= s"LOCATION '$uri'\n"
      }
    }
  }

  private def showHiveTableProperties(metadata: CatalogTable, builder: StringBuilder): Unit = {
    if (metadata.properties.nonEmpty) {
      val filteredProps = metadata.properties.filterNot {
        // Skips "EXTERNAL" property for external tables
        case (key, _) => key == "EXTERNAL" && metadata.tableType == EXTERNAL
      }

      val props = filteredProps.map { case (key, value) =>
        s"'${escapeSingleQuotedString(key)}' = '${escapeSingleQuotedString(value)}'"
      }

      if (props.nonEmpty) {
        builder ++= props.mkString("TBLPROPERTIES (\n  ", ",\n  ", "\n)\n")
      }
    }
  }

  private def showCreateDataSourceTable(metadata: CatalogTable): String = {
    val builder = StringBuilder.newBuilder

    builder ++= s"CREATE TABLE ${table.quotedString} "
    showDataSourceTableDataColumns(metadata, builder)
    showDataSourceTableOptions(metadata, builder)
    showDataSourceTableNonDataColumns(metadata, builder)

    builder.toString()
  }

  private def showDataSourceTableDataColumns(
      metadata: CatalogTable, builder: StringBuilder): Unit = {
    DDLUtils.getSchemaFromTableProperties(metadata).foreach { schema =>
      val columns = schema.fields.map(f => s"${quoteIdentifier(f.name)} ${f.dataType.sql}")
      builder ++= columns.mkString("(", ", ", ")")
    }

    builder ++= "\n"
  }

  private def showDataSourceTableOptions(metadata: CatalogTable, builder: StringBuilder): Unit = {
    val props = metadata.properties

    builder ++= s"USING ${props(CreateDataSourceTableUtils.DATASOURCE_PROVIDER)}\n"

    val dataSourceOptions = metadata.storage.serdeProperties.filterNot {
      case (key, value) =>
        // If it's a managed table, omit PATH option. Spark SQL always creates external table
        // when the table creation DDL contains the PATH option.
        key.toLowerCase == "path" && metadata.tableType == MANAGED
    }.map {
      case (key, value) => s"${quoteIdentifier(key)} '${escapeSingleQuotedString(value)}'"
    }

    if (dataSourceOptions.nonEmpty) {
      builder ++= "OPTIONS (\n"
      builder ++= dataSourceOptions.mkString("  ", ",\n  ", "\n")
      builder ++= ")\n"
    }
  }

  private def showDataSourceTableNonDataColumns(
      metadata: CatalogTable, builder: StringBuilder): Unit = {
    val partCols = DDLUtils.getPartitionColumnsFromTableProperties(metadata)
    if (partCols.nonEmpty) {
      builder ++= s"PARTITIONED BY ${partCols.mkString("(", ", ", ")")}\n"
    }

    DDLUtils.getBucketSpecFromTableProperties(metadata).foreach { spec =>
      if (spec.bucketColumnNames.nonEmpty) {
        builder ++= s"CLUSTERED BY ${spec.bucketColumnNames.mkString("(", ", ", ")")}\n"

        if (spec.sortColumnNames.nonEmpty) {
          builder ++= s"SORTED BY ${spec.sortColumnNames.mkString("(", ", ", ")")}\n"
        }

        builder ++= s"INTO ${spec.numBuckets} BUCKETS\n"
      }
    }
  }

  private def escapeSingleQuotedString(str: String): String = {
    val builder = StringBuilder.newBuilder

    str.foreach {
      case '\'' => builder ++= s"\\\'"
      case ch => builder += ch
    }

    builder.toString()
  }
}
