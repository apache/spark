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

package org.apache.spark.sql.hive.execution

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.common.StatsSetupConst
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.MetaStoreUtils
import org.apache.hadoop.hive.ql.metadata.Table

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.{BucketSpec, DataSource, LogicalRelation}
import org.apache.spark.sql.hive.{HiveContext, MetastoreRelation}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

/**
 * Analyzes the given table in the current database to generate statistics, which will be
 * used in query optimizations.
 *
 * Right now, it only supports Hive tables and it only updates the size of a Hive table
 * in the Hive metastore.
 */
private[hive]
case class AnalyzeTable(tableName: String) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val sessionState = sqlContext.sessionState
    val hiveContext = sqlContext.asInstanceOf[HiveContext]
    val tableIdent = sessionState.sqlParser.parseTableIdentifier(tableName)
    val relation = EliminateSubqueryAliases(sessionState.catalog.lookupRelation(tableIdent))

    relation match {
      case relation: MetastoreRelation =>
        // This method is mainly based on
        // org.apache.hadoop.hive.ql.stats.StatsUtils.getFileSizeForTable(HiveConf, Table)
        // in Hive 0.13 (except that we do not use fs.getContentSummary).
        // TODO: Generalize statistics collection.
        // TODO: Why fs.getContentSummary returns wrong size on Jenkins?
        // Can we use fs.getContentSummary in future?
        // Seems fs.getContentSummary returns wrong table size on Jenkins. So we use
        // countFileSize to count the table size.
        val stagingDir = hiveContext.metadataHive.getConf(
          HiveConf.ConfVars.STAGINGDIR.varname,
          HiveConf.ConfVars.STAGINGDIR.defaultStrVal)

        def calculateTableSize(fs: FileSystem, path: Path): Long = {
          val fileStatus = fs.getFileStatus(path)
          val size = if (fileStatus.isDirectory) {
            fs.listStatus(path)
              .map { status =>
              if (!status.getPath().getName().startsWith(stagingDir)) {
                calculateTableSize(fs, status.getPath)
              } else {
                0L
              }
            }
              .sum
          } else {
            fileStatus.getLen
          }

          size
        }

        def getFileSizeForTable(conf: HiveConf, table: Table): Long = {
          val path = table.getPath
          var size: Long = 0L
          try {
            val fs = path.getFileSystem(conf)
            size = calculateTableSize(fs, path)
          } catch {
            case e: Exception =>
              logWarning(
                s"Failed to get the size of table ${table.getTableName} in the " +
                  s"database ${table.getDbName} because of ${e.toString}", e)
              size = 0L
          }

          size
        }

        val tableParameters = relation.hiveQlTable.getParameters
        val oldTotalSize =
          Option(tableParameters.get(StatsSetupConst.TOTAL_SIZE))
            .map(_.toLong)
            .getOrElse(0L)
        val newTotalSize =
          getFileSizeForTable(hiveContext.sessionState.hiveconf, relation.hiveQlTable)
        // Update the Hive metastore if the total size of the table is different than the size
        // recorded in the Hive metastore.
        // This logic is based on org.apache.hadoop.hive.ql.exec.StatsTask.aggregateStats().
        if (newTotalSize > 0 && newTotalSize != oldTotalSize) {
          sessionState.catalog.alterTable(
            relation.table.copy(
              properties = relation.table.properties +
                (StatsSetupConst.TOTAL_SIZE -> newTotalSize.toString)))
        }
      case otherRelation =>
        throw new UnsupportedOperationException(
          s"Analyze only works for Hive tables, but $tableName is a ${otherRelation.nodeName}")
    }
    Seq.empty[Row]
  }
}

private[hive]
case class AddJar(path: String) extends RunnableCommand {

  override val output: Seq[Attribute] = {
    val schema = StructType(
      StructField("result", IntegerType, false) :: Nil)
    schema.toAttributes
  }

  override def run(sqlContext: SQLContext): Seq[Row] = {
    sqlContext.addJar(path)

    Seq(Row(0))
  }
}

private[hive]
case class AddFile(path: String) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val hiveContext = sqlContext.asInstanceOf[HiveContext]
    hiveContext.runSqlHive(s"ADD FILE $path")
    hiveContext.sparkContext.addFile(path)
    Seq.empty[Row]
  }
}

private[hive]
case class CreateMetastoreDataSource(
    tableIdent: TableIdentifier,
    userSpecifiedSchema: Option[StructType],
    provider: String,
    options: Map[String, String],
    allowExisting: Boolean,
    managedIfNoPath: Boolean) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    // Since we are saving metadata to metastore, we need to check if metastore supports
    // the table name and database name we have for this query. MetaStoreUtils.validateName
    // is the method used by Hive to check if a table name or a database name is valid for
    // the metastore.
    if (!MetaStoreUtils.validateName(tableIdent.table)) {
      throw new AnalysisException(s"Table name ${tableIdent.table} is not a valid name for " +
        s"metastore. Metastore only accepts table name containing characters, numbers and _.")
    }
    if (tableIdent.database.isDefined && !MetaStoreUtils.validateName(tableIdent.database.get)) {
      throw new AnalysisException(s"Database name ${tableIdent.database.get} is not a valid name " +
        s"for metastore. Metastore only accepts database name containing " +
        s"characters, numbers and _.")
    }

    val tableName = tableIdent.unquotedString
    val hiveContext = sqlContext.asInstanceOf[HiveContext]

    if (hiveContext.sessionState.catalog.tableExists(tableIdent)) {
      if (allowExisting) {
        return Seq.empty[Row]
      } else {
        throw new AnalysisException(s"Table $tableName already exists.")
      }
    }

    var isExternal = true
    val optionsWithPath =
      if (!options.contains("path") && managedIfNoPath) {
        isExternal = false
        options + ("path" ->
          hiveContext.sessionState.catalog.hiveDefaultTableFilePath(tableIdent))
      } else {
        options
      }

    // Create the relation to validate the arguments before writing the metadata to the metastore.
    DataSource(
      sqlContext = sqlContext,
      userSpecifiedSchema = userSpecifiedSchema,
      className = provider,
      bucketSpec = None,
      options = optionsWithPath).resolveRelation()

    hiveContext.sessionState.catalog.createDataSourceTable(
      tableIdent,
      userSpecifiedSchema,
      Array.empty[String],
      bucketSpec = None,
      provider,
      optionsWithPath,
      isExternal)

    Seq.empty[Row]
  }
}

private[hive]
case class CreateMetastoreDataSourceAsSelect(
    tableIdent: TableIdentifier,
    provider: String,
    partitionColumns: Array[String],
    bucketSpec: Option[BucketSpec],
    mode: SaveMode,
    options: Map[String, String],
    query: LogicalPlan) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    // Since we are saving metadata to metastore, we need to check if metastore supports
    // the table name and database name we have for this query. MetaStoreUtils.validateName
    // is the method used by Hive to check if a table name or a database name is valid for
    // the metastore.
    if (!MetaStoreUtils.validateName(tableIdent.table)) {
      throw new AnalysisException(s"Table name ${tableIdent.table} is not a valid name for " +
        s"metastore. Metastore only accepts table name containing characters, numbers and _.")
    }
    if (tableIdent.database.isDefined && !MetaStoreUtils.validateName(tableIdent.database.get)) {
      throw new AnalysisException(s"Database name ${tableIdent.database.get} is not a valid name " +
        s"for metastore. Metastore only accepts database name containing " +
        s"characters, numbers and _.")
    }

    val tableName = tableIdent.unquotedString
    val hiveContext = sqlContext.asInstanceOf[HiveContext]
    var createMetastoreTable = false
    var isExternal = true
    val optionsWithPath =
      if (!options.contains("path")) {
        isExternal = false
        options + ("path" ->
          hiveContext.sessionState.catalog.hiveDefaultTableFilePath(tableIdent))
      } else {
        options
      }

    var existingSchema = None: Option[StructType]
    if (sqlContext.sessionState.catalog.tableExists(tableIdent)) {
      // Check if we need to throw an exception or just return.
      mode match {
        case SaveMode.ErrorIfExists =>
          throw new AnalysisException(s"Table $tableName already exists. " +
            s"If you are using saveAsTable, you can set SaveMode to SaveMode.Append to " +
            s"insert data into the table or set SaveMode to SaveMode.Overwrite to overwrite" +
            s"the existing data. " +
            s"Or, if you are using SQL CREATE TABLE, you need to drop $tableName first.")
        case SaveMode.Ignore =>
          // Since the table already exists and the save mode is Ignore, we will just return.
          return Seq.empty[Row]
        case SaveMode.Append =>
          // Check if the specified data source match the data source of the existing table.
          val dataSource = DataSource(
            sqlContext = sqlContext,
            userSpecifiedSchema = Some(query.schema.asNullable),
            partitionColumns = partitionColumns,
            bucketSpec = bucketSpec,
            className = provider,
            options = optionsWithPath)
          // TODO: Check that options from the resolved relation match the relation that we are
          // inserting into (i.e. using the same compression).

          EliminateSubqueryAliases(
            sqlContext.sessionState.catalog.lookupRelation(tableIdent)) match {
            case l @ LogicalRelation(_: InsertableRelation | _: HadoopFsRelation, _, _) =>
              existingSchema = Some(l.schema)
            case o =>
              throw new AnalysisException(s"Saving data in ${o.toString} is not supported.")
          }
        case SaveMode.Overwrite =>
          hiveContext.sql(s"DROP TABLE IF EXISTS $tableName")
          // Need to create the table again.
          createMetastoreTable = true
      }
    } else {
      // The table does not exist. We need to create it in metastore.
      createMetastoreTable = true
    }

    val data = Dataset.ofRows(hiveContext, query)
    val df = existingSchema match {
      // If we are inserting into an existing table, just use the existing schema.
      case Some(s) => sqlContext.internalCreateDataFrame(data.queryExecution.toRdd, s)
      case None => data
    }

    // Create the relation based on the data of df.
    val dataSource = DataSource(
      sqlContext,
      className = provider,
      partitionColumns = partitionColumns,
      bucketSpec = bucketSpec,
      options = optionsWithPath)

    val result = dataSource.write(mode, df)

    if (createMetastoreTable) {
      // We will use the schema of resolved.relation as the schema of the table (instead of
      // the schema of df). It is important since the nullability may be changed by the relation
      // provider (for example, see org.apache.spark.sql.parquet.DefaultSource).
      hiveContext.sessionState.catalog.createDataSourceTable(
        tableIdent,
        Some(result.schema),
        partitionColumns,
        bucketSpec,
        provider,
        optionsWithPath,
        isExternal)
    }

    // Refresh the cache of the table in the catalog.
    hiveContext.sessionState.catalog.refreshTable(tableIdent)
    Seq.empty[Row]
  }
}
