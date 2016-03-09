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

package org.apache.spark.sql.hive

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.google.common.base.Objects
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hive.common.StatsSetupConst
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.{TableType => HiveTableType, Warehouse}
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.ql.metadata.{Table => HiveTable, _}
import org.apache.hadoop.hive.ql.plan.TableDesc

import org.apache.spark.Logging
import org.apache.spark.sql.{AnalysisException, SaveMode, SQLContext}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{Catalog, MultiInstanceRelation, OverrideCatalog}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.DataTypeParser
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.execution.{datasources, FileRelation}
import org.apache.spark.sql.execution.datasources.{Partition => ParquetPartition, _}
import org.apache.spark.sql.execution.datasources.parquet.{DefaultSource, ParquetRelation}
import org.apache.spark.sql.hive.client._
import org.apache.spark.sql.hive.execution.HiveNativeCommand
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

private[hive] case class HiveSerDe(
    inputFormat: Option[String] = None,
    outputFormat: Option[String] = None,
    serde: Option[String] = None)

private[hive] object HiveSerDe {
  /**
   * Get the Hive SerDe information from the data source abbreviation string or classname.
   *
   * @param source Currently the source abbreviation can be one of the following:
   *               SequenceFile, RCFile, ORC, PARQUET, and case insensitive.
   * @param hiveConf Hive Conf
   * @return HiveSerDe associated with the specified source
   */
  def sourceToSerDe(source: String, hiveConf: HiveConf): Option[HiveSerDe] = {
    val serdeMap = Map(
      "sequencefile" ->
        HiveSerDe(
          inputFormat = Option("org.apache.hadoop.mapred.SequenceFileInputFormat"),
          outputFormat = Option("org.apache.hadoop.mapred.SequenceFileOutputFormat")),

      "rcfile" ->
        HiveSerDe(
          inputFormat = Option("org.apache.hadoop.hive.ql.io.RCFileInputFormat"),
          outputFormat = Option("org.apache.hadoop.hive.ql.io.RCFileOutputFormat"),
          serde = Option(hiveConf.getVar(HiveConf.ConfVars.HIVEDEFAULTRCFILESERDE))),

      "orc" ->
        HiveSerDe(
          inputFormat = Option("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"),
          outputFormat = Option("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"),
          serde = Option("org.apache.hadoop.hive.ql.io.orc.OrcSerde")),

      "parquet" ->
        HiveSerDe(
          inputFormat = Option("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"),
          outputFormat = Option("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
          serde = Option("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")))

    val key = source.toLowerCase match {
      case s if s.startsWith("org.apache.spark.sql.parquet") => "parquet"
      case s if s.startsWith("org.apache.spark.sql.orc") => "orc"
      case s => s
    }

    serdeMap.get(key)
  }
}


// TODO: replace this with o.a.s.sql.hive.HiveCatalog once we merge SQLContext and HiveContext
private[hive] class HiveMetastoreCatalog(val client: HiveClient, hive: HiveContext)
  extends Catalog with Logging {

  val conf = hive.conf

  /** Usages should lock on `this`. */
  protected[hive] lazy val hiveWarehouse = new Warehouse(hive.hiveconf)

  /** A fully qualified identifier for a table (i.e., database.tableName) */
  case class QualifiedTableName(database: String, name: String)

  private def getQualifiedTableName(tableIdent: TableIdentifier): QualifiedTableName = {
    QualifiedTableName(
      tableIdent.database.getOrElse(client.currentDatabase).toLowerCase,
      tableIdent.table.toLowerCase)
  }

  private def getQualifiedTableName(t: CatalogTable): QualifiedTableName = {
    QualifiedTableName(
      t.specifiedDatabase.getOrElse(client.currentDatabase).toLowerCase,
      t.name.toLowerCase)
  }

  /** A cache of Spark SQL data source tables that have been accessed. */
  protected[hive] val cachedDataSourceTables: LoadingCache[QualifiedTableName, LogicalPlan] = {
    val cacheLoader = new CacheLoader[QualifiedTableName, LogicalPlan]() {
      override def load(in: QualifiedTableName): LogicalPlan = {
        logDebug(s"Creating new cached data source for $in")
        val table = client.getTable(in.database, in.name)

        def schemaStringFromParts: Option[String] = {
          table.properties.get("spark.sql.sources.schema.numParts").map { numParts =>
            val parts = (0 until numParts.toInt).map { index =>
              val part = table.properties.get(s"spark.sql.sources.schema.part.$index").orNull
              if (part == null) {
                throw new AnalysisException(
                  "Could not read schema from the metastore because it is corrupted " +
                    s"(missing part $index of the schema, $numParts parts are expected).")
              }

              part
            }
            // Stick all parts back to a single schema string.
            parts.mkString
          }
        }

        def getColumnNames(colType: String): Seq[String] = {
          table.properties.get(s"spark.sql.sources.schema.num${colType.capitalize}Cols").map {
            numCols => (0 until numCols.toInt).map { index =>
              table.properties.getOrElse(s"spark.sql.sources.schema.${colType}Col.$index",
                throw new AnalysisException(
                  s"Could not read $colType columns from the metastore because it is corrupted " +
                    s"(missing part $index of it, $numCols parts are expected)."))
            }
          }.getOrElse(Nil)
        }

        // Originally, we used spark.sql.sources.schema to store the schema of a data source table.
        // After SPARK-6024, we removed this flag.
        // Although we are not using spark.sql.sources.schema any more, we need to still support.
        val schemaString =
          table.properties.get("spark.sql.sources.schema").orElse(schemaStringFromParts)

        val userSpecifiedSchema =
          schemaString.map(s => DataType.fromJson(s).asInstanceOf[StructType])

        // We only need names at here since userSpecifiedSchema we loaded from the metastore
        // contains partition columns. We can always get datatypes of partitioning columns
        // from userSpecifiedSchema.
        val partitionColumns = getColumnNames("part")

        val bucketSpec = table.properties.get("spark.sql.sources.schema.numBuckets").map { n =>
          BucketSpec(n.toInt, getColumnNames("bucket"), getColumnNames("sort"))
        }

        val options = table.storage.serdeProperties
        val dataSource =
          DataSource(
            hive,
            userSpecifiedSchema = userSpecifiedSchema,
            partitionColumns = partitionColumns,
            bucketSpec = bucketSpec,
            className = table.properties("spark.sql.sources.provider"),
            options = options)

        LogicalRelation(
          dataSource.resolveRelation(),
          metastoreTableIdentifier = Some(TableIdentifier(in.name, Some(in.database))))
      }
    }

    CacheBuilder.newBuilder().maximumSize(1000).build(cacheLoader)
  }

  override def refreshTable(tableIdent: TableIdentifier): Unit = {
    // refreshTable does not eagerly reload the cache. It just invalidate the cache.
    // Next time when we use the table, it will be populated in the cache.
    // Since we also cache ParquetRelations converted from Hive Parquet tables and
    // adding converted ParquetRelations into the cache is not defined in the load function
    // of the cache (instead, we add the cache entry in convertToParquetRelation),
    // it is better at here to invalidate the cache to avoid confusing waring logs from the
    // cache loader (e.g. cannot find data source provider, which is only defined for
    // data source table.).
    invalidateTable(tableIdent)
  }

  def invalidateTable(tableIdent: TableIdentifier): Unit = {
    cachedDataSourceTables.invalidate(getQualifiedTableName(tableIdent))
  }

  def createDataSourceTable(
      tableIdent: TableIdentifier,
      userSpecifiedSchema: Option[StructType],
      partitionColumns: Array[String],
      bucketSpec: Option[BucketSpec],
      provider: String,
      options: Map[String, String],
      isExternal: Boolean): Unit = {
    val QualifiedTableName(dbName, tblName) = getQualifiedTableName(tableIdent)

    val tableProperties = new mutable.HashMap[String, String]
    tableProperties.put("spark.sql.sources.provider", provider)

    // Saves optional user specified schema.  Serialized JSON schema string may be too long to be
    // stored into a single metastore SerDe property.  In this case, we split the JSON string and
    // store each part as a separate SerDe property.
    userSpecifiedSchema.foreach { schema =>
      val threshold = conf.schemaStringLengthThreshold
      val schemaJsonString = schema.json
      // Split the JSON string.
      val parts = schemaJsonString.grouped(threshold).toSeq
      tableProperties.put("spark.sql.sources.schema.numParts", parts.size.toString)
      parts.zipWithIndex.foreach { case (part, index) =>
        tableProperties.put(s"spark.sql.sources.schema.part.$index", part)
      }
    }

    if (userSpecifiedSchema.isDefined && partitionColumns.length > 0) {
      tableProperties.put("spark.sql.sources.schema.numPartCols", partitionColumns.length.toString)
      partitionColumns.zipWithIndex.foreach { case (partCol, index) =>
        tableProperties.put(s"spark.sql.sources.schema.partCol.$index", partCol)
      }
    }

    if (userSpecifiedSchema.isDefined && bucketSpec.isDefined) {
      val BucketSpec(numBuckets, bucketColumnNames, sortColumnNames) = bucketSpec.get

      tableProperties.put("spark.sql.sources.schema.numBuckets", numBuckets.toString)
      tableProperties.put("spark.sql.sources.schema.numBucketCols",
        bucketColumnNames.length.toString)
      bucketColumnNames.zipWithIndex.foreach { case (bucketCol, index) =>
        tableProperties.put(s"spark.sql.sources.schema.bucketCol.$index", bucketCol)
      }

      if (sortColumnNames.nonEmpty) {
        tableProperties.put("spark.sql.sources.schema.numSortCols",
          sortColumnNames.length.toString)
        sortColumnNames.zipWithIndex.foreach { case (sortCol, index) =>
          tableProperties.put(s"spark.sql.sources.schema.sortCol.$index", sortCol)
        }
      }
    }

    if (userSpecifiedSchema.isEmpty && partitionColumns.length > 0) {
      // The table does not have a specified schema, which means that the schema will be inferred
      // when we load the table. So, we are not expecting partition columns and we will discover
      // partitions when we load the table. However, if there are specified partition columns,
      // we simply ignore them and provide a warning message.
      logWarning(
        s"The schema and partitions of table $tableIdent will be inferred when it is loaded. " +
          s"Specified partition columns (${partitionColumns.mkString(",")}) will be ignored.")
    }

    val tableType = if (isExternal) {
      tableProperties.put("EXTERNAL", "TRUE")
      CatalogTableType.EXTERNAL_TABLE
    } else {
      tableProperties.put("EXTERNAL", "FALSE")
      CatalogTableType.MANAGED_TABLE
    }

    val maybeSerDe = HiveSerDe.sourceToSerDe(provider, hive.hiveconf)
    val dataSource =
      DataSource(
        hive,
        userSpecifiedSchema = userSpecifiedSchema,
        partitionColumns = partitionColumns,
        bucketSpec = bucketSpec,
        className = provider,
        options = options)

    def newSparkSQLSpecificMetastoreTable(): CatalogTable = {
      CatalogTable(
        specifiedDatabase = Option(dbName),
        name = tblName,
        tableType = tableType,
        schema = Nil,
        storage = CatalogStorageFormat(
          locationUri = None,
          inputFormat = None,
          outputFormat = None,
          serde = None,
          serdeProperties = options
        ),
        properties = tableProperties.toMap)
    }

    def newHiveCompatibleMetastoreTable(
        relation: HadoopFsRelation,
        serde: HiveSerDe): CatalogTable = {
      assert(partitionColumns.isEmpty)
      assert(relation.partitionSchema.isEmpty)

      CatalogTable(
        specifiedDatabase = Option(dbName),
        name = tblName,
        tableType = tableType,
        storage = CatalogStorageFormat(
          locationUri = Some(relation.location.paths.map(_.toUri.toString).head),
          inputFormat = serde.inputFormat,
          outputFormat = serde.outputFormat,
          serde = serde.serde,
          serdeProperties = options
        ),
        schema = relation.schema.map { f =>
          CatalogColumn(f.name, HiveMetastoreTypes.toMetastoreType(f.dataType))
        },
        properties = tableProperties.toMap,
        viewText = None) // TODO: We need to place the SQL string here
    }

    // TODO: Support persisting partitioned data source relations in Hive compatible format
    val qualifiedTableName = tableIdent.quotedString
    val skipHiveMetadata = options.getOrElse("skipHiveMetadata", "false").toBoolean
    val (hiveCompatibleTable, logMessage) = (maybeSerDe, dataSource.resolveRelation()) match {
      case _ if skipHiveMetadata =>
        val message =
          s"Persisting partitioned data source relation $qualifiedTableName into " +
            "Hive metastore in Spark SQL specific format, which is NOT compatible with Hive."
        (None, message)

      case (Some(serde), relation: HadoopFsRelation)
        if relation.location.paths.length == 1 && relation.partitionSchema.isEmpty =>
        val hiveTable = newHiveCompatibleMetastoreTable(relation, serde)
        val message =
          s"Persisting data source relation $qualifiedTableName with a single input path " +
            s"into Hive metastore in Hive compatible format. Input path: " +
            s"${relation.location.paths.head}."
        (Some(hiveTable), message)

      case (Some(serde), relation: HadoopFsRelation) if relation.partitionSchema.nonEmpty =>
        val message =
          s"Persisting partitioned data source relation $qualifiedTableName into " +
            "Hive metastore in Spark SQL specific format, which is NOT compatible with Hive. " +
            "Input path(s): " + relation.location.paths.mkString("\n", "\n", "")
        (None, message)

      case (Some(serde), relation: HadoopFsRelation) =>
        val message =
          s"Persisting data source relation $qualifiedTableName with multiple input paths into " +
            "Hive metastore in Spark SQL specific format, which is NOT compatible with Hive. " +
            s"Input paths: " + relation.location.paths.mkString("\n", "\n", "")
        (None, message)

      case (Some(serde), _) =>
        val message =
          s"Data source relation $qualifiedTableName is not a " +
            s"${classOf[HadoopFsRelation].getSimpleName}. Persisting it into Hive metastore " +
            "in Spark SQL specific format, which is NOT compatible with Hive."
        (None, message)

      case _ =>
        val message =
          s"Couldn't find corresponding Hive SerDe for data source provider $provider. " +
            s"Persisting data source relation $qualifiedTableName into Hive metastore in " +
            s"Spark SQL specific format, which is NOT compatible with Hive."
        (None, message)
    }

    (hiveCompatibleTable, logMessage) match {
      case (Some(table), message) =>
        // We first try to save the metadata of the table in a Hive compatible way.
        // If Hive throws an error, we fall back to save its metadata in the Spark SQL
        // specific way.
        try {
          logInfo(message)
          client.createTable(table, ignoreIfExists = false)
        } catch {
          case throwable: Throwable =>
            val warningMessage =
              s"Could not persist $qualifiedTableName in a Hive compatible way. Persisting " +
                s"it into Hive metastore in Spark SQL specific format."
            logWarning(warningMessage, throwable)
            val sparkSqlSpecificTable = newSparkSQLSpecificMetastoreTable()
            client.createTable(sparkSqlSpecificTable, ignoreIfExists = false)
        }

      case (None, message) =>
        logWarning(message)
        val hiveTable = newSparkSQLSpecificMetastoreTable()
        client.createTable(hiveTable, ignoreIfExists = false)
    }
  }

  def hiveDefaultTableFilePath(tableIdent: TableIdentifier): String = {
    // Code based on: hiveWarehouse.getTablePath(currentDatabase, tableName)
    val QualifiedTableName(dbName, tblName) = getQualifiedTableName(tableIdent)
    new Path(new Path(client.getDatabase(dbName).locationUri), tblName).toString
  }

  override def tableExists(tableIdent: TableIdentifier): Boolean = {
    val QualifiedTableName(dbName, tblName) = getQualifiedTableName(tableIdent)
    client.getTableOption(dbName, tblName).isDefined
  }

  override def lookupRelation(
      tableIdent: TableIdentifier,
      alias: Option[String]): LogicalPlan = {
    val qualifiedTableName = getQualifiedTableName(tableIdent)
    val table = client.getTable(qualifiedTableName.database, qualifiedTableName.name)

    if (table.properties.get("spark.sql.sources.provider").isDefined) {
      val dataSourceTable = cachedDataSourceTables(qualifiedTableName)
      val tableWithQualifiers = SubqueryAlias(qualifiedTableName.name, dataSourceTable)
      // Then, if alias is specified, wrap the table with a Subquery using the alias.
      // Otherwise, wrap the table with a Subquery using the table name.
      alias.map(a => SubqueryAlias(a, tableWithQualifiers)).getOrElse(tableWithQualifiers)
    } else if (table.tableType == CatalogTableType.VIRTUAL_VIEW) {
      val viewText = table.viewText.getOrElse(sys.error("Invalid view without text."))
      alias match {
        // because hive use things like `_c0` to build the expanded text
        // currently we cannot support view from "create view v1(c1) as ..."
        case None => SubqueryAlias(table.name, hive.parseSql(viewText))
        case Some(aliasText) => SubqueryAlias(aliasText, hive.parseSql(viewText))
      }
    } else {
      MetastoreRelation(
        qualifiedTableName.database, qualifiedTableName.name, alias)(table, client, hive)
    }
  }

  private def convertToParquetRelation(metastoreRelation: MetastoreRelation): LogicalRelation = {
    val metastoreSchema = StructType.fromAttributes(metastoreRelation.output)
    val mergeSchema = hive.convertMetastoreParquetWithSchemaMerging

    val parquetOptions = Map(
      ParquetRelation.MERGE_SCHEMA -> mergeSchema.toString,
      ParquetRelation.METASTORE_TABLE_NAME -> TableIdentifier(
        metastoreRelation.tableName,
        Some(metastoreRelation.databaseName)
      ).unquotedString
    )
    val tableIdentifier =
      QualifiedTableName(metastoreRelation.databaseName, metastoreRelation.tableName)

    def getCached(
        tableIdentifier: QualifiedTableName,
        pathsInMetastore: Seq[String],
        schemaInMetastore: StructType,
        partitionSpecInMetastore: Option[PartitionSpec]): Option[LogicalRelation] = {
      cachedDataSourceTables.getIfPresent(tableIdentifier) match {
        case null => None // Cache miss
        case logical @ LogicalRelation(parquetRelation: HadoopFsRelation, _, _) =>
          // If we have the same paths, same schema, and same partition spec,
          // we will use the cached Parquet Relation.
          val useCached =
            parquetRelation.location.paths.map(_.toString).toSet == pathsInMetastore.toSet &&
            logical.schema.sameType(metastoreSchema) &&
            parquetRelation.partitionSpec == partitionSpecInMetastore.getOrElse {
              PartitionSpec(StructType(Nil), Array.empty[datasources.Partition])
            }

          if (useCached) {
            Some(logical)
          } else {
            // If the cached relation is not updated, we invalidate it right away.
            cachedDataSourceTables.invalidate(tableIdentifier)
            None
          }
        case other =>
          logWarning(
            s"${metastoreRelation.databaseName}.${metastoreRelation.tableName} should be stored " +
              s"as Parquet. However, we are getting a $other from the metastore cache. " +
              s"This cached entry will be invalidated.")
          cachedDataSourceTables.invalidate(tableIdentifier)
          None
      }
    }

    val result = if (metastoreRelation.hiveQlTable.isPartitioned) {
      val partitionSchema = StructType.fromAttributes(metastoreRelation.partitionKeys)
      val partitionColumnDataTypes = partitionSchema.map(_.dataType)
      // We're converting the entire table into ParquetRelation, so predicates to Hive metastore
      // are empty.
      val partitions = metastoreRelation.getHiveQlPartitions().map { p =>
        val location = p.getLocation
        val values = InternalRow.fromSeq(p.getValues.asScala.zip(partitionColumnDataTypes).map {
          case (rawValue, dataType) => Cast(Literal(rawValue), dataType).eval(null)
        })
        ParquetPartition(values, location)
      }
      val partitionSpec = PartitionSpec(partitionSchema, partitions)

      val cached = getCached(
        tableIdentifier,
        metastoreRelation.table.storage.locationUri.toSeq,
        metastoreSchema,
        Some(partitionSpec))

      val parquetRelation = cached.getOrElse {
        val paths = new Path(metastoreRelation.table.storage.locationUri.get) :: Nil
        val fileCatalog = new MetaStoreFileCatalog(hive, paths, partitionSpec)
        val format = new DefaultSource()
        val inferredSchema = format.inferSchema(hive, parquetOptions, fileCatalog.allFiles())

        val mergedSchema = inferredSchema.map { inferred =>
          ParquetRelation.mergeMetastoreParquetSchema(metastoreSchema, inferred)
        }.getOrElse(metastoreSchema)

        val relation = HadoopFsRelation(
          sqlContext = hive,
          location = fileCatalog,
          partitionSchema = partitionSchema,
          dataSchema = mergedSchema,
          bucketSpec = None, // We don't support hive bucketed tables, only ones we write out.
          fileFormat = new DefaultSource(),
          options = parquetOptions)

        val created = LogicalRelation(relation)
        cachedDataSourceTables.put(tableIdentifier, created)
        created
      }

      parquetRelation
    } else {
      val paths = Seq(metastoreRelation.hiveQlTable.getDataLocation.toString)

      val cached = getCached(tableIdentifier, paths, metastoreSchema, None)
      val parquetRelation = cached.getOrElse {
        val created =
          LogicalRelation(
            DataSource(
              sqlContext = hive,
              paths = paths,
              userSpecifiedSchema = Some(metastoreRelation.schema),
              options = parquetOptions,
              className = "parquet").resolveRelation())

        cachedDataSourceTables.put(tableIdentifier, created)
        created
      }

      parquetRelation
    }
    result.copy(expectedOutputAttributes = Some(metastoreRelation.output))
  }

  override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    val db = databaseName.getOrElse(client.currentDatabase)

    client.listTables(db).map(tableName => (tableName, false))
  }

  /**
   * When scanning or writing to non-partitioned Metastore Parquet tables, convert them to Parquet
   * data source relations for better performance.
   */
  object ParquetConversions extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      if (!plan.resolved || plan.analyzed) {
        return plan
      }

      plan transformUp {
        // Write path
        case InsertIntoTable(r: MetastoreRelation, partition, child, overwrite, ifNotExists)
          // Inserting into partitioned table is not supported in Parquet data source (yet).
          if !r.hiveQlTable.isPartitioned && hive.convertMetastoreParquet &&
            r.tableDesc.getSerdeClassName.toLowerCase.contains("parquet") =>
          val parquetRelation = convertToParquetRelation(r)
          InsertIntoTable(parquetRelation, partition, child, overwrite, ifNotExists)

        // Write path
        case InsertIntoHiveTable(r: MetastoreRelation, partition, child, overwrite, ifNotExists)
          // Inserting into partitioned table is not supported in Parquet data source (yet).
          if !r.hiveQlTable.isPartitioned && hive.convertMetastoreParquet &&
            r.tableDesc.getSerdeClassName.toLowerCase.contains("parquet") =>
          val parquetRelation = convertToParquetRelation(r)
          InsertIntoTable(parquetRelation, partition, child, overwrite, ifNotExists)

        // Read path
        case relation: MetastoreRelation if hive.convertMetastoreParquet &&
          relation.tableDesc.getSerdeClassName.toLowerCase.contains("parquet") =>
          val parquetRelation = convertToParquetRelation(relation)
          SubqueryAlias(relation.alias.getOrElse(relation.tableName), parquetRelation)
      }
    }
  }

  /**
   * Creates any tables required for query execution.
   * For example, because of a CREATE TABLE X AS statement.
   */
  object CreateTables extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      // Wait until children are resolved.
      case p: LogicalPlan if !p.childrenResolved => p
      case p: LogicalPlan if p.resolved => p

      case CreateViewAsSelect(table, child, allowExisting, replace, sql) if conf.nativeView =>
        if (allowExisting && replace) {
          throw new AnalysisException(
            "It is not allowed to define a view with both IF NOT EXISTS and OR REPLACE.")
        }

        val QualifiedTableName(dbName, tblName) = getQualifiedTableName(table)

        execution.CreateViewAsSelect(
          table.copy(
            specifiedDatabase = Some(dbName),
            name = tblName),
          child,
          allowExisting,
          replace)

      case CreateViewAsSelect(table, child, allowExisting, replace, sql) =>
        HiveNativeCommand(sql)

      case p @ CreateTableAsSelect(table, child, allowExisting) =>
        val schema = if (table.schema.nonEmpty) {
          table.schema
        } else {
          child.output.map { a =>
            CatalogColumn(a.name, HiveMetastoreTypes.toMetastoreType(a.dataType), a.nullable)
          }
        }

        val desc = table.copy(schema = schema)

        if (hive.convertCTAS && table.storage.serde.isEmpty) {
          // Do the conversion when spark.sql.hive.convertCTAS is true and the query
          // does not specify any storage format (file format and storage handler).
          if (table.specifiedDatabase.isDefined) {
            throw new AnalysisException(
              "Cannot specify database name in a CTAS statement " +
                "when spark.sql.hive.convertCTAS is set to true.")
          }

          val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists
          CreateTableUsingAsSelect(
            TableIdentifier(desc.name),
            conf.defaultDataSourceName,
            temporary = false,
            Array.empty[String],
            bucketSpec = None,
            mode,
            options = Map.empty[String, String],
            child
          )
        } else {
          val desc = if (table.storage.serde.isEmpty) {
            // add default serde
            table.withNewStorage(
              serde = Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
          } else {
            table
          }

          val QualifiedTableName(dbName, tblName) = getQualifiedTableName(table)

          execution.CreateTableAsSelect(
            desc.copy(
              specifiedDatabase = Some(dbName),
              name = tblName),
            child,
            allowExisting)
        }
    }
  }

  /**
   * Casts input data to correct data types according to table definition before inserting into
   * that table.
   */
  object PreInsertionCasts extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
      // Wait until children are resolved.
      case p: LogicalPlan if !p.childrenResolved => p

      case p @ InsertIntoTable(table: MetastoreRelation, _, child, _, _) =>
        castChildOutput(p, table, child)
    }

    def castChildOutput(p: InsertIntoTable, table: MetastoreRelation, child: LogicalPlan)
      : LogicalPlan = {
      val childOutputDataTypes = child.output.map(_.dataType)
      val numDynamicPartitions = p.partition.values.count(_.isEmpty)
      val tableOutputDataTypes =
        (table.attributes ++ table.partitionKeys.takeRight(numDynamicPartitions))
          .take(child.output.length).map(_.dataType)

      if (childOutputDataTypes == tableOutputDataTypes) {
        InsertIntoHiveTable(table, p.partition, p.child, p.overwrite, p.ifNotExists)
      } else if (childOutputDataTypes.size == tableOutputDataTypes.size &&
        childOutputDataTypes.zip(tableOutputDataTypes)
          .forall { case (left, right) => left.sameType(right) }) {
        // If both types ignoring nullability of ArrayType, MapType, StructType are the same,
        // use InsertIntoHiveTable instead of InsertIntoTable.
        InsertIntoHiveTable(table, p.partition, p.child, p.overwrite, p.ifNotExists)
      } else {
        // Only do the casting when child output data types differ from table output data types.
        val castedChildOutput = child.output.zip(table.output).map {
          case (input, output) if input.dataType != output.dataType =>
            Alias(Cast(input, output.dataType), input.name)()
          case (input, _) => input
        }

        p.copy(child = logical.Project(castedChildOutput, child))
      }
    }
  }

  /**
   * UNIMPLEMENTED: It needs to be decided how we will persist in-memory tables to the metastore.
   * For now, if this functionality is desired mix in the in-memory [[OverrideCatalog]].
   */
  override def registerTable(tableIdent: TableIdentifier, plan: LogicalPlan): Unit = {
    throw new UnsupportedOperationException
  }

  /**
   * UNIMPLEMENTED: It needs to be decided how we will persist in-memory tables to the metastore.
   * For now, if this functionality is desired mix in the in-memory [[OverrideCatalog]].
   */
  override def unregisterTable(tableIdent: TableIdentifier): Unit = {
    throw new UnsupportedOperationException
  }

  override def unregisterAllTables(): Unit = {}

  override def setCurrentDatabase(databaseName: String): Unit = {
    client.setCurrentDatabase(databaseName)
  }
}

/**
 * An override of the standard HDFS listing based catalog, that overrides the partition spec with
 * the information from the metastore.
 */
class MetaStoreFileCatalog(
    hive: HiveContext,
    paths: Seq[Path],
    partitionSpecFromHive: PartitionSpec)
  extends HDFSFileCatalog(hive, Map.empty, paths) {


  override def getStatus(path: Path): Array[FileStatus] = {
    val fs = path.getFileSystem(hive.sparkContext.hadoopConfiguration)
    fs.listStatus(path)
  }

  override def partitionSpec(schema: Option[StructType]): PartitionSpec = partitionSpecFromHive
}

/**
 * A logical plan representing insertion into Hive table.
 * This plan ignores nullability of ArrayType, MapType, StructType unlike InsertIntoTable
 * because Hive table doesn't have nullability for ARRAY, MAP, STRUCT types.
 */
private[hive] case class InsertIntoHiveTable(
    table: MetastoreRelation,
    partition: Map[String, Option[String]],
    child: LogicalPlan,
    overwrite: Boolean,
    ifNotExists: Boolean)
  extends LogicalPlan {

  override def children: Seq[LogicalPlan] = child :: Nil
  override def output: Seq[Attribute] = Seq.empty

  val numDynamicPartitions = partition.values.count(_.isEmpty)

  // This is the expected schema of the table prepared to be inserted into,
  // including dynamic partition columns.
  val tableOutput = table.attributes ++ table.partitionKeys.takeRight(numDynamicPartitions)

  override lazy val resolved: Boolean = childrenResolved && child.output.zip(tableOutput).forall {
    case (childAttr, tableAttr) => childAttr.dataType.sameType(tableAttr.dataType)
  }
}

private[hive] case class MetastoreRelation(
    databaseName: String,
    tableName: String,
    alias: Option[String])
    (val table: CatalogTable,
     @transient private val client: HiveClient,
     @transient private val sqlContext: SQLContext)
  extends LeafNode with MultiInstanceRelation with FileRelation {

  override def equals(other: Any): Boolean = other match {
    case relation: MetastoreRelation =>
      databaseName == relation.databaseName &&
        tableName == relation.tableName &&
        alias == relation.alias &&
        output == relation.output
    case _ => false
  }

  override def hashCode(): Int = {
    Objects.hashCode(databaseName, tableName, alias, output)
  }

  override protected def otherCopyArgs: Seq[AnyRef] = table :: sqlContext :: Nil

  private def toHiveColumn(c: CatalogColumn): FieldSchema = {
    new FieldSchema(c.name, c.dataType, c.comment.orNull)
  }

  // TODO: merge this with HiveClientImpl#toHiveTable
  @transient val hiveQlTable: HiveTable = {
    // We start by constructing an API table as Hive performs several important transformations
    // internally when converting an API table to a QL table.
    val tTable = new org.apache.hadoop.hive.metastore.api.Table()
    tTable.setTableName(table.name)
    tTable.setDbName(table.database)

    val tableParameters = new java.util.HashMap[String, String]()
    tTable.setParameters(tableParameters)
    table.properties.foreach { case (k, v) => tableParameters.put(k, v) }

    tTable.setTableType(table.tableType match {
      case CatalogTableType.EXTERNAL_TABLE => HiveTableType.EXTERNAL_TABLE.toString
      case CatalogTableType.MANAGED_TABLE => HiveTableType.MANAGED_TABLE.toString
      case CatalogTableType.INDEX_TABLE => HiveTableType.INDEX_TABLE.toString
      case CatalogTableType.VIRTUAL_VIEW => HiveTableType.VIRTUAL_VIEW.toString
    })

    val sd = new org.apache.hadoop.hive.metastore.api.StorageDescriptor()
    tTable.setSd(sd)
    sd.setCols(table.schema.map(toHiveColumn).asJava)
    tTable.setPartitionKeys(table.partitionColumns.map(toHiveColumn).asJava)

    table.storage.locationUri.foreach(sd.setLocation)
    table.storage.inputFormat.foreach(sd.setInputFormat)
    table.storage.outputFormat.foreach(sd.setOutputFormat)

    val serdeInfo = new org.apache.hadoop.hive.metastore.api.SerDeInfo
    table.storage.serde.foreach(serdeInfo.setSerializationLib)
    sd.setSerdeInfo(serdeInfo)

    val serdeParameters = new java.util.HashMap[String, String]()
    table.storage.serdeProperties.foreach { case (k, v) => serdeParameters.put(k, v) }
    serdeInfo.setParameters(serdeParameters)

    new HiveTable(tTable)
  }

  @transient override lazy val statistics: Statistics = Statistics(
    sizeInBytes = {
      val totalSize = hiveQlTable.getParameters.get(StatsSetupConst.TOTAL_SIZE)
      val rawDataSize = hiveQlTable.getParameters.get(StatsSetupConst.RAW_DATA_SIZE)
      // TODO: check if this estimate is valid for tables after partition pruning.
      // NOTE: getting `totalSize` directly from params is kind of hacky, but this should be
      // relatively cheap if parameters for the table are populated into the metastore.  An
      // alternative would be going through Hadoop's FileSystem API, which can be expensive if a lot
      // of RPCs are involved.  Besides `totalSize`, there are also `numFiles`, `numRows`,
      // `rawDataSize` keys (see StatsSetupConst in Hive) that we can look at in the future.
      BigInt(
        // When table is external,`totalSize` is always zero, which will influence join strategy
        // so when `totalSize` is zero, use `rawDataSize` instead
        // if the size is still less than zero, we use default size
        Option(totalSize).map(_.toLong).filter(_ > 0)
          .getOrElse(Option(rawDataSize).map(_.toLong).filter(_ > 0)
          .getOrElse(sqlContext.conf.defaultSizeInBytes)))
    }
  )

  // When metastore partition pruning is turned off, we cache the list of all partitions to
  // mimic the behavior of Spark < 1.5
  private lazy val allPartitions: Seq[CatalogTablePartition] = client.getAllPartitions(table)

  def getHiveQlPartitions(predicates: Seq[Expression] = Nil): Seq[Partition] = {
    val rawPartitions = if (sqlContext.conf.metastorePartitionPruning) {
      client.getPartitionsByFilter(table, predicates)
    } else {
      allPartitions
    }

    rawPartitions.map { p =>
      val tPartition = new org.apache.hadoop.hive.metastore.api.Partition
      tPartition.setDbName(databaseName)
      tPartition.setTableName(tableName)
      tPartition.setValues(p.spec.values.toList.asJava)

      val sd = new org.apache.hadoop.hive.metastore.api.StorageDescriptor()
      tPartition.setSd(sd)
      sd.setCols(table.schema.map(toHiveColumn).asJava)
      p.storage.locationUri.foreach(sd.setLocation)
      p.storage.inputFormat.foreach(sd.setInputFormat)
      p.storage.outputFormat.foreach(sd.setOutputFormat)

      val serdeInfo = new org.apache.hadoop.hive.metastore.api.SerDeInfo
      sd.setSerdeInfo(serdeInfo)
      // maps and lists should be set only after all elements are ready (see HIVE-7975)
      p.storage.serde.foreach(serdeInfo.setSerializationLib)

      val serdeParameters = new java.util.HashMap[String, String]()
      table.storage.serdeProperties.foreach { case (k, v) => serdeParameters.put(k, v) }
      p.storage.serdeProperties.foreach { case (k, v) => serdeParameters.put(k, v) }
      serdeInfo.setParameters(serdeParameters)

      new Partition(hiveQlTable, tPartition)
    }
  }

  /** Only compare database and tablename, not alias. */
  override def sameResult(plan: LogicalPlan): Boolean = {
    plan match {
      case mr: MetastoreRelation =>
        mr.databaseName == databaseName && mr.tableName == tableName
      case _ => false
    }
  }

  val tableDesc = new TableDesc(
    hiveQlTable.getInputFormatClass,
    // The class of table should be org.apache.hadoop.hive.ql.metadata.Table because
    // getOutputFormatClass will use HiveFileFormatUtils.getOutputFormatSubstitute to
    // substitute some output formats, e.g. substituting SequenceFileOutputFormat to
    // HiveSequenceFileOutputFormat.
    hiveQlTable.getOutputFormatClass,
    hiveQlTable.getMetadata
  )

  implicit class SchemaAttribute(f: CatalogColumn) {
    def toAttribute: AttributeReference = AttributeReference(
      f.name,
      HiveMetastoreTypes.toDataType(f.dataType),
      // Since data can be dumped in randomly with no validation, everything is nullable.
      nullable = true
    )(qualifiers = Seq(alias.getOrElse(tableName)))
  }

  /** PartitionKey attributes */
  val partitionKeys = table.partitionColumns.map(_.toAttribute)

  /** Non-partitionKey attributes */
  val attributes = table.schema.map(_.toAttribute)

  val output = attributes ++ partitionKeys

  /** An attribute map that can be used to lookup original attributes based on expression id. */
  val attributeMap = AttributeMap(output.map(o => (o, o)))

  /** An attribute map for determining the ordinal for non-partition columns. */
  val columnOrdinals = AttributeMap(attributes.zipWithIndex)

  override def inputFiles: Array[String] = {
    val partLocations = client
      .getPartitionsByFilter(table, Nil)
      .flatMap(_.storage.locationUri)
      .toArray
    if (partLocations.nonEmpty) {
      partLocations
    } else {
      Array(
        table.storage.locationUri.getOrElse(
          sys.error(s"Could not get the location of ${table.qualifiedName}.")))
    }
  }


  override def newInstance(): MetastoreRelation = {
    MetastoreRelation(databaseName, tableName, alias)(table, client, sqlContext)
  }
}


private[hive] object HiveMetastoreTypes {
  def toDataType(metastoreType: String): DataType = DataTypeParser.parse(metastoreType)

  def decimalMetastoreString(decimalType: DecimalType): String = decimalType match {
    case DecimalType.Fixed(precision, scale) => s"decimal($precision,$scale)"
    case _ => s"decimal($HiveShim.UNLIMITED_DECIMAL_PRECISION,$HiveShim.UNLIMITED_DECIMAL_SCALE)"
  }

  def toMetastoreType(dt: DataType): String = dt match {
    case ArrayType(elementType, _) => s"array<${toMetastoreType(elementType)}>"
    case StructType(fields) =>
      s"struct<${fields.map(f => s"${f.name}:${toMetastoreType(f.dataType)}").mkString(",")}>"
    case MapType(keyType, valueType, _) =>
      s"map<${toMetastoreType(keyType)},${toMetastoreType(valueType)}>"
    case StringType => "string"
    case FloatType => "float"
    case IntegerType => "int"
    case ByteType => "tinyint"
    case ShortType => "smallint"
    case DoubleType => "double"
    case LongType => "bigint"
    case BinaryType => "binary"
    case BooleanType => "boolean"
    case DateType => "date"
    case d: DecimalType => decimalMetastoreString(d)
    case TimestampType => "timestamp"
    case NullType => "void"
    case udt: UserDefinedType[_] => toMetastoreType(udt.sqlType)
  }
}
