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

package org.apache.spark.sql.catalyst.analysis

import scala.collection.mutable.{HashMap, HashSet}

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.CONFIG
import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalyst.{SQLConfHelper, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStorageFormat, CatalogTable, CatalogTableType, CatalogUtils, ClusterBySpec}
import org.apache.spark.sql.catalyst.expressions.{Alias, AliasHelper, RowOrdering}
import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, GeneratedColumn, IdentityColumn, ResolveDefaultColumns, WriteDistributionAndOrdering}
import org.apache.spark.sql.catalyst.util.{ResolveDefaultColumns => DefaultCols}
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns._
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.WriteDistributionMode
import org.apache.spark.sql.connector.expressions.{FieldReference, LogicalExpressions, NamedReference, RewritableTransform, SortOrder => V2SortOrder, Transform}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.{CreateDataSourceTableCommand, DDLUtils}
import org.apache.spark.sql.execution.datasources.{BucketingUtils, CreateTable => CreateTableV1, PartitioningUtils}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Utils
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf}
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StructType}
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.util.ArrayImplicits._

/** Lowering, preprocessing and validation of CREATE TABLE and REPLACE TABLE plans. */
private[sql] object CreateTableHelper extends SQLConfHelper with Logging with AliasHelper {

  /** Lowers a resolved session-catalog CREATE TABLE when its provider uses V1 commands. */
  private[analysis] def lowerCreateTable(
      createTable: CreateTable): LogicalPlan = createTable match {
    case c @ CreateTable(ResolvedV1CommandIdentifier(ident), _, _, tableSpec: TableSpec, _, _, _)
        if c.resolved && c.columns.forall(_.isDefaultValueTypeCoerced) =>
      val (storageFormat, provider) = getStorageFormatAndProvider(
        c.tableSpec.provider, tableSpec.options, c.tableSpec.location, c.tableSpec.serde,
        ctas = false)
      if (!isV2Provider(provider)) {
        if (tableSpec.constraints.nonEmpty) {
          throw QueryCompilationErrors.unsupportedTableOperationError(
            ident, "CONSTRAINT")
        }
        failIfWriteDistributionOrOrdering(
          ident, "CREATE TABLE", c.writeDistributionMode, c.writeOrdering)
        // For V1 CREATE TABLE command, the default value expression is hidden in the
        // StructField metadata, and won't be constant folded by the optimizer. Here we
        // manually constant fold it, as exist default needs to be a constant.
        val fields = c.columns.map { col =>
          col.defaultValue.map { defaultValue =>
            val existsDefaultSQL = constantFoldDefaultValue(
              defaultValue, col.name, col.dataType, "CREATE TABLE")
            // toV1Column already sets CURRENT_DEFAULT, so just update EXISTS_DEFAULT
            val field = col.toV1Column
            val newMetadata = new MetadataBuilder().withMetadata(field.metadata)
              .putString(DefaultCols.EXISTS_DEFAULT_COLUMN_METADATA_KEY, existsDefaultSQL).build()
            field.copy(metadata = newMetadata)
          }.getOrElse(col.toV1Column)
        }
        constructV1TableCmd(None, c.tableSpec, ident, StructType(fields), c.partitioning,
          c.ignoreIfExists, storageFormat, provider)
      } else {
        c
      }

    case _ => createTable
  }

  /** Rejects REPLACE TABLE for a session-catalog provider that only supports V1 commands. */
  private[analysis] def lowerReplaceTable(
      replaceTable: ReplaceTable): LogicalPlan = replaceTable match {
    case c @ ReplaceTable(ResolvedV1CommandIdentifier(ident), _, _, _, _, _, _) if c.resolved =>
      val provider = c.tableSpec.provider.getOrElse(conf.defaultDataSourceName)
      if (!isV2Provider(provider)) {
        throw QueryCompilationErrors.unsupportedTableOperationError(
          ident, "REPLACE TABLE")
      } else {
        c
      }

    case _ => replaceTable
  }

  private[analysis] def constructV1TableCmd(
      query: Option[LogicalPlan],
      tableSpec: TableSpecBase,
      ident: TableIdentifier,
      tableSchema: StructType,
      partitioning: Seq[Transform],
      ignoreIfExists: Boolean,
      storageFormat: CatalogStorageFormat,
      provider: String): CreateTableV1 = {
    val tableDesc = buildCatalogTable(
      ident, tableSchema, partitioning, tableSpec.properties, provider, tableSpec.location,
      tableSpec.comment, tableSpec.collation, storageFormat, tableSpec.external)
    val mode = if (ignoreIfExists) SaveMode.Ignore else SaveMode.ErrorIfExists
    CreateTableV1(tableDesc, mode, query)
  }

  private[analysis] def buildStorageFormatFromSerdeInfo(
      location: Option[String],
      maybeSerdeInfo: Option[SerdeInfo]): CatalogStorageFormat = {
    HiveSerDe.buildStorageFormat(
      location,
      maybeSerdeInfo,
      si => QueryCompilationErrors.invalidFileFormatForStoredAsError(si))
  }

  private[analysis] def getStorageFormatAndProvider(
      provider: Option[String],
      options: Map[String, String],
      location: Option[String],
      maybeSerdeInfo: Option[SerdeInfo],
      ctas: Boolean): (CatalogStorageFormat, String) = {
    val nonHiveStorageFormat = CatalogStorageFormat.empty.copy(
      locationUri = location.map(CatalogUtils.stringToURI),
      properties = options)
    val defaultHiveStorage = HiveSerDe.getDefaultStorage(conf).copy(
      locationUri = location.map(CatalogUtils.stringToURI),
      properties = options)
    val tableProvider = DataSourceV2Utils.createTableProvider(
      provider, maybeSerdeInfo.isDefined, ctas, conf)

    if (provider.isDefined) {
      // The parser guarantees that USING and STORED AS/ROW FORMAT won't co-exist.
      if (maybeSerdeInfo.isDefined) {
        throw SparkException.internalError(
          s"Cannot create table with both USING ${provider.get} and " +
            s"${maybeSerdeInfo.get.describe}")
      }
      (nonHiveStorageFormat, tableProvider)
    } else if (maybeSerdeInfo.isDefined) {
      val serdeInfo = maybeSerdeInfo.get
      SerdeInfo.checkSerdePropMerging(serdeInfo.serdeProperties, defaultHiveStorage.properties)
      val storageFormat = if (serdeInfo.storedAs.isDefined) {
        // If `STORED AS fileFormat` is used, infer inputFormat, outputFormat and serde from it.
        HiveSerDe.sourceToSerDe(serdeInfo.storedAs.get) match {
          case Some(hiveSerde) =>
            defaultHiveStorage.copy(
              inputFormat = hiveSerde.inputFormat.orElse(defaultHiveStorage.inputFormat),
              outputFormat = hiveSerde.outputFormat.orElse(defaultHiveStorage.outputFormat),
              // User specified serde takes precedence over the one inferred from file format.
              serde = serdeInfo.serde.orElse(hiveSerde.serde).orElse(defaultHiveStorage.serde),
              properties = serdeInfo.serdeProperties ++ defaultHiveStorage.properties)
          case _ => throw QueryCompilationErrors.invalidFileFormatForStoredAsError(serdeInfo)
        }
      } else {
        defaultHiveStorage.copy(
          inputFormat =
            serdeInfo.formatClasses.map(_.input).orElse(defaultHiveStorage.inputFormat),
          outputFormat =
            serdeInfo.formatClasses.map(_.output).orElse(defaultHiveStorage.outputFormat),
          serde = serdeInfo.serde.orElse(defaultHiveStorage.serde),
          properties = serdeInfo.serdeProperties ++ defaultHiveStorage.properties)
      }
      (storageFormat, tableProvider)
    } else {
      // If neither USING nor STORED AS/ROW FORMAT is specified, we create native data source
      // tables if:
      //   1. `LEGACY_CREATE_HIVE_TABLE_BY_DEFAULT` is false, or
      //   2. It's a CTAS and `conf.convertCTAS` is true.
      if (!DataSourceV2Utils.createsHiveTableByDefault(ctas, conf)) {
        (nonHiveStorageFormat, tableProvider)
      } else {
        logWarning(log"A Hive serde table will be created as there is no table provider " +
          log"specified. You can set " +
          log"${MDC(CONFIG, SQLConf.LEGACY_CREATE_HIVE_TABLE_BY_DEFAULT.key)} to false so that " +
          log"native data source table will be created instead.")
        (defaultHiveStorage, tableProvider)
      }
    }
  }

  private def buildCatalogTable(
      table: TableIdentifier,
      schema: StructType,
      partitioning: Seq[Transform],
      properties: Map[String, String],
      provider: String,
      location: Option[String],
      comment: Option[String],
      collation: Option[String],
      storageFormat: CatalogStorageFormat,
      external: Boolean): CatalogTable = {
    val tableType = if (external || location.isDefined) {
      CatalogTableType.EXTERNAL
    } else {
      CatalogTableType.MANAGED
    }
    val (partitionColumns, maybeBucketSpec, maybeClusterBySpec) = partitioning.convertTransforms

    CatalogTable(
      identifier = table,
      tableType = tableType,
      storage = storageFormat,
      schema = schema,
      provider = Some(provider),
      partitionColumnNames = partitionColumns,
      bucketSpec = maybeBucketSpec,
      properties = properties ++
        maybeClusterBySpec.map(
          clusterBySpec => ClusterBySpec.toProperty(schema, clusterBySpec, conf.resolver)),
      comment = comment,
      collation = collation
    )
  }

  /** Rejects a write distribution or ordering, which a v1 table cannot record. */
  private[analysis] def failIfWriteDistributionOrOrdering(
      ident: TableIdentifier,
      operation: String,
      writeDistributionMode: WriteDistributionMode,
      writeOrdering: Seq[V2SortOrder]): Unit = {
    if (WriteDistributionAndOrdering.isRequested(writeDistributionMode, writeOrdering)) {
      throw QueryCompilationErrors.unsupportedTableOperationError(
        ident, s"$operation ... ${WriteDistributionAndOrdering.CLAUSES}")
    }
  }

  private[analysis] def isV2Provider(provider: String): Boolean = {
    DataSourceV2Utils.isV2Provider(provider, conf)
  }

  /** Validates and constant-folds a V1 column default for EXISTS_DEFAULT metadata. */
  private[analysis] def constantFoldDefaultValue(
      defaultValue: DefaultValueExpression,
      colName: String,
      colType: DataType,
      statementType: String): String = {
    validateDefaultValueExpr(defaultValue, statementType, colName, Some(colType))
    val fakePlan = Project(Seq(Alias(defaultValue.child, colName)()), OneRowRelation())
    val optimizedPlan = ConstantFolding(DefaultColumnOptimizer.FinishAnalysis(fakePlan))
    val constantFolded = optimizedPlan.collectFirst {
      case Project(Seq(expr), OneRowRelation()) => trimAliases(expr)
    }.get
    if (!constantFolded.foldable) {
      throw QueryCompilationErrors.defaultValueNotConstantError(statementType, colName,
        defaultValue.originalSQL)
    }
    constantFolded.sql
  }

  /** Checks partitioning and bucketing when a V1 CREATE TABLE has no schema. */
  def checkCreateTableWithoutSchema(createTable: CreateTableV1): CreateTableV1 = {
    val tableDesc = createTable.tableDesc
    if (tableDesc.bucketSpec.isDefined) {
      throw new AnalysisException(
        errorClass = "SPECIFY_BUCKETING_IS_NOT_ALLOWED",
        messageParameters = Map.empty
      )
    }
    if (tableDesc.partitionColumnNames.nonEmpty) {
      throw new AnalysisException(
        errorClass = "SPECIFY_PARTITION_IS_NOT_ALLOWED",
        messageParameters = Map.empty)
    }
    createTable
  }

  /** Normalizes a V1 CREATE TABLE schema and places partition columns last. */
  def normalizeCreateTableWithoutQuery(createTable: CreateTableV1): CreateTableV1 = {
    val tableDesc = createTable.tableDesc
    DDLUtils.checkTableColumns(tableDesc)

    val normalizedTable = normalizeCatalogTable(tableDesc.schema, tableDesc)

    val normalizedSchemaByName = HashMap(normalizedTable.schema.map(s => s.name -> s): _*)
    val partitionSchema = normalizedTable.partitionColumnNames.map { partCol =>
      normalizedSchemaByName(partCol)
    }
    val partitionSchemaSet = HashSet(partitionSchema: _*)
    val reorderedSchema = StructType(
      normalizedTable.schema.filterNot(partitionSchemaSet.contains) ++ partitionSchema
    )

    createTable.copy(tableDesc = normalizedTable.copy(schema = reorderedSchema))
  }

  /** Validates and normalizes a V2 table's columns, partitioning and write ordering. */
  def preprocessV2CreateTablePlan(create: V2CreateTablePlan): LogicalPlan = {
    val schema = create.tableSchema
    val partitioning = create.partitioning
    val isCaseSensitive = conf.caseSensitiveAnalysis
    // Check that columns are not duplicated in the schema
    val flattenedSchema = SchemaUtils.explodeNestedFieldNames(schema)
    SchemaUtils.checkColumnNameDuplication(
      flattenedSchema,
      isCaseSensitive)

    // Check that columns are not duplicated in the partitioning statement
    SchemaUtils.checkTransformDuplication(
      partitioning, "in the partitioning", isCaseSensitive)

    // The write ordering has no duplicate check: a repeated sort key is redundant, and
    // `bucket(4, id), bucket(8, id)` are distinct keys.

    if (schema.isEmpty) {
      if (partitioning.nonEmpty) {
        throw QueryCompilationErrors.specifyPartitionNotAllowedWhenTableSchemaNotDefinedError()
      }
      if (create.writeOrdering.nonEmpty) {
        throw QueryCompilationErrors
          .specifyWriteOrderingNotAllowedWhenTableSchemaNotDefinedError(create.origin)
      }

      create
    } else {
      // Resolve and normalize partition columns as necessary
      val resolver = conf.resolver
      val normalizedPartitions = partitioning.map {
        case transform: RewritableTransform =>
          val rewritten = transform.references().map { ref =>
            // Throws an exception if the reference cannot be resolved
            val position = SchemaUtils
              .findColumnPosition(ref.fieldNames().toImmutableArraySeq, schema, resolver)
            FieldReference(SchemaUtils.getColumnName(position, schema))
          }
          transform.withReferences(rewritten.toImmutableArraySeq)
        case other => other
      }

      // Unlike the partitioning, an ordering reference to a missing column, including a path
      // through a non-struct field, is kept as is for CheckAnalysis to report, and each
      // reference is normalized independently.
      def normalizeResolvable(ref: NamedReference): NamedReference = {
        WriteDistributionAndOrdering
          .findColumn(schema, ref.fieldNames().toImmutableArraySeq, resolver)
          .map { case (path, field) => FieldReference(path :+ field.name) }
          .getOrElse(ref)
      }

      val normalizedOrdering = create.writeOrdering.map { sortOrder =>
        val key = sortOrder.expression() match {
          case ref: NamedReference => normalizeResolvable(ref)
          case transform: RewritableTransform =>
            transform.withReferences(
              transform.references().map(normalizeResolvable).toImmutableArraySeq)
          case other => other
        }
        LogicalExpressions.sort(key, sortOrder.direction(), sortOrder.nullOrdering())
      }

      create.withPartitioning(normalizedPartitions).withWriteOrdering(normalizedOrdering)
    }
  }

  def normalizeCatalogTable(schema: StructType, table: CatalogTable): CatalogTable = {
    SchemaUtils.checkSchemaColumnNameDuplication(
      schema,
      conf.caseSensitiveAnalysis)
    if (!conf.allowCollationsInMapKeys) {
      SchemaUtils.checkNoCollationsInMapKeys(schema)
    }

    val normalizedPartCols = normalizePartitionColumns(schema, table)
    val normalizedBucketSpec = normalizeBucketSpec(schema, table)

    normalizedBucketSpec.foreach { spec =>
      for (bucketCol <- spec.bucketColumnNames if normalizedPartCols.contains(bucketCol)) {
        throw QueryCompilationErrors.bucketingColumnCannotBePartOfPartitionColumnsError(
          bucketCol, normalizedPartCols)
      }
      for (sortCol <- spec.sortColumnNames if normalizedPartCols.contains(sortCol)) {
        throw QueryCompilationErrors.bucketSortingColumnCannotBePartOfPartitionColumnsError(
          sortCol, normalizedPartCols)
      }
    }

    val normalizedProperties = table.properties ++ table.clusterBySpec.map { spec =>
      ClusterBySpec.toProperty(schema, spec, conf.resolver)
    }

    table.copy(partitionColumnNames = normalizedPartCols, bucketSpec = normalizedBucketSpec,
      properties = normalizedProperties)
  }

  private def normalizePartitionColumns(schema: StructType, table: CatalogTable): Seq[String] = {
    val normalizedPartitionCols = CatalogUtils.normalizePartCols(
      tableName = table.identifier.unquotedString,
      tableCols = schema.map(_.name),
      partCols = table.partitionColumnNames,
      resolver = conf.resolver)

    SchemaUtils.checkColumnNameDuplication(normalizedPartitionCols, conf.resolver)

    if (schema.nonEmpty && normalizedPartitionCols.length == schema.length) {
      throw new AnalysisException(
        errorClass = "ALL_PARTITION_COLUMNS_NOT_ALLOWED",
        messageParameters = Map.empty)
    }

    val normalizedPartitionColsSet = HashSet(normalizedPartitionCols: _*)
    schema
      .filter(f => normalizedPartitionColsSet.contains(f.name))
      .foreach { field =>
        if (!PartitioningUtils.canPartitionOn(field.dataType)) {
          throw QueryCompilationErrors.invalidPartitionColumnDataTypeError(field)
        }
      }

    normalizedPartitionCols
  }

  private def normalizeBucketSpec(schema: StructType, table: CatalogTable): Option[BucketSpec] = {
    table.bucketSpec match {
      case Some(bucketSpec) =>
        val normalizedBucketSpec = CatalogUtils.normalizeBucketSpec(
          tableName = table.identifier.unquotedString,
          tableCols = schema.map(_.name),
          bucketSpec = bucketSpec,
          resolver = conf.resolver)

        SchemaUtils.checkColumnNameDuplication(
          normalizedBucketSpec.bucketColumnNames,
          conf.resolver)
        SchemaUtils.checkColumnNameDuplication(
          normalizedBucketSpec.sortColumnNames,
          conf.resolver)

        normalizedBucketSpec.sortColumnNames.map(schema(_)).map(_.dataType).foreach {
          case dt if RowOrdering.isOrderable(dt) => // OK
          case other => failAnalysis(s"Cannot use ${other.catalogString} for sorting column")
        }

        schema.filter(f => normalizedBucketSpec.bucketColumnNames.contains(f.name))
          .foreach { field =>
            if (!BucketingUtils.canBucketOn(field.dataType)) {
              throw QueryCompilationErrors.invalidBucketColumnDataTypeError(field.dataType)
            }
          }

        Some(normalizedBucketSpec)

      case None => None
    }
  }

  private def failAnalysis(msg: String) = {
    throw new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_3072", messageParameters = Map("msg" -> msg))
  }

  /** Validates a V1 datasource table and builds its CREATE TABLE command. */
  def createDataSourceTableCommand(
      tableDesc: CatalogTable,
      mode: SaveMode): CreateDataSourceTableCommand = {
    ResolveDefaultColumns.validateTableProviderForDefaultValue(
      tableDesc.schema, tableDesc.provider, "CREATE TABLE", false)

    if (GeneratedColumn.hasGeneratedColumns(tableDesc.schema)) {
      throw QueryCompilationErrors.unsupportedTableOperationError(
        tableDesc.identifier, "generated columns")
    }

    if (IdentityColumn.hasIdentityColumns(tableDesc.schema)) {
      throw QueryCompilationErrors.unsupportedTableOperationError(
        tableDesc.identifier, "identity columns")
    }

    CreateDataSourceTableCommand(tableDesc, ignoreIfExists = mode == SaveMode.Ignore)
  }

  private[analysis] def replaceCharWithVarcharInColumn(col: ColumnDefinition): ColumnDefinition = {
    col.copy(dataType = CharVarcharUtils.replaceCharWithVarchar(col.dataType))
  }

  private[analysis] def replaceCharWithVarcharInTableMeta(tbl: CatalogTable): CatalogTable = {
    tbl.copy(schema = CharVarcharUtils.replaceCharWithVarchar(tbl.schema).asInstanceOf[StructType])
  }
}
