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

package org.apache.spark.sql.execution.datasources

import java.util.Locale

import scala.collection.mutable.{HashMap, HashSet}
import scala.jdk.CollectionConverters._

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{Collate, Collation, Expression, InputFileBlockLength, InputFileBlockStart, InputFileName, RowOrdering}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.catalyst.util.TypeUtils._
import org.apache.spark.sql.connector.expressions.{FieldReference, RewritableTransform}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.command.ViewHelper.generateViewProperties
import org.apache.spark.sql.execution.datasources.{CreateTable => CreateTableV1}
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.PartitioningUtils.normalizePartitionSpec
import org.apache.spark.sql.util.SchemaUtils
import org.apache.spark.util.ArrayImplicits._

/**
 * Replaces [[UnresolvedRelation]]s if the plan is for direct query on files.
 */
class ResolveSQLOnFile(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  private def maybeSQLFile(u: UnresolvedRelation): Boolean = {
    conf.runSQLonFile && u.multipartIdentifier.size == 2
  }

  private def resolveDataSource(unresolved: UnresolvedRelation): DataSource = {
    val ident = unresolved.multipartIdentifier
    val dataSource = DataSource(
      sparkSession,
      paths = Seq(CatalogUtils.stringToURI(ident.last).toString),
      className = ident.head,
      options = unresolved.options.asScala.toMap)
    // `dataSource.providingClass` may throw ClassNotFoundException, the caller side will try-catch
    // it and return the original plan, so that the analyzer can report table not found later.
    val isFileFormat = classOf[FileFormat].isAssignableFrom(dataSource.providingClass)
    if (!isFileFormat ||
      dataSource.className.toLowerCase(Locale.ROOT) == DDLUtils.HIVE_PROVIDER) {
      unresolved.failAnalysis(
        errorClass = "UNSUPPORTED_DATASOURCE_FOR_DIRECT_QUERY",
        messageParameters = Map("dataSourceType" -> ident.head))
    }
    dataSource
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case r @ RelationTimeTravel(u: UnresolvedRelation, timestamp, _)
        if maybeSQLFile(u) && timestamp.forall(_.resolved) =>
      // If we successfully look up the data source, then this is a path-based table, so we should
      // fail to time travel. Otherwise, this is some other catalog table that isn't resolved yet,
      // so we should leave it be for now.
      try {
        resolveDataSource(u)
        throw QueryCompilationErrors.timeTravelUnsupportedError(toSQLId(u.multipartIdentifier))
      } catch {
        case _: ClassNotFoundException => r
      }

    case u: UnresolvedRelation if maybeSQLFile(u) =>
      try {
        val ds = resolveDataSource(u)
        LogicalRelation(ds.resolveRelation())
      } catch {
        case _: ClassNotFoundException => u
        case e: SparkIllegalArgumentException if e.getErrorClass != null =>
          u.failAnalysis(
            errorClass = e.getErrorClass,
            messageParameters = e.getMessageParameters.asScala.toMap,
            cause = e)
        case e: Exception if !e.isInstanceOf[AnalysisException] =>
          // the provider is valid, but failed to create a logical plan
          u.failAnalysis(
            errorClass = "UNSUPPORTED_DATASOURCE_FOR_DIRECT_QUERY",
            messageParameters = Map("dataSourceType" -> u.multipartIdentifier.head),
            cause = e
          )
      }
  }
}

/**
 * Preprocess [[CreateTable]], to do some normalization and checking.
 */
case class PreprocessTableCreation(catalog: SessionCatalog) extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    // When we CREATE TABLE without specifying the table schema, we should fail the query if
    // bucketing information is specified, as we can't infer bucketing from data files currently.
    // Since the runtime inferred partition columns could be different from what user specified,
    // we fail the query if the partitioning information is specified.
    case c @ CreateTableV1(tableDesc, _, None) if tableDesc.schema.isEmpty =>
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
      c

    // When we append data to an existing table, check if the given provider, partition columns,
    // bucket spec, etc. match the existing table, and adjust the columns order of the given query
    // if necessary.
    case c @ CreateTableV1(tableDesc, SaveMode.Append, Some(query))
        if query.resolved && catalog.tableExists(tableDesc.identifier) =>
      // This is guaranteed by the parser and `DataFrameWriter`
      assert(tableDesc.provider.isDefined)

      val db = tableDesc.identifier.database.getOrElse(catalog.getCurrentDatabase)
      val tableIdentWithDB = tableDesc.identifier.copy(database = Some(db))
      val tableName = tableIdentWithDB.unquotedString
      val existingTable = catalog.getTableMetadata(tableIdentWithDB)

      if (existingTable.tableType == CatalogTableType.VIEW) {
        throw QueryCompilationErrors.saveDataIntoViewNotAllowedError()
      }

      // Check if the specified data source match the data source of the existing table.
      val existingProvider = DataSource.lookupDataSource(existingTable.provider.get, conf)
      val specifiedProvider = DataSource.lookupDataSource(tableDesc.provider.get, conf)
      // TODO: Check that options from the resolved relation match the relation that we are
      // inserting into (i.e. using the same compression).
      // If the one of the provider is [[FileDataSourceV2]] and the other one is its corresponding
      // [[FileFormat]], the two providers are considered compatible.
      if (fallBackV2ToV1(existingProvider) != fallBackV2ToV1(specifiedProvider)) {
        throw QueryCompilationErrors.mismatchedTableFormatError(
          tableName, existingProvider, specifiedProvider)
      }
      tableDesc.storage.locationUri match {
        case Some(location) if location.getPath != existingTable.location.getPath =>
          throw QueryCompilationErrors.mismatchedTableLocationError(
            tableIdentWithDB, existingTable, tableDesc)
        case _ =>
      }

      if (query.schema.length != existingTable.schema.length) {
        throw QueryCompilationErrors.mismatchedTableColumnNumberError(
          tableName, existingTable, query)
      }

      val resolver = conf.resolver
      val tableCols = existingTable.schema.map(_.name)

      // As we are inserting into an existing table, we should respect the existing schema and
      // adjust the column order of the given dataframe according to it, or throw exception
      // if the column names do not match.
      val adjustedColumns = tableCols.map { col =>
        query.resolve(Seq(col), resolver).getOrElse {
          val inputColumns = query.schema.map(_.name).mkString(", ")
          throw QueryCompilationErrors.cannotResolveColumnGivenInputColumnsError(col, inputColumns)
        }
      }

      // Check if the specified partition columns match the existing table.
      val specifiedPartCols = CatalogUtils.normalizePartCols(
        tableName, tableCols, tableDesc.partitionColumnNames, resolver)
      if (specifiedPartCols != existingTable.partitionColumnNames) {
        val existingPartCols = existingTable.partitionColumnNames.mkString(", ")
        throw QueryCompilationErrors.mismatchedTablePartitionColumnError(
          tableName, specifiedPartCols, existingPartCols)
      }

      // Check if the specified bucketing match the existing table.
      val specifiedBucketSpec = tableDesc.bucketSpec.map { bucketSpec =>
        CatalogUtils.normalizeBucketSpec(tableName, tableCols, bucketSpec, resolver)
      }
      if (specifiedBucketSpec != existingTable.bucketSpec) {
        val specifiedBucketString =
          specifiedBucketSpec.map(_.toString).getOrElse("not bucketed")
        val existingBucketString =
          existingTable.bucketSpec.map(_.toString).getOrElse("not bucketed")
        throw QueryCompilationErrors.mismatchedTableBucketingError(
          tableName, specifiedBucketString, existingBucketString)
      }

      // Check if the specified clustering columns match the existing table.
      val specifiedClusterBySpec = tableDesc.clusterBySpec
      val existingClusterBySpec = existingTable.clusterBySpec
      if (specifiedClusterBySpec != existingClusterBySpec) {
        val specifiedClusteringString =
          specifiedClusterBySpec.map(_.toString).getOrElse("")
        val existingClusteringString =
          existingClusterBySpec.map(_.toString).getOrElse("")
        throw QueryCompilationErrors.mismatchedTableClusteringError(
          tableName, specifiedClusteringString, existingClusteringString)
      }

      val newQuery = if (adjustedColumns != query.output) {
        Project(adjustedColumns, query)
      } else {
        query
      }

      c.copy(
        tableDesc = existingTable,
        query = Some(TableOutputResolver.resolveOutputColumns(
          tableDesc.qualifiedName, toAttributes(existingTable.schema), newQuery,
          byName = true, conf)))

    // Here we normalize partition, bucket and sort column names, w.r.t. the case sensitivity
    // config, and do various checks:
    //   * column names in table definition can't be duplicated.
    //   * partition, bucket and sort column names must exist in table definition.
    //   * partition, bucket and sort column names can't be duplicated.
    //   * can't use all table columns as partition columns.
    //   * partition columns' type must be AtomicType.
    //   * sort columns' type must be orderable.
    //   * reorder table schema or output of query plan, to put partition columns at the end.
    case c @ CreateTableV1(tableDesc, _, query) if query.forall(_.resolved) =>
      if (query.isDefined) {
        assert(tableDesc.schema.isEmpty,
          "Schema may not be specified in a Create Table As Select (CTAS) statement")

        val analyzedQuery = query.get
        val normalizedTable = normalizeCatalogTable(analyzedQuery.schema, tableDesc)

        DDLUtils.checkTableColumns(tableDesc.copy(schema = analyzedQuery.schema))

        val output = analyzedQuery.output

        val outputByName = HashMap(output.map(o => o.name -> o): _*)
        val partitionAttrs = normalizedTable.partitionColumnNames.map { partCol =>
          outputByName(partCol)
        }
        val partitionAttrsSet = HashSet(partitionAttrs: _*)
        val newOutput = output.filterNot(partitionAttrsSet.contains) ++ partitionAttrs

        val reorderedQuery = if (newOutput == output) {
          analyzedQuery
        } else {
          Project(newOutput, analyzedQuery)
        }

        c.copy(tableDesc = normalizedTable, query = Some(reorderedQuery))
      } else {
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

        c.copy(tableDesc = normalizedTable.copy(schema = reorderedSchema))
      }

    case create: V2CreateTablePlan if create.childrenResolved =>
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

      if (schema.isEmpty) {
        if (partitioning.nonEmpty) {
          throw QueryCompilationErrors.specifyPartitionNotAllowedWhenTableSchemaNotDefinedError()
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

        create.withPartitioning(normalizedPartitions)
      }
  }

  private def fallBackV2ToV1(cls: Class[_]): Class[_] =
    cls.getDeclaredConstructor().newInstance() match {
      case f: FileDataSourceV2 => f.fallbackFileFormat
      case _ => cls
    }

  private def normalizeCatalogTable(schema: StructType, table: CatalogTable): CatalogTable = {
    SchemaUtils.checkSchemaColumnNameDuplication(
      schema,
      conf.caseSensitiveAnalysis)

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
}

/**
 * Preprocess the [[InsertIntoStatement]] plan. Throws exception if the number of columns mismatch,
 * or specified partition columns are different from the existing partition columns in the target
 * table. It also does data type casting and field renaming, to make sure that the columns to be
 * inserted have the correct data type and fields have the correct names.
 */
object PreprocessTableInsertion extends ResolveInsertionBase {
  private def preprocess(
      insert: InsertIntoStatement,
      tblName: String,
      partColNames: StructType,
      catalogTable: Option[CatalogTable]): InsertIntoStatement = {

    val normalizedPartSpec = normalizePartitionSpec(
      insert.partitionSpec, partColNames, tblName, conf.resolver)

    val staticPartCols = normalizedPartSpec.filter(_._2.isDefined).keySet
    val expectedColumns = insert.table.output.filterNot(a => staticPartCols.contains(a.name))

    val partitionsTrackedByCatalog = catalogTable.isDefined &&
      catalogTable.get.partitionColumnNames.nonEmpty &&
      catalogTable.get.tracksPartitionsInCatalog
    if (partitionsTrackedByCatalog && normalizedPartSpec.nonEmpty) {
      // empty partition column value
      if (normalizedPartSpec.values.flatten.exists(v => v != null && v.isEmpty)) {
        val spec = normalizedPartSpec.map(p => p._1 + "=" + p._2).mkString("[", ", ", "]")
        throw QueryCompilationErrors.invalidPartitionSpecError(
          s"The spec ($spec) contains an empty partition column value")
      }
    }

    // Create a project if this INSERT has a user-specified column list.
    val hasColumnList = insert.userSpecifiedCols.nonEmpty
    val query = if (hasColumnList) {
      createProjectForByNameQuery(tblName, insert)
    } else {
      insert.query
    }
    val newQuery = try {
      val byName = hasColumnList || insert.byName
      TableOutputResolver.suitableForByNameCheck(byName, expected = expectedColumns,
        queryOutput = query.output)
      TableOutputResolver.resolveOutputColumns(
        tblName,
        expectedColumns,
        query,
        byName,
        conf,
        supportColDefaultValue = true)
    } catch {
      case e: AnalysisException if staticPartCols.nonEmpty &&
        (e.getErrorClass == "INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS" ||
          e.getErrorClass == "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS") =>
        val newException = e.copy(
          errorClass = Some("INSERT_PARTITION_COLUMN_ARITY_MISMATCH"),
          messageParameters = e.messageParameters ++ Map(
            "tableColumns" -> insert.table.output.map(c => toSQLId(c.name)).mkString(", "),
            "staticPartCols" -> staticPartCols.toSeq.sorted.map(c => toSQLId(c)).mkString(", ")
          ))
        newException.setStackTrace(e.getStackTrace)
        throw newException
    }
    if (normalizedPartSpec.nonEmpty) {
      if (normalizedPartSpec.size != partColNames.length) {
        throw QueryCompilationErrors.requestedPartitionsMismatchTablePartitionsError(
          tblName, normalizedPartSpec, partColNames)
      }

      insert.copy(query = newQuery, partitionSpec = normalizedPartSpec)
    } else {
      // All partition columns are dynamic because the InsertIntoTable command does
      // not explicitly specify partitioning columns.
      insert.copy(query = newQuery, partitionSpec = partColNames.map(_.name).map(_ -> None).toMap)
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case i @ InsertIntoStatement(table, _, _, query, _, _, _) if table.resolved && query.resolved =>
      table match {
        case relation: HiveTableRelation =>
          val metadata = relation.tableMeta
          preprocess(i, metadata.identifier.quotedString, metadata.partitionSchema,
            Some(metadata))
        case LogicalRelation(h: HadoopFsRelation, _, catalogTable, _) =>
          val tblName = catalogTable.map(_.identifier.quotedString).getOrElse("unknown")
          preprocess(i, tblName, h.partitionSchema, catalogTable)
        case LogicalRelation(_: InsertableRelation, _, catalogTable, _) =>
          val tblName = catalogTable.map(_.identifier.quotedString).getOrElse("unknown")
          preprocess(i, tblName, new StructType(), catalogTable)
        case _ => i
      }
  }
}

/**
 * A rule to check whether the functions are supported only when Hive support is enabled
 */
object HiveOnlyCheck extends (LogicalPlan => Unit) {
  def apply(plan: LogicalPlan): Unit = {
    plan.foreach {
      case CreateTableV1(tableDesc, _, _) if DDLUtils.isHiveTable(tableDesc) =>
        throw QueryCompilationErrors.ddlWithoutHiveSupportEnabledError(
          "CREATE Hive TABLE (AS SELECT)")
      case i: InsertIntoDir if DDLUtils.isHiveTable(i.provider) =>
        throw QueryCompilationErrors.ddlWithoutHiveSupportEnabledError(
          "INSERT OVERWRITE DIRECTORY with the Hive format")
      case _ => // OK
    }
  }
}


/**
 * A rule to do various checks before reading a table.
 */
object PreReadCheck extends (LogicalPlan => Unit) {
  def apply(plan: LogicalPlan): Unit = {
    plan.foreach {
      case operator: LogicalPlan =>
        operator transformExpressionsUp {
          case e @ (_: InputFileName | _: InputFileBlockLength | _: InputFileBlockStart) =>
            checkNumInputFileBlockSources(e, operator)
            e
        }
    }
  }

  private def checkNumInputFileBlockSources(e: Expression, operator: LogicalPlan): Int = {
    operator match {
      case _: HiveTableRelation => 1
      case _ @ LogicalRelation(_: HadoopFsRelation, _, _, _) => 1
      case _: LeafNode => 0
      // UNION ALL has multiple children, but these children do not concurrently use InputFileBlock.
      case u: Union =>
        if (u.children.map(checkNumInputFileBlockSources(e, _)).sum >= 1) 1 else 0
      case o =>
        val numInputFileBlockSources = o.children.map(checkNumInputFileBlockSources(e, _)).sum
        if (numInputFileBlockSources > 1) {
          e.failAnalysis(
            errorClass = "MULTI_SOURCES_UNSUPPORTED_FOR_EXPRESSION",
            messageParameters = Map("expr" -> toSQLExpr(e)))
        } else {
          numInputFileBlockSources
        }
    }
  }
}


/**
 * A rule to do various checks before inserting into or writing to a data source table.
 */
object PreWriteCheck extends (LogicalPlan => Unit) {

  def apply(plan: LogicalPlan): Unit = {
    plan.foreach {
      case InsertIntoStatement(l @ LogicalRelation(relation, _, _, _), partition,
          _, query, _, _, _) =>
        // Get all input data source relations of the query.
        val srcRelations = query.collect {
          case LogicalRelation(src, _, _, _) => src
        }
        if (srcRelations.contains(relation)) {
          throw new AnalysisException(
            errorClass = "UNSUPPORTED_INSERT.READ_FROM",
            messageParameters = Map("relationId" -> toSQLId(relation.toString)))
        } else {
          // OK
        }

        relation match {
          case _: HadoopFsRelation => // OK

          // Right now, we do not support insert into a non-file-based data source table with
          // partition specs.
          case i: InsertableRelation if partition.nonEmpty =>
            throw new AnalysisException(
              errorClass = "UNSUPPORTED_INSERT.NOT_PARTITIONED",
              messageParameters = Map("relationId" -> toSQLId(i.toString)))

          case _ =>
            throw new AnalysisException(
              errorClass = "UNSUPPORTED_INSERT.NOT_ALLOWED",
              messageParameters = Map("relationId" -> toSQLId(relation.toString)))
        }

      case InsertIntoStatement(t, _, _, _, _, _, _)
        if !t.isInstanceOf[LeafNode] ||
          t.isInstanceOf[Range] ||
          t.isInstanceOf[OneRowRelation] ||
          t.isInstanceOf[LocalRelation] =>
        throw new AnalysisException(
          errorClass = "UNSUPPORTED_INSERT.RDD_BASED",
          messageParameters = Map.empty)

      case _ => // OK
    }
  }
}

/**
 * A rule to qualify relative locations with warehouse path before it breaks in catalog
 * operation and data reading and writing.
 *
 * @param catalog the session catalog
 */
case class QualifyLocationWithWarehouse(catalog: SessionCatalog) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case c @ CreateTableV1(tableDesc, _, _) if !c.locationQualifiedOrAbsent =>
      val qualifiedTableIdent = catalog.qualifyIdentifier(tableDesc.identifier)
      val loc = tableDesc.storage.locationUri.get
      val db = qualifiedTableIdent.database.get
      val newLocation = catalog.makeQualifiedTablePath(loc, db)
      val newTable = tableDesc.copy(
        identifier = qualifiedTableIdent,
        storage = tableDesc.storage.copy(locationUri = Some(newLocation))
      )
      c.copy(tableDesc = newTable)
  }
}

object CollationCheck extends (LogicalPlan => Unit) {
  def apply(plan: LogicalPlan): Unit = {
    plan.foreach {
      case operator: LogicalPlan =>
        operator.expressions.foreach(_.foreach(
          e =>
            if (isCollationExpression(e) && !SQLConf.get.collationEnabled) {
              throw QueryCompilationErrors.collationNotEnabledError()
            }
          )
        )
    }
  }

  private def isCollationExpression(expression: Expression): Boolean =
    expression.isInstanceOf[Collation] || expression.isInstanceOf[Collate]
}


/**
 * This rule checks for references to views WITH SCHEMA [TYPE] EVOLUTION and synchronizes the
 * catalog if evolution was detected.
 * It does so by walking the resolved plan looking for View operators for persisted views.
 */
object ViewSyncSchemaToMetaStore extends (LogicalPlan => Unit) {
  def apply(plan: LogicalPlan): Unit = {
    plan.foreach {
      case View(metaData, false, viewQuery)
        if (metaData.viewSchemaMode == SchemaTypeEvolution ||
          metaData.viewSchemaMode == SchemaEvolution) =>
        val viewSchemaMode = metaData.viewSchemaMode
        val viewFields = metaData.schema.fields
        val viewQueryFields = viewQuery.schema.fields
        val session = SparkSession.getActiveSession.get
        val redoSignature =
          viewSchemaMode == SchemaEvolution && viewFields.length != viewQueryFields.length
        val fieldNames = viewQuery.schema.fieldNames

        val redo = redoSignature || viewFields.zipWithIndex.exists { case (field, index) =>
          val planField = viewQueryFields(index)
          (field.dataType != planField.dataType ||
            field.nullable != planField.nullable ||
            (viewSchemaMode == SchemaEvolution && (
              field.getComment() != planField.getComment() ||
              field.name != planField.name)))
        }

        if (redo) {
          val newProperties = if (viewSchemaMode == SchemaEvolution) {
            generateViewProperties(
              metaData.properties,
              session,
              fieldNames,
              fieldNames,
              metaData.viewSchemaMode)
          } else {
            metaData.properties
          }
          val newSchema = if (viewSchemaMode == SchemaTypeEvolution) {
            val newFields = viewQuery.schema.map {
              case StructField(name, dataType, nullable, _) =>
                StructField(name, dataType, nullable,
                  viewFields.find(_.name == name).get.metadata)
            }
            StructType(newFields)
          } else {
            viewQuery.schema
          }
          SchemaUtils.checkColumnNameDuplication(fieldNames.toImmutableArraySeq,
            session.sessionState.conf.resolver)
          val updatedViewMeta = metaData.copy(
            properties = newProperties,
            schema = newSchema)
          session.sessionState.catalog.alterTable(updatedViewMeta)
        }
      case _ => // OK
    }
  }
}
