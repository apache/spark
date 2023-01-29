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

import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{Expression, InputFileBlockLength, InputFileBlockStart, InputFileName, RowOrdering}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.expressions.{FieldReference, RewritableTransform}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.{CreateTable => CreateTableV1}
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.types.{AtomicType, StructType}
import org.apache.spark.sql.util.PartitioningUtils.normalizePartitionSpec
import org.apache.spark.sql.util.SchemaUtils

/**
 * Replaces [[UnresolvedRelation]]s if the plan is for direct query on files.
 */
class ResolveSQLOnFile(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  private def maybeSQLFile(u: UnresolvedRelation): Boolean = {
    conf.runSQLonFile && u.multipartIdentifier.size == 2
  }

  private def resolveDataSource(ident: Seq[String]): DataSource = {
    val dataSource = DataSource(sparkSession, paths = Seq(ident.last), className = ident.head)
    // `dataSource.providingClass` may throw ClassNotFoundException, the caller side will try-catch
    // it and return the original plan, so that the analyzer can report table not found later.
    val isFileFormat = classOf[FileFormat].isAssignableFrom(dataSource.providingClass)
    if (!isFileFormat ||
      dataSource.className.toLowerCase(Locale.ROOT) == DDLUtils.HIVE_PROVIDER) {
      throw QueryCompilationErrors.unsupportedDataSourceTypeForDirectQueryOnFilesError(
        dataSource.className)
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
        resolveDataSource(u.multipartIdentifier)
        throw QueryCompilationErrors.timeTravelUnsupportedError("path-based tables")
      } catch {
        case _: ClassNotFoundException => r
      }

    case u: UnresolvedRelation if maybeSQLFile(u) =>
      try {
        val ds = resolveDataSource(u.multipartIdentifier)
        LogicalRelation(ds.resolveRelation())
      } catch {
        case _: ClassNotFoundException => u
        case e: Exception =>
          // the provider is valid, but failed to create a logical plan
          u.failAnalysis(
            errorClass = "_LEGACY_ERROR_TEMP_2332",
            messageParameters = Map("msg" -> e.getMessage),
            cause = e)
      }
  }
}

/**
 * Preprocess [[CreateTable]], to do some normalization and checking.
 */
case class PreprocessTableCreation(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  // catalog is a def and not a val/lazy val as the latter would introduce a circular reference
  private def catalog = sparkSession.sessionState.catalog

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    // When we CREATE TABLE without specifying the table schema, we should fail the query if
    // bucketing information is specified, as we can't infer bucketing from data files currently.
    // Since the runtime inferred partition columns could be different from what user specified,
    // we fail the query if the partitioning information is specified.
    case c @ CreateTableV1(tableDesc, _, None) if tableDesc.schema.isEmpty =>
      if (tableDesc.bucketSpec.isDefined) {
        failAnalysis("Cannot specify bucketing information if the table schema is not specified " +
          "when creating and will be inferred at runtime")
      }
      if (tableDesc.partitionColumnNames.nonEmpty) {
        failAnalysis("It is not allowed to specify partition columns when the table schema is " +
          "not defined. When the table schema is not provided, schema and partition columns " +
          "will be inferred.")
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

      val newQuery = if (adjustedColumns != query.output) {
        Project(adjustedColumns, query)
      } else {
        query
      }

      c.copy(
        tableDesc = existingTable,
        query = Some(TableOutputResolver.resolveOutputColumns(
          tableDesc.qualifiedName, existingTable.schema.toAttributes, newQuery,
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
        val partitionAttrs = normalizedTable.partitionColumnNames.map { partCol =>
          output.find(_.name == partCol).get
        }
        val newOutput = output.filterNot(partitionAttrs.contains) ++ partitionAttrs
        val reorderedQuery = if (newOutput == output) {
          analyzedQuery
        } else {
          Project(newOutput, analyzedQuery)
        }

        c.copy(tableDesc = normalizedTable, query = Some(reorderedQuery))
      } else {
        DDLUtils.checkTableColumns(tableDesc)
        val normalizedTable = normalizeCatalogTable(tableDesc.schema, tableDesc)

        val partitionSchema = normalizedTable.partitionColumnNames.map { partCol =>
          normalizedTable.schema.find(_.name == partCol).get
        }

        val reorderedSchema =
          StructType(normalizedTable.schema.filterNot(partitionSchema.contains) ++ partitionSchema)

        c.copy(tableDesc = normalizedTable.copy(schema = reorderedSchema))
      }

    case create: V2CreateTablePlan if create.childrenResolved =>
      val schema = create.tableSchema
      val partitioning = create.partitioning
      val identifier = create.tableName
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
              val position = SchemaUtils.findColumnPosition(ref.fieldNames(), schema, resolver)
              FieldReference(SchemaUtils.getColumnName(position, schema))
            }
            transform.withReferences(rewritten)
          case other => other
        }

        create.withPartitioning(normalizedPartitions)
      }
  }

  private def fallBackV2ToV1(cls: Class[_]): Class[_] = cls.newInstance match {
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

    table.copy(partitionColumnNames = normalizedPartCols, bucketSpec = normalizedBucketSpec)
  }

  private def normalizePartitionColumns(schema: StructType, table: CatalogTable): Seq[String] = {
    val normalizedPartitionCols = CatalogUtils.normalizePartCols(
      tableName = table.identifier.unquotedString,
      tableCols = schema.map(_.name),
      partCols = table.partitionColumnNames,
      resolver = conf.resolver)

    SchemaUtils.checkColumnNameDuplication(normalizedPartitionCols, conf.resolver)

    if (schema.nonEmpty && normalizedPartitionCols.length == schema.length) {
      failAnalysis("Cannot use all columns for partition columns")
    }

    schema.filter(f => normalizedPartitionCols.contains(f.name)).map(_.dataType).foreach {
      case _: AtomicType => // OK
      case other => failAnalysis(s"Cannot use ${other.catalogString} for partition column")
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

        Some(normalizedBucketSpec)

      case None => None
    }
  }

  private def failAnalysis(msg: String) = throw new AnalysisException(msg)
}

/**
 * Preprocess the [[InsertIntoStatement]] plan. Throws exception if the number of columns mismatch,
 * or specified partition columns are different from the existing partition columns in the target
 * table. It also does data type casting and field renaming, to make sure that the columns to be
 * inserted have the correct data type and fields have the correct names.
 */
object PreprocessTableInsertion extends Rule[LogicalPlan] {
  private def preprocess(
      insert: InsertIntoStatement,
      tblName: String,
      partColNames: StructType,
      catalogTable: Option[CatalogTable]): InsertIntoStatement = {

    val normalizedPartSpec = normalizePartitionSpec(
      insert.partitionSpec, partColNames, tblName, conf.resolver)

    val staticPartCols = normalizedPartSpec.filter(_._2.isDefined).keySet
    val expectedColumns = insert.table.output.filterNot(a => staticPartCols.contains(a.name))

    if (expectedColumns.length != insert.query.schema.length) {
      throw QueryCompilationErrors.mismatchedInsertedDataColumnNumberError(
        tblName, insert, staticPartCols)
    }

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

    val newQuery = TableOutputResolver.resolveOutputColumns(
      tblName, expectedColumns, insert.query, byName = false, conf)
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
    case i @ InsertIntoStatement(table, _, _, query, _, _) if table.resolved && query.resolved =>
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
            errorClass = "_LEGACY_ERROR_TEMP_2302",
            messageParameters = Map("name" -> e.prettyName))
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

  def failAnalysis(msg: String): Unit = { throw new AnalysisException(msg) }

  def apply(plan: LogicalPlan): Unit = {
    plan.foreach {
      case InsertIntoStatement(l @ LogicalRelation(relation, _, _, _), partition, _, query, _, _) =>
        // Get all input data source relations of the query.
        val srcRelations = query.collect {
          case LogicalRelation(src, _, _, _) => src
        }
        if (srcRelations.contains(relation)) {
          failAnalysis("Cannot insert into table that is also being read from.")
        } else {
          // OK
        }

        relation match {
          case _: HadoopFsRelation => // OK

          // Right now, we do not support insert into a non-file-based data source table with
          // partition specs.
          case _: InsertableRelation if partition.nonEmpty =>
            failAnalysis(s"Insert into a partition is not allowed because $l is not partitioned.")

          case _ => failAnalysis(s"$relation does not allow insertion.")
        }

      case InsertIntoStatement(t, _, _, _, _, _)
        if !t.isInstanceOf[LeafNode] ||
          t.isInstanceOf[Range] ||
          t.isInstanceOf[OneRowRelation] ||
          t.isInstanceOf[LocalRelation] =>
        failAnalysis(s"Inserting into an RDD-based table is not allowed.")

      case _ => // OK
    }
  }
}
