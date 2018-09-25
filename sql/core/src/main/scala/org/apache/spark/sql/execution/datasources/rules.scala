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
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast, Expression, InputFileBlockLength, InputFileBlockStart, InputFileName, RowOrdering}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.types.{AtomicType, StructType}
import org.apache.spark.sql.util.SchemaUtils

/**
 * Replaces [[UnresolvedRelation]]s if the plan is for direct query on files.
 */
class ResolveSQLOnFile(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  private def maybeSQLFile(u: UnresolvedRelation): Boolean = {
    sparkSession.sessionState.conf.runSQLonFile && u.tableIdentifier.database.isDefined
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case u: UnresolvedRelation if maybeSQLFile(u) =>
      try {
        val dataSource = DataSource(
          sparkSession,
          paths = u.tableIdentifier.table :: Nil,
          className = u.tableIdentifier.database.get)

        // `dataSource.providingClass` may throw ClassNotFoundException, then the outer try-catch
        // will catch it and return the original plan, so that the analyzer can report table not
        // found later.
        val isFileFormat = classOf[FileFormat].isAssignableFrom(dataSource.providingClass)
        if (!isFileFormat ||
            dataSource.className.toLowerCase(Locale.ROOT) == DDLUtils.HIVE_PROVIDER) {
          throw new AnalysisException("Unsupported data source type for direct query on files: " +
            s"${u.tableIdentifier.database.get}")
        }
        LogicalRelation(dataSource.resolveRelation())
      } catch {
        case _: ClassNotFoundException => u
        case e: Exception =>
          // the provider is valid, but failed to create a logical plan
          u.failAnalysis(e.getMessage, e)
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
    case c @ CreateTable(tableDesc, _, None) if tableDesc.schema.isEmpty =>
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
    case c @ CreateTable(tableDesc, SaveMode.Append, Some(query))
        if query.resolved && catalog.tableExists(tableDesc.identifier) =>
      // This is guaranteed by the parser and `DataFrameWriter`
      assert(tableDesc.provider.isDefined)

      val db = tableDesc.identifier.database.getOrElse(catalog.getCurrentDatabase)
      val tableIdentWithDB = tableDesc.identifier.copy(database = Some(db))
      val tableName = tableIdentWithDB.unquotedString
      val existingTable = catalog.getTableMetadata(tableIdentWithDB)

      if (existingTable.tableType == CatalogTableType.VIEW) {
        throw new AnalysisException("Saving data into a view is not allowed.")
      }

      // Check if the specified data source match the data source of the existing table.
      val conf = sparkSession.sessionState.conf
      val existingProvider = DataSource.lookupDataSource(existingTable.provider.get, conf)
      val specifiedProvider = DataSource.lookupDataSource(tableDesc.provider.get, conf)
      // TODO: Check that options from the resolved relation match the relation that we are
      // inserting into (i.e. using the same compression).
      if (existingProvider != specifiedProvider) {
        throw new AnalysisException(s"The format of the existing table $tableName is " +
          s"`${existingProvider.getSimpleName}`. It doesn't match the specified format " +
          s"`${specifiedProvider.getSimpleName}`.")
      }
      tableDesc.storage.locationUri match {
        case Some(location) if location.getPath != existingTable.location.getPath =>
          throw new AnalysisException(
            s"The location of the existing table ${tableIdentWithDB.quotedString} is " +
              s"`${existingTable.location}`. It doesn't match the specified location " +
              s"`${tableDesc.location}`.")
        case _ =>
      }

      if (query.schema.length != existingTable.schema.length) {
        throw new AnalysisException(
          s"The column number of the existing table $tableName" +
            s"(${existingTable.schema.catalogString}) doesn't match the data schema" +
            s"(${query.schema.catalogString})")
      }

      val resolver = sparkSession.sessionState.conf.resolver
      val tableCols = existingTable.schema.map(_.name)

      // As we are inserting into an existing table, we should respect the existing schema and
      // adjust the column order of the given dataframe according to it, or throw exception
      // if the column names do not match.
      val adjustedColumns = tableCols.map { col =>
        query.resolve(Seq(col), resolver).getOrElse {
          val inputColumns = query.schema.map(_.name).mkString(", ")
          throw new AnalysisException(
            s"cannot resolve '$col' given input columns: [$inputColumns]")
        }
      }

      // Check if the specified partition columns match the existing table.
      val specifiedPartCols = CatalogUtils.normalizePartCols(
        tableName, tableCols, tableDesc.partitionColumnNames, resolver)
      if (specifiedPartCols != existingTable.partitionColumnNames) {
        val existingPartCols = existingTable.partitionColumnNames.mkString(", ")
        throw new AnalysisException(
          s"""
             |Specified partitioning does not match that of the existing table $tableName.
             |Specified partition columns: [${specifiedPartCols.mkString(", ")}]
             |Existing partition columns: [$existingPartCols]
          """.stripMargin)
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
        throw new AnalysisException(
          s"""
             |Specified bucketing does not match that of the existing table $tableName.
             |Specified bucketing: $specifiedBucketString
             |Existing bucketing: $existingBucketString
          """.stripMargin)
      }

      val newQuery = if (adjustedColumns != query.output) {
        Project(adjustedColumns, query)
      } else {
        query
      }

      c.copy(
        tableDesc = existingTable,
        query = Some(DDLPreprocessingUtils.castAndRenameQueryOutput(
          newQuery, existingTable.schema.toAttributes, conf)))

    // Here we normalize partition, bucket and sort column names, w.r.t. the case sensitivity
    // config, and do various checks:
    //   * column names in table definition can't be duplicated.
    //   * partition, bucket and sort column names must exist in table definition.
    //   * partition, bucket and sort column names can't be duplicated.
    //   * can't use all table columns as partition columns.
    //   * partition columns' type must be AtomicType.
    //   * sort columns' type must be orderable.
    //   * reorder table schema or output of query plan, to put partition columns at the end.
    case c @ CreateTable(tableDesc, _, query) if query.forall(_.resolved) =>
      if (query.isDefined) {
        assert(tableDesc.schema.isEmpty,
          "Schema may not be specified in a Create Table As Select (CTAS) statement")

        val analyzedQuery = query.get
        val normalizedTable = normalizeCatalogTable(analyzedQuery.schema, tableDesc)

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
        val normalizedTable = normalizeCatalogTable(tableDesc.schema, tableDesc)

        val partitionSchema = normalizedTable.partitionColumnNames.map { partCol =>
          normalizedTable.schema.find(_.name == partCol).get
        }

        val reorderedSchema =
          StructType(normalizedTable.schema.filterNot(partitionSchema.contains) ++ partitionSchema)

        c.copy(tableDesc = normalizedTable.copy(schema = reorderedSchema))
      }
  }

  private def normalizeCatalogTable(schema: StructType, table: CatalogTable): CatalogTable = {
    SchemaUtils.checkSchemaColumnNameDuplication(
      schema,
      "in the table definition of " + table.identifier,
      sparkSession.sessionState.conf.caseSensitiveAnalysis)

    val normalizedPartCols = normalizePartitionColumns(schema, table)
    val normalizedBucketSpec = normalizeBucketSpec(schema, table)

    normalizedBucketSpec.foreach { spec =>
      for (bucketCol <- spec.bucketColumnNames if normalizedPartCols.contains(bucketCol)) {
        throw new AnalysisException(s"bucketing column '$bucketCol' should not be part of " +
          s"partition columns '${normalizedPartCols.mkString(", ")}'")
      }
      for (sortCol <- spec.sortColumnNames if normalizedPartCols.contains(sortCol)) {
        throw new AnalysisException(s"bucket sorting column '$sortCol' should not be part of " +
          s"partition columns '${normalizedPartCols.mkString(", ")}'")
      }
    }

    table.copy(partitionColumnNames = normalizedPartCols, bucketSpec = normalizedBucketSpec)
  }

  private def normalizePartitionColumns(schema: StructType, table: CatalogTable): Seq[String] = {
    val normalizedPartitionCols = CatalogUtils.normalizePartCols(
      tableName = table.identifier.unquotedString,
      tableCols = schema.map(_.name),
      partCols = table.partitionColumnNames,
      resolver = sparkSession.sessionState.conf.resolver)

    SchemaUtils.checkColumnNameDuplication(
      normalizedPartitionCols,
      "in the partition schema",
      sparkSession.sessionState.conf.resolver)

    if (schema.nonEmpty && normalizedPartitionCols.length == schema.length) {
      if (DDLUtils.isHiveTable(table)) {
        // When we hit this branch, it means users didn't specify schema for the table to be
        // created, as we always include partition columns in table schema for hive serde tables.
        // The real schema will be inferred at hive metastore by hive serde, plus the given
        // partition columns, so we should not fail the analysis here.
      } else {
        failAnalysis("Cannot use all columns for partition columns")
      }

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
          resolver = sparkSession.sessionState.conf.resolver)

        SchemaUtils.checkColumnNameDuplication(
          normalizedBucketSpec.bucketColumnNames,
          "in the bucket definition",
          sparkSession.sessionState.conf.resolver)
        SchemaUtils.checkColumnNameDuplication(
          normalizedBucketSpec.sortColumnNames,
          "in the sort definition",
          sparkSession.sessionState.conf.resolver)

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
 * Preprocess the [[InsertIntoTable]] plan. Throws exception if the number of columns mismatch, or
 * specified partition columns are different from the existing partition columns in the target
 * table. It also does data type casting and field renaming, to make sure that the columns to be
 * inserted have the correct data type and fields have the correct names.
 */
case class PreprocessTableInsertion(conf: SQLConf) extends Rule[LogicalPlan] {
  private def preprocess(
      insert: InsertIntoTable,
      tblName: String,
      partColNames: Seq[String]): InsertIntoTable = {

    val normalizedPartSpec = PartitioningUtils.normalizePartitionSpec(
      insert.partition, partColNames, tblName, conf.resolver)

    val staticPartCols = normalizedPartSpec.filter(_._2.isDefined).keySet
    val expectedColumns = insert.table.output.filterNot(a => staticPartCols.contains(a.name))

    if (expectedColumns.length != insert.query.schema.length) {
      throw new AnalysisException(
        s"$tblName requires that the data to be inserted have the same number of columns as the " +
          s"target table: target table has ${insert.table.output.size} column(s) but the " +
          s"inserted data has ${insert.query.output.length + staticPartCols.size} column(s), " +
          s"including ${staticPartCols.size} partition column(s) having constant value(s).")
    }

    val newQuery = DDLPreprocessingUtils.castAndRenameQueryOutput(
      insert.query, expectedColumns, conf)
    if (normalizedPartSpec.nonEmpty) {
      if (normalizedPartSpec.size != partColNames.length) {
        throw new AnalysisException(
          s"""
             |Requested partitioning does not match the table $tblName:
             |Requested partitions: ${normalizedPartSpec.keys.mkString(",")}
             |Table partitions: ${partColNames.mkString(",")}
           """.stripMargin)
      }

      insert.copy(query = newQuery, partition = normalizedPartSpec)
    } else {
      // All partition columns are dynamic because the InsertIntoTable command does
      // not explicitly specify partitioning columns.
      insert.copy(query = newQuery, partition = partColNames.map(_ -> None).toMap)
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case i @ InsertIntoTable(table, _, query, _, _) if table.resolved && query.resolved =>
      table match {
        case relation: HiveTableRelation =>
          val metadata = relation.tableMeta
          preprocess(i, metadata.identifier.quotedString, metadata.partitionColumnNames)
        case LogicalRelation(h: HadoopFsRelation, _, catalogTable, _) =>
          val tblName = catalogTable.map(_.identifier.quotedString).getOrElse("unknown")
          preprocess(i, tblName, h.partitionSchema.map(_.name))
        case LogicalRelation(_: InsertableRelation, _, catalogTable, _) =>
          val tblName = catalogTable.map(_.identifier.quotedString).getOrElse("unknown")
          preprocess(i, tblName, Nil)
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
      case CreateTable(tableDesc, _, _) if DDLUtils.isHiveTable(tableDesc) =>
        throw new AnalysisException("Hive support is required to CREATE Hive TABLE (AS SELECT)")
      case i: InsertIntoDir if DDLUtils.isHiveTable(i.provider) =>
        throw new AnalysisException(
          "Hive support is required to INSERT OVERWRITE DIRECTORY with the Hive format")
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
          e.failAnalysis(s"'${e.prettyName}' does not support more than one sources")
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
      case InsertIntoTable(l @ LogicalRelation(relation, _, _, _), partition, query, _, _) =>
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

      case InsertIntoTable(t, _, _, _, _)
        if !t.isInstanceOf[LeafNode] ||
          t.isInstanceOf[Range] ||
          t.isInstanceOf[OneRowRelation] ||
          t.isInstanceOf[LocalRelation] =>
        failAnalysis(s"Inserting into an RDD-based table is not allowed.")

      case _ => // OK
    }
  }
}

object DDLPreprocessingUtils {

  /**
   * Adjusts the name and data type of the input query output columns, to match the expectation.
   */
  def castAndRenameQueryOutput(
      query: LogicalPlan,
      expectedOutput: Seq[Attribute],
      conf: SQLConf): LogicalPlan = {
    val newChildOutput = expectedOutput.zip(query.output).map {
      case (expected, actual) =>
        if (expected.dataType.sameType(actual.dataType) &&
          expected.name == actual.name &&
          expected.metadata == actual.metadata) {
          actual
        } else {
          // Renaming is needed for handling the following cases like
          // 1) Column names/types do not match, e.g., INSERT INTO TABLE tab1 SELECT 1, 2
          // 2) Target tables have column metadata
          Alias(
            Cast(actual, expected.dataType, Option(conf.sessionLocalTimeZone)),
            expected.name)(explicitMetadata = Option(expected.metadata))
        }
    }

    if (newChildOutput == query.output) {
      query
    } else {
      Project(newChildOutput, query)
    }
  }
}
