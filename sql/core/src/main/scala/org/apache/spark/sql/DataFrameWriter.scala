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

package org.apache.spark.sql

import java.util.{Locale, Properties}

import scala.collection.JavaConverters._

import org.apache.spark.annotation.Stable
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, NoSuchTableException, UnresolvedIdentifier, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, CreateTableAsSelect, InsertIntoStatement, LogicalPlan, OptionList, OverwriteByExpression, OverwritePartitionsDynamic, ReplaceTableAsSelect, UnresolvedTableSpec}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, CatalogV2Implicits, CatalogV2Util, Identifier, SupportsCatalogOptions, Table, TableCatalog, TableProvider, V1Table}
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.catalog.TableWritePrivilege
import org.apache.spark.sql.connector.catalog.TableWritePrivilege._
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.{CreateTable, DataSource, DataSourceUtils, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.PartitionOverwriteMode
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Interface used to write a [[Dataset]] to external storage systems (e.g. file systems,
 * key-value stores, etc). Use `Dataset.write` to access this.
 *
 * @since 1.4.0
 */
@Stable
final class DataFrameWriter[T] private[sql](ds: Dataset[T]) {

  private val df = ds.toDF()

  /**
   * Specifies the behavior when data or table already exists. Options include:
   * <ul>
   * <li>`SaveMode.Overwrite`: overwrite the existing data.</li>
   * <li>`SaveMode.Append`: append the data.</li>
   * <li>`SaveMode.Ignore`: ignore the operation (i.e. no-op).</li>
   * <li>`SaveMode.ErrorIfExists`: throw an exception at runtime.</li>
   * </ul>
   * <p>
   * The default option is `ErrorIfExists`.
   *
   * @since 1.4.0
   */
  def mode(saveMode: SaveMode): DataFrameWriter[T] = {
    this.mode = saveMode
    this
  }

  /**
   * Specifies the behavior when data or table already exists. Options include:
   * <ul>
   * <li>`overwrite`: overwrite the existing data.</li>
   * <li>`append`: append the data.</li>
   * <li>`ignore`: ignore the operation (i.e. no-op).</li>
   * <li>`error` or `errorifexists`: default option, throw an exception at runtime.</li>
   * </ul>
   *
   * @since 1.4.0
   */
  def mode(saveMode: String): DataFrameWriter[T] = {
    saveMode.toLowerCase(Locale.ROOT) match {
      case "overwrite" => mode(SaveMode.Overwrite)
      case "append" => mode(SaveMode.Append)
      case "ignore" => mode(SaveMode.Ignore)
      case "error" | "errorifexists" | "default" => mode(SaveMode.ErrorIfExists)
      case _ => throw new IllegalArgumentException(s"Unknown save mode: $saveMode. Accepted " +
        "save modes are 'overwrite', 'append', 'ignore', 'error', 'errorifexists', 'default'.")
    }
  }

  /**
   * Specifies the underlying output data source. Built-in options include "parquet", "json", etc.
   *
   * @since 1.4.0
   */
  def format(source: String): DataFrameWriter[T] = {
    this.source = source
    this
  }

  /**
   * Adds an output option for the underlying data source.
   *
   * All options are maintained in a case-insensitive way in terms of key names.
   * If a new option has the same key case-insensitively, it will override the existing option.
   *
   * @since 1.4.0
   */
  def option(key: String, value: String): DataFrameWriter[T] = {
    this.extraOptions = this.extraOptions + (key -> value)
    this
  }

  /**
   * Adds an output option for the underlying data source.
   *
   * All options are maintained in a case-insensitive way in terms of key names.
   * If a new option has the same key case-insensitively, it will override the existing option.
   *
   * @since 2.0.0
   */
  def option(key: String, value: Boolean): DataFrameWriter[T] = option(key, value.toString)

  /**
   * Adds an output option for the underlying data source.
   *
   * All options are maintained in a case-insensitive way in terms of key names.
   * If a new option has the same key case-insensitively, it will override the existing option.
   *
   * @since 2.0.0
   */
  def option(key: String, value: Long): DataFrameWriter[T] = option(key, value.toString)

  /**
   * Adds an output option for the underlying data source.
   *
   * All options are maintained in a case-insensitive way in terms of key names.
   * If a new option has the same key case-insensitively, it will override the existing option.
   *
   * @since 2.0.0
   */
  def option(key: String, value: Double): DataFrameWriter[T] = option(key, value.toString)

  /**
   * (Scala-specific) Adds output options for the underlying data source.
   *
   * All options are maintained in a case-insensitive way in terms of key names.
   * If a new option has the same key case-insensitively, it will override the existing option.
   *
   * @since 1.4.0
   */
  def options(options: scala.collection.Map[String, String]): DataFrameWriter[T] = {
    this.extraOptions ++= options
    this
  }

  /**
   * Adds output options for the underlying data source.
   *
   * All options are maintained in a case-insensitive way in terms of key names.
   * If a new option has the same key case-insensitively, it will override the existing option.
   *
   * @since 1.4.0
   */
  def options(options: java.util.Map[String, String]): DataFrameWriter[T] = {
    this.options(options.asScala)
    this
  }

  /**
   * Partitions the output by the given columns on the file system. If specified, the output is
   * laid out on the file system similar to Hive's partitioning scheme. As an example, when we
   * partition a dataset by year and then month, the directory layout would look like:
   * <ul>
   * <li>year=2016/month=01/</li>
   * <li>year=2016/month=02/</li>
   * </ul>
   *
   * Partitioning is one of the most widely used techniques to optimize physical data layout.
   * It provides a coarse-grained index for skipping unnecessary data reads when queries have
   * predicates on the partitioned columns. In order for partitioning to work well, the number
   * of distinct values in each column should typically be less than tens of thousands.
   *
   * This is applicable for all file-based data sources (e.g. Parquet, JSON) starting with Spark
   * 2.1.0.
   *
   * @since 1.4.0
   */
  @scala.annotation.varargs
  def partitionBy(colNames: String*): DataFrameWriter[T] = {
    this.partitioningColumns = Option(colNames)
    this
  }

  /**
   * Buckets the output by the given columns. If specified, the output is laid out on the file
   * system similar to Hive's bucketing scheme, but with a different bucket hash function
   * and is not compatible with Hive's bucketing.
   *
   * This is applicable for all file-based data sources (e.g. Parquet, JSON) starting with Spark
   * 2.1.0.
   *
   * @since 2.0
   */
  @scala.annotation.varargs
  def bucketBy(numBuckets: Int, colName: String, colNames: String*): DataFrameWriter[T] = {
    this.numBuckets = Option(numBuckets)
    this.bucketColumnNames = Option(colName +: colNames)
    this
  }

  /**
   * Sorts the output in each bucket by the given columns.
   *
   * This is applicable for all file-based data sources (e.g. Parquet, JSON) starting with Spark
   * 2.1.0.
   *
   * @since 2.0
   */
  @scala.annotation.varargs
  def sortBy(colName: String, colNames: String*): DataFrameWriter[T] = {
    this.sortColumnNames = Option(colName +: colNames)
    this
  }

  /**
   * Saves the content of the `DataFrame` at the specified path.
   *
   * @since 1.4.0
   */
  def save(path: String): Unit = {
    if (!df.sparkSession.sessionState.conf.legacyPathOptionBehavior &&
        extraOptions.contains("path")) {
      throw QueryCompilationErrors.pathOptionNotSetCorrectlyWhenWritingError()
    }
    saveInternal(Some(path))
  }

  /**
   * Saves the content of the `DataFrame` as the specified table.
   *
   * @since 1.4.0
   */
  def save(): Unit = saveInternal(None)

  private def saveInternal(path: Option[String]): Unit = {
    if (source.toLowerCase(Locale.ROOT) == DDLUtils.HIVE_PROVIDER) {
      throw QueryCompilationErrors.cannotOperateOnHiveDataSourceFilesError("write")
    }

    assertNotBucketed("save")

    val maybeV2Provider = lookupV2Provider()
    if (maybeV2Provider.isDefined) {
      val provider = maybeV2Provider.get
      val sessionOptions = DataSourceV2Utils.extractSessionConfigs(
        provider, df.sparkSession.sessionState.conf)

      val optionsWithPath = getOptionsWithPath(path)

      val finalOptions = sessionOptions.filterKeys(!optionsWithPath.contains(_)).toMap ++
        optionsWithPath.originalMap
      val dsOptions = new CaseInsensitiveStringMap(finalOptions.asJava)

      def getTable: Table = {
        // If the source accepts external table metadata, here we pass the schema of input query
        // and the user-specified partitioning to `getTable`. This is for avoiding
        // schema/partitioning inference, which can be very expensive.
        // If the query schema is not compatible with the existing data, the behavior is undefined.
        // For example, writing file source will success but the following reads will fail.
        if (provider.supportsExternalMetadata()) {
          provider.getTable(
            df.schema.asNullable,
            partitioningAsV2.toArray,
            dsOptions.asCaseSensitiveMap())
        } else {
          DataSourceV2Utils.getTableFromProvider(provider, dsOptions, userSpecifiedSchema = None)
        }
      }

      import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._
      val catalogManager = df.sparkSession.sessionState.catalogManager
      mode match {
        case SaveMode.Append | SaveMode.Overwrite =>
          val (table, catalog, ident) = provider match {
            case supportsExtract: SupportsCatalogOptions =>
              val ident = supportsExtract.extractIdentifier(dsOptions)
              val catalog = CatalogV2Util.getTableProviderCatalog(
                supportsExtract, catalogManager, dsOptions)

              (catalog.loadTable(ident), Some(catalog), Some(ident))
            case _: TableProvider =>
              val t = getTable
              if (t.supports(BATCH_WRITE)) {
                (t, None, None)
              } else {
                // Streaming also uses the data source V2 API. So it may be that the data source
                // implements v2, but has no v2 implementation for batch writes. In that case, we
                // fall back to saving as though it's a V1 source.
                return saveToV1Source(path)
              }
          }

          val relation = DataSourceV2Relation.create(table, catalog, ident, dsOptions)
          checkPartitioningMatchesV2Table(table)
          if (mode == SaveMode.Append) {
            runCommand(df.sparkSession) {
              AppendData.byName(relation, df.logicalPlan, finalOptions)
            }
          } else {
            // Truncate the table. TableCapabilityCheck will throw a nice exception if this
            // isn't supported
            runCommand(df.sparkSession) {
              OverwriteByExpression.byName(
                relation, df.logicalPlan, Literal(true), finalOptions)
            }
          }

        case createMode =>
          provider match {
            case supportsExtract: SupportsCatalogOptions =>
              val ident = supportsExtract.extractIdentifier(dsOptions)
              val catalog = CatalogV2Util.getTableProviderCatalog(
                supportsExtract, catalogManager, dsOptions)

              val tableSpec = UnresolvedTableSpec(
                properties = Map.empty,
                provider = Some(source),
                optionExpression = OptionList(Seq.empty),
                location = extraOptions.get("path"),
                comment = extraOptions.get(TableCatalog.PROP_COMMENT),
                serde = None,
                external = false)
              runCommand(df.sparkSession) {
                CreateTableAsSelect(
                  UnresolvedIdentifier(catalog.name +: ident.namespace.toSeq :+ ident.name),
                  partitioningAsV2,
                  df.queryExecution.analyzed,
                  tableSpec,
                  finalOptions,
                  ignoreIfExists = createMode == SaveMode.Ignore)
              }
            case _: TableProvider =>
              if (getTable.supports(BATCH_WRITE)) {
                throw QueryCompilationErrors.writeWithSaveModeUnsupportedBySourceError(
                  source, createMode.name())
              } else {
                // Streaming also uses the data source V2 API. So it may be that the data source
                // implements v2, but has no v2 implementation for batch writes. In that case, we
                // fallback to saving as though it's a V1 source.
                saveToV1Source(path)
              }
          }
      }

    } else {
      saveToV1Source(path)
    }
  }

  private def getOptionsWithPath(path: Option[String]): CaseInsensitiveMap[String] = {
    if (path.isEmpty) {
      extraOptions
    } else {
      extraOptions + ("path" -> path.get)
    }
  }

  private def saveToV1Source(path: Option[String]): Unit = {
    partitioningColumns.foreach { columns =>
      extraOptions = extraOptions + (
        DataSourceUtils.PARTITIONING_COLUMNS_KEY ->
        DataSourceUtils.encodePartitioningColumns(columns))
    }

    val optionsWithPath = getOptionsWithPath(path)

    // Code path for data source v1.
    runCommand(df.sparkSession) {
      DataSource(
        sparkSession = df.sparkSession,
        className = source,
        partitionColumns = partitioningColumns.getOrElse(Nil),
        options = optionsWithPath.originalMap).planForWriting(mode, df.logicalPlan)
    }
  }

  /**
   * Inserts the content of the `DataFrame` to the specified table. It requires that
   * the schema of the `DataFrame` is the same as the schema of the table.
   *
   * @note Unlike `saveAsTable`, `insertInto` ignores the column names and just uses position-based
   * resolution. For example:
   *
   * @note SaveMode.ErrorIfExists and SaveMode.Ignore behave as SaveMode.Append in `insertInto` as
   *       `insertInto` is not a table creating operation.
   *
   * {{{
   *    scala> Seq((1, 2)).toDF("i", "j").write.mode("overwrite").saveAsTable("t1")
   *    scala> Seq((3, 4)).toDF("j", "i").write.insertInto("t1")
   *    scala> Seq((5, 6)).toDF("a", "b").write.insertInto("t1")
   *    scala> sql("select * from t1").show
   *    +---+---+
   *    |  i|  j|
   *    +---+---+
   *    |  5|  6|
   *    |  3|  4|
   *    |  1|  2|
   *    +---+---+
   * }}}
   *
   * Because it inserts data to an existing table, format or options will be ignored.
   *
   * @since 1.4.0
   */
  def insertInto(tableName: String): Unit = {
    import df.sparkSession.sessionState.analyzer.{AsTableIdentifier, NonSessionCatalogAndIdentifier, SessionCatalogAndIdentifier}
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

    assertNotBucketed("insertInto")

    if (partitioningColumns.isDefined) {
      throw QueryCompilationErrors.partitionByDoesNotAllowedWhenUsingInsertIntoError()
    }

    val session = df.sparkSession
    val canUseV2 = lookupV2Provider().isDefined

    session.sessionState.sqlParser.parseMultipartIdentifier(tableName) match {
      case NonSessionCatalogAndIdentifier(catalog, ident) =>
        insertInto(catalog, ident)

      case SessionCatalogAndIdentifier(catalog, ident)
          if canUseV2 && ident.namespace().length <= 1 =>
        insertInto(catalog, ident)

      case AsTableIdentifier(tableIdentifier) =>
        insertInto(tableIdentifier)
      case other =>
        throw QueryCompilationErrors.cannotFindCatalogToHandleIdentifierError(other.quoted)
    }
  }

  private def insertInto(catalog: CatalogPlugin, ident: Identifier): Unit = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

    val table = catalog.asTableCatalog.loadTable(ident, getWritePrivileges.toSet.asJava) match {
      case _: V1Table =>
        return insertInto(TableIdentifier(ident.name(), ident.namespace().headOption))
      case t =>
        DataSourceV2Relation.create(t, Some(catalog), Some(ident))
    }

    val command = mode match {
      case SaveMode.Append | SaveMode.ErrorIfExists | SaveMode.Ignore =>
        AppendData.byPosition(table, df.logicalPlan, extraOptions.toMap)

      case SaveMode.Overwrite =>
        val conf = df.sparkSession.sessionState.conf
        val dynamicPartitionOverwrite = table.table.partitioning.size > 0 &&
          conf.partitionOverwriteMode == PartitionOverwriteMode.DYNAMIC

        if (dynamicPartitionOverwrite) {
          OverwritePartitionsDynamic.byPosition(table, df.logicalPlan, extraOptions.toMap)
        } else {
          OverwriteByExpression.byPosition(table, df.logicalPlan, Literal(true), extraOptions.toMap)
        }
    }

    runCommand(df.sparkSession) {
      command
    }
  }

  private def insertInto(tableIdent: TableIdentifier): Unit = {
    runCommand(df.sparkSession) {
      InsertIntoStatement(
        table = UnresolvedRelation(tableIdent).requireWritePrivileges(getWritePrivileges),
        partitionSpec = Map.empty[String, Option[String]],
        Nil,
        query = df.logicalPlan,
        overwrite = mode == SaveMode.Overwrite,
        ifPartitionNotExists = false)
    }
  }

  private def getWritePrivileges: Seq[TableWritePrivilege] = mode match {
    case SaveMode.Overwrite => Seq(INSERT, DELETE)
    case _ => Seq(INSERT)
  }

  private def getBucketSpec: Option[BucketSpec] = {
    if (sortColumnNames.isDefined && numBuckets.isEmpty) {
      throw QueryCompilationErrors.sortByWithoutBucketingError()
    }

    numBuckets.map { n =>
      BucketSpec(n, bucketColumnNames.get, sortColumnNames.getOrElse(Nil))
    }
  }

  private def assertNotBucketed(operation: String): Unit = {
    if (getBucketSpec.isDefined) {
      if (sortColumnNames.isEmpty) {
        throw QueryCompilationErrors.bucketByUnsupportedByOperationError(operation)
      } else {
        throw QueryCompilationErrors.bucketByAndSortByUnsupportedByOperationError(operation)
      }
    }
  }

  private def assertNotPartitioned(operation: String): Unit = {
    if (partitioningColumns.isDefined) {
      throw QueryCompilationErrors.operationNotSupportPartitioningError(operation)
    }
  }

  /**
   * Saves the content of the `DataFrame` as the specified table.
   *
   * In the case the table already exists, behavior of this function depends on the
   * save mode, specified by the `mode` function (default to throwing an exception).
   * When `mode` is `Overwrite`, the schema of the `DataFrame` does not need to be
   * the same as that of the existing table.
   *
   * When `mode` is `Append`, if there is an existing table, we will use the format and options of
   * the existing table. The column order in the schema of the `DataFrame` doesn't need to be same
   * as that of the existing table. Unlike `insertInto`, `saveAsTable` will use the column names to
   * find the correct column positions. For example:
   *
   * {{{
   *    scala> Seq((1, 2)).toDF("i", "j").write.mode("overwrite").saveAsTable("t1")
   *    scala> Seq((3, 4)).toDF("j", "i").write.mode("append").saveAsTable("t1")
   *    scala> sql("select * from t1").show
   *    +---+---+
   *    |  i|  j|
   *    +---+---+
   *    |  1|  2|
   *    |  4|  3|
   *    +---+---+
   * }}}
   *
   * In this method, save mode is used to determine the behavior if the data source table exists in
   * Spark catalog. We will always overwrite the underlying data of data source (e.g. a table in
   * JDBC data source) if the table doesn't exist in Spark catalog, and will always append to the
   * underlying data of data source if the table already exists.
   *
   * When the DataFrame is created from a non-partitioned `HadoopFsRelation` with a single input
   * path, and the data source provider can be mapped to an existing Hive builtin SerDe (i.e. ORC
   * and Parquet), the table is persisted in a Hive compatible format, which means other systems
   * like Hive will be able to read this table. Otherwise, the table is persisted in a Spark SQL
   * specific format.
   *
   * @since 1.4.0
   */
  def saveAsTable(tableName: String): Unit = {
    import df.sparkSession.sessionState.analyzer.{AsTableIdentifier, NonSessionCatalogAndIdentifier, SessionCatalogAndIdentifier}
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

    val session = df.sparkSession
    val canUseV2 = lookupV2Provider().isDefined || (df.sparkSession.sessionState.conf.getConf(
        SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION).isDefined &&
        !df.sparkSession.sessionState.catalogManager.catalog(CatalogManager.SESSION_CATALOG_NAME)
          .isInstanceOf[DelegatingCatalogExtension])

    session.sessionState.sqlParser.parseMultipartIdentifier(tableName) match {
      case nameParts @ NonSessionCatalogAndIdentifier(catalog, ident) =>
        saveAsTable(catalog.asTableCatalog, ident, nameParts)

      case nameParts @ SessionCatalogAndIdentifier(catalog, ident)
          if canUseV2 && ident.namespace().length <= 1 =>
        saveAsTable(catalog.asTableCatalog, ident, nameParts)

      case AsTableIdentifier(tableIdentifier) =>
        saveAsTable(tableIdentifier)

      case other =>
        throw QueryCompilationErrors.cannotFindCatalogToHandleIdentifierError(other.quoted)
    }
  }


  private def saveAsTable(
      catalog: TableCatalog, ident: Identifier, nameParts: Seq[String]): Unit = {
    val tableOpt = try Option(catalog.loadTable(ident, getWritePrivileges.toSet.asJava)) catch {
      case _: NoSuchTableException => None
    }

    val command = (mode, tableOpt) match {
      case (_, Some(_: V1Table)) =>
        return saveAsTable(TableIdentifier(ident.name(), ident.namespace().headOption))

      case (SaveMode.Append, Some(table)) =>
        checkPartitioningMatchesV2Table(table)
        val v2Relation = DataSourceV2Relation.create(table, Some(catalog), Some(ident))
        AppendData.byName(v2Relation, df.logicalPlan, extraOptions.toMap)

      case (SaveMode.Overwrite, _) =>
        val tableSpec = UnresolvedTableSpec(
          properties = Map.empty,
          provider = Some(source),
          optionExpression = OptionList(Seq.empty),
          location = extraOptions.get("path"),
          comment = extraOptions.get(TableCatalog.PROP_COMMENT),
          serde = None,
          external = false)
        ReplaceTableAsSelect(
          UnresolvedIdentifier(nameParts),
          partitioningAsV2,
          df.queryExecution.analyzed,
          tableSpec,
          writeOptions = extraOptions.toMap,
          orCreate = true) // Create the table if it doesn't exist

      case (other, _) =>
        // We have a potential race condition here in AppendMode, if the table suddenly gets
        // created between our existence check and physical execution, but this can't be helped
        // in any case.
        val tableSpec = UnresolvedTableSpec(
          properties = Map.empty,
          provider = Some(source),
          optionExpression = OptionList(Seq.empty),
          location = extraOptions.get("path"),
          comment = extraOptions.get(TableCatalog.PROP_COMMENT),
          serde = None,
          external = false)

        CreateTableAsSelect(
          UnresolvedIdentifier(nameParts),
          partitioningAsV2,
          df.queryExecution.analyzed,
          tableSpec,
          writeOptions = extraOptions.toMap,
          other == SaveMode.Ignore)
    }

    runCommand(df.sparkSession) {
      command
    }
  }

  private def saveAsTable(tableIdent: TableIdentifier): Unit = {
    val catalog = df.sparkSession.sessionState.catalog
    val qualifiedIdent = catalog.qualifyIdentifier(tableIdent)
    val tableExists = catalog.tableExists(qualifiedIdent)

    (tableExists, mode) match {
      case (true, SaveMode.Ignore) =>
        // Do nothing

      case (true, SaveMode.ErrorIfExists) =>
        throw QueryCompilationErrors.tableAlreadyExistsError(qualifiedIdent)

      case (true, SaveMode.Overwrite) =>
        // Get all input data source or hive relations of the query.
        val srcRelations = df.logicalPlan.collect {
          case LogicalRelation(src: BaseRelation, _, _, _) => src
          case relation: HiveTableRelation => relation.tableMeta.identifier
        }

        val tableRelation = df.sparkSession.table(qualifiedIdent).queryExecution.analyzed
        EliminateSubqueryAliases(tableRelation) match {
          // check if the table is a data source table (the relation is a BaseRelation).
          case LogicalRelation(dest: BaseRelation, _, _, _) if srcRelations.contains(dest) =>
            throw QueryCompilationErrors.cannotOverwriteTableThatIsBeingReadFromError(
              qualifiedIdent)
          // check hive table relation when overwrite mode
          case relation: HiveTableRelation
              if srcRelations.contains(relation.tableMeta.identifier) =>
            throw QueryCompilationErrors.cannotOverwriteTableThatIsBeingReadFromError(
              qualifiedIdent)
          case _ => // OK
        }

        // Drop the existing table
        catalog.dropTable(qualifiedIdent, ignoreIfNotExists = true, purge = false)
        createTable(qualifiedIdent)
        // Refresh the cache of the table in the catalog.
        catalog.refreshTable(qualifiedIdent)

      case _ => createTable(qualifiedIdent)
    }
  }

  private def createTable(tableIdent: TableIdentifier): Unit = {
    val storage = DataSource.buildStorageFormatFromOptions(extraOptions.toMap)
    val tableType = if (storage.locationUri.isDefined) {
      CatalogTableType.EXTERNAL
    } else {
      CatalogTableType.MANAGED
    }

    val tableDesc = CatalogTable(
      identifier = tableIdent,
      tableType = tableType,
      storage = storage,
      schema = new StructType,
      provider = Some(source),
      partitionColumnNames = partitioningColumns.getOrElse(Nil),
      bucketSpec = getBucketSpec)

    runCommand(df.sparkSession)(
      CreateTable(tableDesc, mode, Some(df.logicalPlan)))
  }

  /** Converts the provided partitioning and bucketing information to DataSourceV2 Transforms. */
  private def partitioningAsV2: Seq[Transform] = {
    val partitioning = partitioningColumns.map { colNames =>
      colNames.map(name => IdentityTransform(FieldReference(name)))
    }.getOrElse(Seq.empty[Transform])
    val bucketing =
      getBucketSpec.map(spec => CatalogV2Implicits.BucketSpecHelper(spec).asTransform).toSeq
    partitioning ++ bucketing
  }

  /**
   * For V2 DataSources, performs if the provided partitioning matches that of the table.
   * Partitioning information is not required when appending data to V2 tables.
   */
  private def checkPartitioningMatchesV2Table(existingTable: Table): Unit = {
    val v2Partitions = partitioningAsV2
    if (v2Partitions.isEmpty) return
    require(v2Partitions.sameElements(existingTable.partitioning()),
      "The provided partitioning does not match of the table.\n" +
      s" - provided: ${v2Partitions.mkString(", ")}\n" +
      s" - table: ${existingTable.partitioning().mkString(", ")}")
  }

  /**
   * Saves the content of the `DataFrame` to an external database table via JDBC. In the case the
   * table already exists in the external database, behavior of this function depends on the
   * save mode, specified by the `mode` function (default to throwing an exception).
   *
   * Don't create too many partitions in parallel on a large cluster; otherwise Spark might crash
   * your external database systems.
   *
   * JDBC-specific option and parameter documentation for storing tables via JDBC in
   * <a href="https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html#data-source-option">
   *   Data Source Option</a> in the version you use.
   *
   * @param table Name of the table in the external database.
   * @param connectionProperties JDBC database connection arguments, a list of arbitrary string
   *                             tag/value. Normally at least a "user" and "password" property
   *                             should be included. "batchsize" can be used to control the
   *                             number of rows per insert. "isolationLevel" can be one of
   *                             "NONE", "READ_COMMITTED", "READ_UNCOMMITTED", "REPEATABLE_READ",
   *                             or "SERIALIZABLE", corresponding to standard transaction
   *                             isolation levels defined by JDBC's Connection object, with default
   *                             of "READ_UNCOMMITTED".
   * @since 1.4.0
   */
  def jdbc(url: String, table: String, connectionProperties: Properties): Unit = {
    assertNotPartitioned("jdbc")
    assertNotBucketed("jdbc")
    // connectionProperties should override settings in extraOptions.
    this.extraOptions ++= connectionProperties.asScala
    // explicit url and dbtable should override all
    this.extraOptions ++= Seq("url" -> url, "dbtable" -> table)
    format("jdbc").save()
  }

  /**
   * Saves the content of the `DataFrame` in JSON format (<a href="http://jsonlines.org/">
   * JSON Lines text format or newline-delimited JSON</a>) at the specified path.
   * This is equivalent to:
   * {{{
   *   format("json").save(path)
   * }}}
   *
   * You can find the JSON-specific options for writing JSON files in
   * <a href="https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option">
   *   Data Source Option</a> in the version you use.
   *
   * @since 1.4.0
   */
  def json(path: String): Unit = {
    format("json").save(path)
  }

  /**
   * Saves the content of the `DataFrame` in Parquet format at the specified path.
   * This is equivalent to:
   * {{{
   *   format("parquet").save(path)
   * }}}
   *
   * Parquet-specific option(s) for writing Parquet files can be found in
   * <a href=
   *   "https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#data-source-option">
   *   Data Source Option</a> in the version you use.
   *
   * @since 1.4.0
   */
  def parquet(path: String): Unit = {
    format("parquet").save(path)
  }

  /**
   * Saves the content of the `DataFrame` in ORC format at the specified path.
   * This is equivalent to:
   * {{{
   *   format("orc").save(path)
   * }}}
   *
   * ORC-specific option(s) for writing ORC files can be found in
   * <a href=
   *   "https://spark.apache.org/docs/latest/sql-data-sources-orc.html#data-source-option">
   *   Data Source Option</a> in the version you use.
   *
   * @since 1.5.0
   */
  def orc(path: String): Unit = {
    format("orc").save(path)
  }

  /**
   * Saves the content of the `DataFrame` in a text file at the specified path.
   * The DataFrame must have only one column that is of string type.
   * Each row becomes a new line in the output file. For example:
   * {{{
   *   // Scala:
   *   df.write.text("/path/to/output")
   *
   *   // Java:
   *   df.write().text("/path/to/output")
   * }}}
   * The text files will be encoded as UTF-8.
   *
   * You can find the text-specific options for writing text files in
   * <a href="https://spark.apache.org/docs/latest/sql-data-sources-text.html#data-source-option">
   *   Data Source Option</a> in the version you use.
   *
   * @since 1.6.0
   */
  def text(path: String): Unit = {
    format("text").save(path)
  }

  /**
   * Saves the content of the `DataFrame` in CSV format at the specified path.
   * This is equivalent to:
   * {{{
   *   format("csv").save(path)
   * }}}
   *
   * You can find the CSV-specific options for writing CSV files in
   * <a href="https://spark.apache.org/docs/latest/sql-data-sources-csv.html#data-source-option">
   *   Data Source Option</a> in the version you use.
   *
   * @since 2.0.0
   */
  def csv(path: String): Unit = {
    format("csv").save(path)
  }

  /**
   * Wrap a DataFrameWriter action to track the QueryExecution and time cost, then report to the
   * user-registered callback functions.
   */
  private def runCommand(session: SparkSession)(command: LogicalPlan): Unit = {
    val qe = new QueryExecution(session, command, df.queryExecution.tracker)
    qe.assertCommandExecuted()
  }

  private def lookupV2Provider(): Option[TableProvider] = {
    DataSource.lookupDataSourceV2(source, df.sparkSession.sessionState.conf) match {
      // TODO(SPARK-28396): File source v2 write path is currently broken.
      case Some(_: FileDataSourceV2) => None
      case other => other
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Builder pattern config options
  ///////////////////////////////////////////////////////////////////////////////////////

  private var source: String = df.sparkSession.sessionState.conf.defaultDataSourceName

  private var mode: SaveMode = SaveMode.ErrorIfExists

  private var extraOptions = CaseInsensitiveMap[String](Map.empty)

  private var partitioningColumns: Option[Seq[String]] = None

  private var bucketColumnNames: Option[Seq[String]] = None

  private var numBuckets: Option[Int] = None

  private var sortColumnNames: Option[Seq[String]] = None
}
