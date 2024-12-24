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

package org.apache.spark.sql.internal

import java.util.Locale

import scala.jdk.CollectionConverters._

import org.apache.spark.annotation.Stable
import org.apache.spark.sql.{DataFrameWriter, Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, NoSuchTableException, UnresolvedIdentifier, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.catalog.TableWritePrivilege._
import org.apache.spark.sql.connector.expressions.{ClusterByTransform, FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.{CreateTable, DataSource, DataSourceUtils, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.internal.SQLConf.PartitionOverwriteMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._

/**
 * Interface used to write a [[Dataset]] to external storage systems (e.g. file systems,
 * key-value stores, etc). Use `Dataset.write` to access this.
 *
 * @since 1.4.0
 */
@Stable
final class DataFrameWriterImpl[T] private[sql](ds: Dataset[T]) extends DataFrameWriter[T] {
  format(ds.sparkSession.sessionState.conf.defaultDataSourceName)

  private val df = ds.toDF()

  /** @inheritdoc */
  override def mode(saveMode: SaveMode): this.type = super.mode(saveMode)

  /** @inheritdoc */
  override def mode(saveMode: String): this.type = super.mode(saveMode)

  /** @inheritdoc */
  override def format(source: String): this.type = super.format(source)

  /** @inheritdoc */
  override def option(key: String, value: String): this.type = super.option(key, value)

  /** @inheritdoc */
  override def option(key: String, value: Boolean): this.type = super.option(key, value)

  /** @inheritdoc */
  override def option(key: String, value: Long): this.type = super.option(key, value)

  /** @inheritdoc */
  override def option(key: String, value: Double): this.type = super.option(key, value)

  /** @inheritdoc */
  override def options(options: scala.collection.Map[String, String]): this.type =
    super.options(options)

  /** @inheritdoc */
  override def options(options: java.util.Map[String, String]): this.type =
    super.options(options)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def partitionBy(colNames: String*): this.type = super.partitionBy(colNames: _*)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def bucketBy(numBuckets: Int, colName: String, colNames: String*): this.type =
    super.bucketBy(numBuckets, colName, colNames: _*)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def sortBy(colName: String, colNames: String*): this.type =
    super.sortBy(colName, colNames: _*)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def clusterBy(colName: String, colNames: String*): this.type =
    super.clusterBy(colName, colNames: _*)

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

      val finalOptions = sessionOptions.filter { case (k, _) => !optionsWithPath.contains(k) } ++
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
                collation = extraOptions.get(TableCatalog.PROP_COLLATION),
                serde = None,
                external = false)
              runCommand(df.sparkSession) {
                CreateTableAsSelect(
                  UnresolvedIdentifier(
                    catalog.name +: ident.namespace.toImmutableArraySeq :+ ident.name),
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
    clusteringColumns.foreach { columns =>
      extraOptions = extraOptions + (
        DataSourceUtils.CLUSTERING_COLUMNS_KEY ->
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
        val dynamicPartitionOverwrite = table.table.partitioning.length > 0 &&
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
    isBucketed()
    numBuckets.map { n =>
      BucketSpec(n, bucketColumnNames.get, sortColumnNames.getOrElse(Nil))
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
          .isInstanceOf[CatalogExtension])

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
          collation = extraOptions.get(TableCatalog.PROP_COLLATION),
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
          collation = extraOptions.get(TableCatalog.PROP_COLLATION),
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
          case l: LogicalRelation => l.relation
          case relation: HiveTableRelation => relation.tableMeta.identifier
        }

        val tableRelation = df.sparkSession.table(qualifiedIdent).queryExecution.analyzed
        EliminateSubqueryAliases(tableRelation) match {
          // check if the table is a data source table (the relation is a BaseRelation).
          case l: LogicalRelation if srcRelations.contains(l.relation) =>
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

    val properties = if (clusteringColumns.isEmpty) {
      Map.empty[String, String]
    } else {
      Map(ClusterBySpec.toPropertyWithoutValidation(
        ClusterBySpec.fromColumnNames(clusteringColumns.get)))
    }

    val tableDesc = CatalogTable(
      identifier = tableIdent,
      tableType = tableType,
      storage = storage,
      schema = new StructType,
      provider = Some(source),
      partitionColumnNames = partitioningColumns.getOrElse(Nil),
      bucketSpec = getBucketSpec,
      properties = properties)

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
    val clustering = clusteringColumns.map { colNames =>
      ClusterByTransform(colNames.map(FieldReference(_)))
    }
    partitioning ++ bucketing ++ clustering
  }

  /**
   * For V2 DataSources, performs if the provided partitioning matches that of the table.
   * Partitioning information is not required when appending data to V2 tables.
   */
  private def checkPartitioningMatchesV2Table(existingTable: Table): Unit = {
    val v2Partitions = partitioningAsV2
    if (v2Partitions.isEmpty) return
    require(v2Partitions.sameElements(existingTable.partitioning()),
      "The provided partitioning or clustering columns do not match the existing table's.\n" +
      s" - provided: ${v2Partitions.mkString(", ")}\n" +
      s" - table: ${existingTable.partitioning().mkString(", ")}")
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
}
