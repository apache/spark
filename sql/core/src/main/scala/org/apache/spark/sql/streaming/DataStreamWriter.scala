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

package org.apache.spark.sql.streaming

import java.util.Locale
import java.util.concurrent.TimeoutException

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Evolving
import org.apache.spark.api.java.function.VoidFunction2
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.plans.logical.{ColumnDefinition, CreateTable, OptionList, UnresolvedTableSpec}
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsWrite, Table, TableCatalog, TableProvider, V1Table, V2TableWithV1Fallback}
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.expressions.{ClusterByTransform, FieldReference}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.{DataSource, DataSourceUtils}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Utils, FileDataSourceV2}
import org.apache.spark.sql.execution.datasources.v2.python.PythonDataSourceV2
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.sources._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/**
 * Interface used to write a streaming `Dataset` to external storage systems (e.g. file systems,
 * key-value stores, etc). Use `Dataset.writeStream` to access this.
 *
 * @since 2.0.0
 */
@Evolving
final class DataStreamWriter[T] private[sql](ds: Dataset[T]) extends api.DataStreamWriter[T] {
  type DS[U] = Dataset[U]

  /** @inheritdoc */
  def outputMode(outputMode: OutputMode): this.type = {
    this.outputMode = outputMode
    this
  }

  /** @inheritdoc */
  def outputMode(outputMode: String): this.type = {
    this.outputMode = InternalOutputModes(outputMode)
    this
  }

  /** @inheritdoc */
  def trigger(trigger: Trigger): this.type = {
    this.trigger = trigger
    this
  }

  /** @inheritdoc */
  def queryName(queryName: String): this.type = {
    this.extraOptions += ("queryName" -> queryName)
    this
  }

  /** @inheritdoc */
  def format(source: String): this.type = {
    this.source = source
    this
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  def partitionBy(colNames: String*): this.type = {
    this.partitioningColumns = Option(colNames)
    validatePartitioningAndClustering()
    this
  }

  /** @inheritdoc */
  @scala.annotation.varargs
  def clusterBy(colNames: String*): this.type = {
    this.clusteringColumns = Option(colNames)
    validatePartitioningAndClustering()
    this
  }

  /** @inheritdoc */
  def option(key: String, value: String): this.type = {
    this.extraOptions += (key -> value)
    this
  }

  /** @inheritdoc */
  def options(options: scala.collection.Map[String, String]): this.type = {
    this.extraOptions ++= options
    this
  }

  /** @inheritdoc */
  def options(options: java.util.Map[String, String]): this.type = {
    this.options(options.asScala)
    this
  }

  /** @inheritdoc */
  def start(path: String): StreamingQuery = {
    if (!ds.sparkSession.sessionState.conf.legacyPathOptionBehavior &&
        extraOptions.contains("path")) {
      throw QueryCompilationErrors.setPathOptionAndCallWithPathParameterError("start")
    }
    startInternal(Some(path))
  }

  /** @inheritdoc */
  @throws[TimeoutException]
  def start(): StreamingQuery = startInternal(None)

  /** @inheritdoc */
  @Evolving
  @throws[TimeoutException]
  def toTable(tableName: String): StreamingQuery = {

    import ds.sparkSession.sessionState.analyzer.CatalogAndIdentifier
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    val parser = ds.sparkSession.sessionState.sqlParser
    val originalMultipartIdentifier = parser.parseMultipartIdentifier(tableName)
    val CatalogAndIdentifier(catalog, identifier) = originalMultipartIdentifier

    // Currently we don't create a logical streaming writer node in logical plan, so cannot rely
    // on analyzer to resolve it. Directly lookup only for temp view to provide clearer message.
    // TODO (SPARK-27484): we should add the writing node before the plan is analyzed.
    if (ds.sparkSession.sessionState.catalog.isTempView(originalMultipartIdentifier)) {
      throw QueryCompilationErrors.tempViewNotSupportStreamingWriteError(tableName)
    }

    if (!catalog.asTableCatalog.tableExists(identifier)) {
      import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

      val properties = normalizedClusteringCols.map { cols =>
        Map(
          DataSourceUtils.CLUSTERING_COLUMNS_KEY -> DataSourceUtils.encodePartitioningColumns(cols))
      }.getOrElse(Map.empty)
      val partitioningOrClusteringTransform = normalizedClusteringCols.map { colNames =>
        Array(ClusterByTransform(colNames.map(col => FieldReference(col)))).toImmutableArraySeq
      }.getOrElse(partitioningColumns.getOrElse(Nil).asTransforms.toImmutableArraySeq)

      /**
       * Note, currently the new table creation by this API doesn't fully cover the V2 table.
       * TODO (SPARK-33638): Full support of v2 table creation
       */
      val tableSpec = UnresolvedTableSpec(
        properties,
        Some(source),
        OptionList(Seq.empty),
        extraOptions.get("path"),
        None,
        None,
        external = false)
      val cmd = CreateTable(
        UnresolvedIdentifier(originalMultipartIdentifier),
        ds.schema.asNullable.map(ColumnDefinition.fromV1Column(_, parser)),
        partitioningOrClusteringTransform,
        tableSpec,
        ignoreIfExists = false)
      Dataset.ofRows(ds.sparkSession, cmd)
    }

    val tableInstance = catalog.asTableCatalog.loadTable(identifier)

    def writeToV1Table(table: CatalogTable): StreamingQuery = {
      if (table.tableType == CatalogTableType.VIEW) {
        throw QueryCompilationErrors.streamingIntoViewNotSupportedError(tableName)
      }
      require(table.provider.isDefined)
      if (source != table.provider.get) {
        throw QueryCompilationErrors.inputSourceDiffersFromDataSourceProviderError(
          source, tableName, table)
      }
      format(table.provider.get).startInternal(
        Some(new Path(table.location).toString), catalogTable = Some(table))
    }

    import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._
    tableInstance match {
      case t: SupportsWrite if t.supports(STREAMING_WRITE) =>
        startQuery(t, extraOptions, catalogAndIdent = Some(catalog.asTableCatalog, identifier))
      case t: V2TableWithV1Fallback =>
        writeToV1Table(t.v1Table)
      case t: V1Table =>
        writeToV1Table(t.v1Table)
      case t => throw QueryCompilationErrors.tableNotSupportStreamingWriteError(tableName, t)
    }
  }

  private def startInternal(
      path: Option[String],
      catalogTable: Option[CatalogTable] = None): StreamingQuery = {
    if (source.toLowerCase(Locale.ROOT) == DDLUtils.HIVE_PROVIDER) {
      throw QueryCompilationErrors.cannotOperateOnHiveDataSourceFilesError("write")
    }

    if (source == DataStreamWriter.SOURCE_NAME_MEMORY) {
      assertNotPartitioned(DataStreamWriter.SOURCE_NAME_MEMORY)
      if (extraOptions.get("queryName").isEmpty) {
        throw QueryCompilationErrors.queryNameNotSpecifiedForMemorySinkError()
      }
      val sink = new MemorySink()
      val resultDf = Dataset.ofRows(ds.sparkSession,
        MemoryPlan(sink, DataTypeUtils.toAttributes(ds.schema)))
      val recoverFromCheckpoint = outputMode == OutputMode.Complete()
      val query = startQuery(sink, extraOptions, recoverFromCheckpoint = recoverFromCheckpoint,
        catalogTable = catalogTable)
      resultDf.createOrReplaceTempView(query.name)
      query
    } else if (source == DataStreamWriter.SOURCE_NAME_FOREACH) {
      assertNotPartitioned(DataStreamWriter.SOURCE_NAME_FOREACH)
      val sink = ForeachWriterTable[Any](foreachWriter, foreachWriterEncoder)
      startQuery(sink, extraOptions, catalogTable = catalogTable)
    } else if (source == DataStreamWriter.SOURCE_NAME_FOREACH_BATCH) {
      assertNotPartitioned(DataStreamWriter.SOURCE_NAME_FOREACH_BATCH)
      if (trigger.isInstanceOf[ContinuousTrigger]) {
        throw QueryCompilationErrors.sourceNotSupportedWithContinuousTriggerError(source)
      }
      val sink = new ForeachBatchSink[T](foreachBatchWriter, ds.exprEnc)
      startQuery(sink, extraOptions, catalogTable = catalogTable)
    } else {
      val cls = DataSource.lookupDataSource(source, ds.sparkSession.sessionState.conf)
      val disabledSources =
        Utils.stringToSeq(ds.sparkSession.sessionState.conf.disabledV2StreamingWriters)
      val useV1Source = disabledSources.contains(cls.getCanonicalName) ||
        // file source v2 does not support streaming yet.
        classOf[FileDataSourceV2].isAssignableFrom(cls)

      val optionsWithPath = if (path.isEmpty) {
        extraOptions
      } else {
        extraOptions + ("path" -> path.get)
      }

      val sink = if (classOf[TableProvider].isAssignableFrom(cls) && !useV1Source) {
        val provider = cls.getConstructor().newInstance().asInstanceOf[TableProvider]
        val sessionOptions = DataSourceV2Utils.extractSessionConfigs(
          source = provider, conf = ds.sparkSession.sessionState.conf)
        val finalOptions = sessionOptions.filter { case (k, _) => !optionsWithPath.contains(k) } ++
          optionsWithPath.originalMap
        val dsOptions = new CaseInsensitiveStringMap(finalOptions.asJava)
        // If the source accepts external table metadata, here we pass the schema of input query
        // to `getTable`. This is for avoiding schema inference, which can be very expensive.
        // If the query schema is not compatible with the existing data, the behavior is undefined.
        val outputSchema = if (provider.supportsExternalMetadata()) {
          Some(ds.schema)
        } else {
          None
        }
        provider match {
          case p: PythonDataSourceV2 => p.setShortName(source)
          case _ =>
        }
        val table = DataSourceV2Utils.getTableFromProvider(
          provider, dsOptions, userSpecifiedSchema = outputSchema)
        import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._
        table match {
          case table: SupportsWrite if table.supports(STREAMING_WRITE) =>
            table
          case _ => createV1Sink(optionsWithPath)
        }
      } else {
        createV1Sink(optionsWithPath)
      }

      startQuery(sink, optionsWithPath, catalogTable = catalogTable)
    }
  }

  private def startQuery(
      sink: Table,
      newOptions: CaseInsensitiveMap[String],
      recoverFromCheckpoint: Boolean = true,
      catalogAndIdent: Option[(TableCatalog, Identifier)] = None,
      catalogTable: Option[CatalogTable] = None): StreamingQuery = {
    val useTempCheckpointLocation = DataStreamWriter.SOURCES_ALLOW_ONE_TIME_QUERY.contains(source)

    ds.sparkSession.sessionState.streamingQueryManager.startQuery(
      newOptions.get("queryName"),
      newOptions.get("checkpointLocation"),
      ds,
      newOptions.originalMap,
      sink,
      outputMode,
      useTempCheckpointLocation = useTempCheckpointLocation,
      recoverFromCheckpointLocation = recoverFromCheckpoint,
      trigger = trigger,
      catalogAndIdent = catalogAndIdent,
      catalogTable = catalogTable)
  }

  private def createV1Sink(optionsWithPath: CaseInsensitiveMap[String]): Sink = {
    // Do not allow the user to specify clustering columns in the options. Ignoring this option is
    // consistent with the behavior of DataFrameWriter on non Path-based tables and with the
    // behavior of DataStreamWriter on partitioning columns specified in options.
    val optionsWithoutClusteringKey =
      optionsWithPath.originalMap - DataSourceUtils.CLUSTERING_COLUMNS_KEY

    val optionsWithClusteringColumns = normalizedClusteringCols match {
      case Some(cols) => optionsWithoutClusteringKey + (
        DataSourceUtils.CLUSTERING_COLUMNS_KEY ->
          DataSourceUtils.encodePartitioningColumns(cols))
      case None => optionsWithoutClusteringKey
    }
    val ds = DataSource(
      this.ds.sparkSession,
      className = source,
      options = optionsWithClusteringColumns,
      partitionColumns = normalizedParCols.getOrElse(Nil))
    ds.createSink(outputMode)
  }

  /** @inheritdoc */
  def foreach(writer: ForeachWriter[T]): this.type = {
    foreachImplementation(writer.asInstanceOf[ForeachWriter[Any]])
  }

  private[sql] def foreachImplementation(writer: ForeachWriter[Any],
      encoder: Option[ExpressionEncoder[Any]] = None): this.type = {
    this.source = DataStreamWriter.SOURCE_NAME_FOREACH
    this.foreachWriter = if (writer != null) {
      ds.sparkSession.sparkContext.clean(writer)
    } else {
      throw new IllegalArgumentException("foreach writer cannot be null")
    }
    encoder.foreach(e => this.foreachWriterEncoder = e)
    this
  }

  /** @inheritdoc */
  @Evolving
  def foreachBatch(function: (Dataset[T], Long) => Unit): this.type = {
    this.source = DataStreamWriter.SOURCE_NAME_FOREACH_BATCH
    if (function == null) throw new IllegalArgumentException("foreachBatch function cannot be null")
    this.foreachBatchWriter = function
    this
  }

  private def normalizedParCols: Option[Seq[String]] = partitioningColumns.map { cols =>
    cols.map(normalize(_, "Partition"))
  }

  private def normalizedClusteringCols: Option[Seq[String]] = clusteringColumns.map { cols =>
    cols.map(normalize(_, "Clustering"))
  }

  /**
   * The given column name may not be equal to any of the existing column names if we were in
   * case-insensitive context. Normalize the given column name to the real one so that we don't
   * need to care about case sensitivity afterwards.
   */
  private def normalize(columnName: String, columnType: String): String = {
    val validColumnNames = ds.logicalPlan.output.map(_.name)
    validColumnNames.find(ds.sparkSession.sessionState.analyzer.resolver(_, columnName))
      .getOrElse(throw QueryCompilationErrors.columnNotFoundInExistingColumnsError(
        columnType, columnName, validColumnNames))
  }

  private def assertNotPartitioned(operation: String): Unit = {
    if (partitioningColumns.isDefined) {
      throw QueryCompilationErrors.operationNotSupportPartitioningError(operation)
    }
  }

  // Validate that partitionBy isn't used with clusterBy.
  private def validatePartitioningAndClustering(): Unit = {
    if (clusteringColumns.nonEmpty && partitioningColumns.nonEmpty) {
      throw QueryCompilationErrors.clusterByWithPartitionedBy()
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Covariant Overrides
  ///////////////////////////////////////////////////////////////////////////////////////

  /** @inheritdoc */
  override def option(key: String, value: Boolean): this.type = super.option(key, value)

  /** @inheritdoc */
  override def option(key: String, value: Long): this.type = super.option(key, value)

  /** @inheritdoc */
  override def option(key: String, value: Double): this.type = super.option(key, value)

  /** @inheritdoc */
  @Evolving
  override def foreachBatch(function: VoidFunction2[Dataset[T], java.lang.Long]): this.type =
    super.foreachBatch(function)

  ///////////////////////////////////////////////////////////////////////////////////////
  // Builder pattern config options
  ///////////////////////////////////////////////////////////////////////////////////////

  private var source: String = ds.sparkSession.sessionState.conf.defaultDataSourceName

  private var outputMode: OutputMode = OutputMode.Append

  private var trigger: Trigger = Trigger.ProcessingTime(0L)

  private var extraOptions = CaseInsensitiveMap[String](Map.empty)

  private var foreachWriter: ForeachWriter[Any] = _

  private var foreachWriterEncoder: ExpressionEncoder[Any] =
    ds.exprEnc.asInstanceOf[ExpressionEncoder[Any]]

  private var foreachBatchWriter: (Dataset[T], Long) => Unit = _

  private var partitioningColumns: Option[Seq[String]] = None

  private var clusteringColumns: Option[Seq[String]] = None
}

object DataStreamWriter {
  val SOURCE_NAME_MEMORY: String = "memory"
  val SOURCE_NAME_FOREACH: String = "foreach"
  val SOURCE_NAME_FOREACH_BATCH: String = "foreachBatch"
  val SOURCE_NAME_CONSOLE: String = "console"
  val SOURCE_NAME_TABLE: String = "table"
  val SOURCE_NAME_NOOP: String = "noop"

  // these writer sources are also used for one-time query, hence allow temp checkpoint location
  val SOURCES_ALLOW_ONE_TIME_QUERY: Seq[String] = Seq(SOURCE_NAME_MEMORY, SOURCE_NAME_FOREACH,
    SOURCE_NAME_FOREACH_BATCH, SOURCE_NAME_CONSOLE, SOURCE_NAME_NOOP)
}
