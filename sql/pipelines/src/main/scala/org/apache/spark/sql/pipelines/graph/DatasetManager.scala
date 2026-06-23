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

package org.apache.spark.sql.pipelines.graph

import scala.jdk.CollectionConverters._
import scala.util.control.{NonFatal, NoStackTrace}

import org.apache.spark.SparkException
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.PersistedView
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connector.catalog.{
  CatalogV2Util,
  Identifier,
  Table => V2Table,
  TableCatalog,
  TableChange,
  TableInfo
}
import org.apache.spark.sql.connector.catalog.CatalogV2Util.v2ColumnsToStructType
import org.apache.spark.sql.connector.expressions.{ClusterByTransform, Expressions, Transform}
import org.apache.spark.sql.execution.command.CreateViewCommand
import org.apache.spark.sql.pipelines.graph.QueryOrigin.ExceptionHelpers
import org.apache.spark.sql.pipelines.util.SchemaInferenceUtils.diffSchemas
import org.apache.spark.sql.pipelines.util.SchemaMergingUtils
import org.apache.spark.sql.types.StructType

/**
 * `DatasetManager` is responsible for materializing tables in the catalog based on the given
 * graph. For each table in the graph, it will create a table if none exists (or if this is a
 * full refresh), or merge the schema of an existing table to match the new flows writing to it.
 */
object DatasetManager extends Logging {

  /**
   * Wraps table materialization exceptions.
   *
   * The target use case of this exception is merely as a means to capture attribution -
   * 1. Indicate that the exception is associated with table materialization.
   * 2. Indicate which table materialization failed for.
   *
   * @param tableName The name of the table that failed to materialize.
   * @param cause The underlying exception that caused the materialization to fail.
   */
  case class TableMaterializationException(
      tableName: String,
      cause: Throwable
  ) extends Exception(cause)
      with NoStackTrace

  /**
   * Materializes the tables in the given graph. This method will create or update the tables
   * in the catalog based on the given graph and context.
   *
   * @param resolvedDataflowGraph The resolved [[DataflowGraph]] with resolved [[Flow]] sorted
   *                              in topological order.
   * @param context The context for the pipeline update.
   * @return The graph with materialized tables.
   */
  def materializeDatasets(
      resolvedDataflowGraph: DataflowGraph,
      context: PipelineUpdateContext
  ): DataflowGraph = {
    val (_, refreshTableIdentsSet, fullRefreshTableIdentsSet) = {
      constructFullRefreshSet(resolvedDataflowGraph.tables, context)
    }

    /** Return all the tables that need to be materialized from the given graph. */
    def tablesToMatz(graph: DataflowGraph): Seq[TableRefreshType] = {
      graph.tables
        .filter(t => fullRefreshTableIdentsSet.contains(t.identifier))
        .map(table => TableRefreshType(table, isFullRefresh = true)) ++
      graph.tables
        .filter(t => refreshTableIdentsSet.contains(t.identifier))
        .map(table => TableRefreshType(table, isFullRefresh = false))
    }

    val tablesToMaterialize = {
      tablesToMatz(resolvedDataflowGraph).map(t => t.table.identifier -> t).toMap
    }

    // materialized [[DataflowGraph]] where each table has been materialized and each table
    // has metadata (e.g., normalized table storage path) populated
    val materializedGraph: DataflowGraph = try {
      DataflowGraphTransformer
        .withDataflowGraphTransformer(resolvedDataflowGraph) { transformer =>
          transformer.transformTables { table =>
            if (tablesToMaterialize.keySet.contains(table.identifier)) {
              try {
                materializeTable(
                  resolvedDataflowGraph = resolvedDataflowGraph,
                  table = table,
                  isFullRefresh = tablesToMaterialize(table.identifier).isFullRefresh,
                  context = context
                )
              } catch {
                case NonFatal(e) =>
                  throw TableMaterializationException(
                    table.displayName,
                    cause = e.addOrigin(table.origin)
                  )
              }
            } else {
              table
            }
          }

        }
        .getDataflowGraph
    } catch {
      case e: SparkException if e.getCause != null => throw e.getCause
    }
    materializeViews(materializedGraph, context)
    materializedGraph
  }

  /**
   * Publish or refresh all the [[PersistedView]]s in the specified [[DataflowGraph]]
   *
   * @param virtualizedConnectedGraphWithTables virtualizedConnectedGraph that has table information
   *                                            from the graph.
   */
  private def materializeViews(
      virtualizedConnectedGraphWithTables: DataflowGraph,
      context: PipelineUpdateContext): Unit = {
    var viewsToPublish: Set[PersistedView] =
      virtualizedConnectedGraphWithTables.persistedViews.toSet
    var publishedViews: Set[TableIdentifier] = Set.empty
    var failedViews: Set[TableIdentifier] = Set.empty

    // To publish a view, it is required that all the input sources must exist in the metastore.
    //  Thereby, if a Persisted View target reads another Persisted View source, the source must be
    //  published first.
    //  Here we make sure all the persisted views are published in correct order
    val persistedViewIdentifiers =
      virtualizedConnectedGraphWithTables.persistedViews.map(_.identifier).toSet
    val viewToFlowMap =
      ViewHelpers.persistedViewIdentifierToFlow(graph = virtualizedConnectedGraphWithTables)
    val materializationDependencies =
      virtualizedConnectedGraphWithTables.persistedViews.map { v =>
        val flow = viewToFlowMap(v.identifier)
        val inputs = flow.inputs.intersect(persistedViewIdentifiers)
        (v.identifier, inputs)
      }.toMap

    // As long as all views are not materialized, we try to materialize them
    while (viewsToPublish.nonEmpty) {
      // Mark any views with failed inputs as skipped
      viewsToPublish
        .filter { v =>
          materializationDependencies(v.identifier)
            .exists(failedViews.contains)
        }
        .foreach { v =>
          val flowToView = viewToFlowMap(v.identifier)
          context.flowProgressEventLogger.recordSkipped(flowToView)

          failedViews += v.identifier
          viewsToPublish -= v
        }

      // Persist any views without pending inputs
      viewsToPublish
        .filter { v =>
          val pendingInputs =
            materializationDependencies(v.identifier).diff(publishedViews)

          pendingInputs.isEmpty
        }
        .foreach { v =>
          val flowToView = viewToFlowMap(v.identifier)
          try {
            materializeView(v, flowToView, context.spark)
            publishedViews += v.identifier
            viewsToPublish -= v
          } catch {
            case NonFatal(ex) =>
              context.flowProgressEventLogger.recordFailed(
                flowToView,
                ex,
                logAsWarn = false
              )
              failedViews += v.identifier
              viewsToPublish -= v
          }
        }
    }
  }

  private def materializeView(view: View, flow: ResolvedFlow, spark: SparkSession): Unit = {
    val command = CreateViewCommand(
      name = view.identifier,
      userSpecifiedColumns = Nil,
      viewType = PersistedView,
      comment = view.comment,
      collation = None,
      properties = view.properties,
      originalText = view.sqlText,
      plan = flow.df.logicalPlan,
      allowExisting = true,
      replace = true,
      isAnalyzed = true
    )

    val queryContext = flow.queryContext

    val catalogManager = spark.sessionState.catalogManager
    val currentCatalogName = catalogManager.currentCatalog.name()
    val currentNamespace = catalogManager.currentNamespace
    try {
      // Using the catalog and database from the flow ensures that reads within the view are
      // directed to the right catalog/database.
      queryContext.currentCatalog.foreach(catalogManager.setCurrentCatalog)
      queryContext.currentDatabase.map(d => Array(d)).foreach(catalogManager.setCurrentNamespace)
      command.run(spark)
    } finally {
      catalogManager.setCurrentCatalog(currentCatalogName)
      catalogManager.setCurrentNamespace(currentNamespace)
    }
  }

  /**
   * Materializes a table in the catalog. This method will create or update the table in the
   * catalog based on the given table and context.
   * @param resolvedDataflowGraph The resolved [[DataflowGraph]] used to infer the table schema.
   * @param table The table to be materialized.
   * @param isFullRefresh Whether this table should be full refreshed or not.
   * @param context The context for the pipeline update.
   * @return The materialized table (with additional metadata set).
   */
  private def materializeTable(
      resolvedDataflowGraph: DataflowGraph,
      table: Table,
      isFullRefresh: Boolean,
      context: PipelineUpdateContext): Table = {
    logInfo(log"Materializing metadata for table ${MDC(LogKeys.TABLE_NAME, table.identifier)}.")
    val catalogManager = context.spark.sessionState.catalogManager
    val catalog = (table.identifier.catalog match {
      case Some(catalogName) =>
        catalogManager.catalog(catalogName)
      case None =>
        catalogManager.currentCatalog
    }).asInstanceOf[TableCatalog]

    val identifier =
      Identifier.of(Array(table.identifier.database.get), table.identifier.identifier)
    val outputSchema = table.specifiedSchema.getOrElse(
      resolvedDataflowGraph.inferredSchema(table.identifier).asNullable
    )
    val mergedProperties = resolveTableProperties(table, identifier)
    val partitioning = table.partitionCols.toSeq.flatten.map(Expressions.identity)
    val clustering = table.clusterCols.map(cols =>
      ClusterByTransform(cols.map(col => Expressions.column(col)))
    ).toSeq

    // Validate that partition and cluster columns don't coexist
    if (partitioning.nonEmpty && clustering.nonEmpty) {
      throw new AnalysisException(
        errorClass = "SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED",
        messageParameters = Map.empty
      )
    }

    val allTransforms = partitioning ++ clustering

    val existingTableOpt = loadTableIfExists(catalog, identifier)

    // Error if partitioning/clustering doesn't match
    existingTableOpt.foreach { existingTable =>
      val existingTransforms = existingTable.partitioning().toSeq
      if (existingTransforms != allTransforms) {
        throw new AnalysisException(
          errorClass = "CANNOT_UPDATE_PARTITION_COLUMNS",
          messageParameters = Map(
            "existingPartitionColumns" -> existingTransforms.mkString(", "),
            "requestedPartitionColumns" -> allTransforms.mkString(", ")
          )
        )
      }
    }

    // A streaming table on a non-full-refresh run is maintained incrementally: its existing data is
    // preserved and its schema is merged with (not replaced by) the schema computed in this run.
    // Every other case (materialized views, and any full refresh) is recomputed from scratch:
    // existing data is wiped and the schema is taken directly from this run's computed schema.
    val isTableIncrementallyUpdated = table.isStreamingTable && !isFullRefresh

    // Wipe the data if we need to
    if (existingTableOpt.isDefined && !isTableIncrementallyUpdated) {
      context.spark.sql(s"TRUNCATE TABLE ${table.identifier.quotedString}")
    }

    if (isFullRefresh) {
      // On full refresh, drop the AutoCDC auxiliary state associated with this table (if any) so
      // that stale delete-tracking data and table properties are not carried forward into the new
      // table generation. We unconditionally issue the DROP for every fully-refreshed target.

      // Intentionally DROP and not TRUNCATE: the auxiliary table is an internal state store
      // that is not part of the dataflow graph, so it does not participate in regular schema
      // evolution like user tables do. On a full refresh we want a clean recreation against
      // the new target schema rather than carrying forward the previous generation's layout.

      val auxiliaryTableId = AutoCdcAuxiliaryTable.identifier(table.identifier)
      context.spark.sql(s"DROP TABLE IF EXISTS ${auxiliaryTableId.quotedString}")
    }

    // Create the table if absent, otherwise evolve it (schema + properties).
    existingTableOpt match {
      case Some(existingTable) =>
        evolveTable(
          catalog = catalog,
          tableIdentifier = identifier,
          existingTable = existingTable,
          desiredSchema = outputSchema,
          properties = mergedProperties,
          mergeWithExistingSchema = isTableIncrementallyUpdated
        )
      case None =>
        createTable(
          catalog = catalog,
          tableIdentifier = identifier,
          schema = outputSchema,
          properties = mergedProperties,
          transforms = allTransforms
        )
    }

    table.copy(
      normalizedPath = Option(
        catalog.loadTable(identifier).properties().get(TableCatalog.PROP_LOCATION)
      )
    )
  }

  /** Loads the table at `identifier` from `catalog`, or `None` if it does not exist. */
  private def loadTableIfExists(
      catalog: TableCatalog,
      identifier: Identifier): Option[V2Table] = {
    Option.when(catalog.tableExists(identifier))(catalog.loadTable(identifier))
  }

  /**
   * Creates the table at `identifier` with the given schema, properties, and partition/cluster
   * transforms. Used when no table yet exists at the identifier.
   *
   * @param schema     the schema to create the table with.
   * @param properties the table properties to create the table with.
   * @param transforms the partition/cluster transforms to create the table with.
   */
  private def createTable(
      catalog: TableCatalog,
      tableIdentifier: Identifier,
      schema: StructType,
      properties: Map[String, String],
      transforms: Seq[Transform]): Unit = {
    catalog.createTable(
      tableIdentifier,
      new TableInfo.Builder()
        .withProperties(properties.asJava)
        .withColumns(CatalogV2Util.structTypeToV2Columns(schema))
        .withPartitions(transforms.toArray)
        .build()
    )
  }

  /**
   * Evolves the already-existing `existingTable` at `identifier` in place by diffing its schema
   * and properties, skipping the catalog `alterTable` entirely when nothing actually changes.
   * Partitioning/clustering cannot change in place, so no transforms are accepted here.
   *
   * @param existingTable           the currently materialized table.
   * @param desiredSchema           the schema the table should have as computed in the current
   *                                execution (the user-specified or inferred schema). This is the
   *                                "incoming" side and may differ from `existingTable`'s recorded
   *                                schema due to schema evolution across runs.
   * @param properties              the table properties to (re)set on evolve.
   * @param mergeWithExistingSchema whether the effective schema is the merge of the existing and
   *                                desired schemas (additive evolution) rather than the desired
   *                                schema as-is.
   */
  private def evolveTable(
      catalog: TableCatalog,
      tableIdentifier: Identifier,
      existingTable: V2Table,
      desiredSchema: StructType,
      properties: Map[String, String],
      mergeWithExistingSchema: Boolean): Unit = {
    val currentSchema = v2ColumnsToStructType(existingTable.columns())
    val targetSchema = if (mergeWithExistingSchema) {
      SchemaMergingUtils.mergeSchemas(currentSchema, desiredSchema)
    } else {
      desiredSchema
    }
    val columnChanges = diffSchemas(currentSchema, targetSchema)

    val existingProperties = existingTable.properties()
    val propertyChanges = properties.collect {
      case (k, v) if !Option(existingProperties.get(k)).contains(v) =>
        TableChange.setProperty(k, v)
    }
    val allTableChanges = columnChanges ++ propertyChanges

    // If there are no table changes to evolve with, avoid the no-op round-trip alter altogether.
    if (allTableChanges.nonEmpty) {
      catalog.alterTable(tableIdentifier, allTableChanges.toArray: _*)
    }
  }

  /**
   * Some fields on the [[Table]] object are represented as reserved table properties by the catalog
   * APIs. This method creates a table properties map that merges the user-provided table properties
   * with these reserved properties.
   */
  private def resolveTableProperties(table: Table, identifier: Identifier): Map[String, String] = {
    val validatedAndCanonicalizedProps =
      PipelinesTableProperties.validateAndCanonicalize(
        table.properties,
        warnFunction = s => logWarning(s)
      )

    val specialProps = Seq(
      (table.comment, "comment", TableCatalog.PROP_COMMENT),
      (table.format, "format", TableCatalog.PROP_PROVIDER)
    ).map {
        case (value, name, reservedPropKey) =>
          validatedAndCanonicalizedProps.get(reservedPropKey).foreach { pc =>
            if (value.isDefined && value.get != pc) {
              throw new IllegalArgumentException(
                s"For dataset $identifier, $name '${value.get}' does not match value '$pc' for " +
                s"reserved table property '$reservedPropKey''"
              )
            }
          }
          reservedPropKey -> value
      }
      .collect { case (key, Some(value)) => key -> value }

    validatedAndCanonicalizedProps ++ specialProps
  }

  /**
   * A case class that represents the type of refresh for a table.
   * @param table The table to be refreshed.
   * @param isFullRefresh Whether this table should be fully refreshed or not.
   */
  private case class TableRefreshType(table: Table, isFullRefresh: Boolean)

  /**
   * Constructs the set of tables that should be fully refreshed and the set of tables that
   * should be refreshed.
   */
  private def constructFullRefreshSet(
      graphTables: Seq[Table],
      context: PipelineUpdateContext
  ): (Seq[Table], Seq[TableIdentifier], Seq[TableIdentifier]) = {
    val (fullRefreshTablesSet, refreshTablesSet) = {
      val specifiedFullRefreshTables = context.fullRefreshTables.filter(graphTables)
      val specifiedRefreshTables = context.refreshTables.filter(graphTables)

      val (fullRefreshAllowed, fullRefreshNotAllowed) = specifiedFullRefreshTables.partition { t =>
        PipelinesTableProperties.resetAllowed.fromMap(t.properties)
      }

      val refreshTables = (specifiedRefreshTables ++ fullRefreshNotAllowed).filterNot { t =>
        fullRefreshAllowed.contains(t)
      }

      if (fullRefreshNotAllowed.nonEmpty) {
        logInfo(
          log"Skipping full refresh on some tables because " +
          log"${MDC(LogKeys.PROPERTY_NAME, PipelinesTableProperties.resetAllowed.key)} " +
          log"was set to false. Tables: " +
          log"${MDC(LogKeys.TABLE_NAME, fullRefreshNotAllowed.map(_.identifier))}"
        )
      }

      (fullRefreshAllowed, refreshTables)
    }
    val allRefreshTables = fullRefreshTablesSet ++ refreshTablesSet
    val refreshTableIdentsSet = refreshTablesSet.map(_.identifier)
    val fullRefreshTableIdentsSet = fullRefreshTablesSet.map(_.identifier)
    (allRefreshTables, refreshTableIdentsSet, fullRefreshTableIdentsSet)
  }
}
