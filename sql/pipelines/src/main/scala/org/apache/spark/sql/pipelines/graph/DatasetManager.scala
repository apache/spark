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
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, PersistedView, Resolver}
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connector.catalog.{
  CatalogV2Util,
  Identifier,
  SupportsRowLevelOperations,
  Table => V2Table,
  TableCatalog,
  TableChange,
  TableInfo
}
import org.apache.spark.sql.connector.catalog.CatalogV2Util.v2ColumnsToStructType
import org.apache.spark.sql.connector.expressions.{ClusterByTransform, Expressions, Transform}
import org.apache.spark.sql.execution.command.CreateViewCommand
import org.apache.spark.sql.pipelines.graph.QueryOrigin.ExceptionHelpers
import org.apache.spark.sql.pipelines.util.{
  PipelinesCatalogUtils,
  SchemaInferenceUtils,
  SchemaMergingUtils
}
import org.apache.spark.sql.pipelines.util.SchemaInferenceUtils.diffSchemas
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
    val sessionCaseSensitive = context.spark.sessionState.conf.caseSensitiveAnalysis
    val inferredSchemas = resolvedDataflowGraph.inferSchemas(sessionCaseSensitive)
    val auxiliaryTableSpecs = resolvedDataflowGraph.auxiliaryTableSpecs(inferredSchemas)

    // materialized [[DataflowGraph]] where each table has been materialized and each table
    // has metadata (e.g., normalized table storage path) populated
    val materializedGraph: DataflowGraph = try {
      DataflowGraphTransformer
        .withDataflowGraphTransformer(resolvedDataflowGraph) { transformer =>
          transformer.transformTables { table =>
            if (tablesToMaterialize.keySet.contains(table.identifier)) {
              try {
                val isFullRefresh = tablesToMaterialize(table.identifier).isFullRefresh
                // Load the existing auxiliary table (if any) once here and thread the snapshot into
                // both materializeTable (which uses it for AutoCDC config-drift validation) and
                // materializeAuxiliaryTable (which uses it to decide evolve-vs-create). Nothing
                // between them mutates the auxiliary table, so a single load is safe and avoids a
                // redundant catalog round trip.
                val auxiliaryTableSpecOpt = auxiliaryTableSpecs.get(table.identifier)
                val existingAuxiliaryTable = auxiliaryTableSpecOpt.flatMap { spec =>
                  val (auxCatalog, auxId) =
                    PipelinesCatalogUtils.resolveTableCatalog(context.spark, spec.identifier)
                  loadTableIfExists(auxCatalog, auxId)
                }
                val (tableWithMaterializationMetadata, catalogTableEntity) = materializeTable(
                  resolvedDataflowGraph = resolvedDataflowGraph,
                  table = table,
                  inferredSchemas = inferredSchemas,
                  isFullRefresh = isFullRefresh,
                  auxiliaryTableSpecOpt = auxiliaryTableSpecOpt,
                  existingAuxiliaryTable = existingAuxiliaryTable,
                  context = context
                )
                // Auxiliary tables' lifecycle should follow the table that it is complementary to.
                // If this table has any auxiliary tables, validate the target can host them and
                // materialize/full-refresh them accordingly.
                auxiliaryTableSpecOpt.foreach {
                  auxiliaryTableSpec =>
                    // If this table is an AutoCDC target table, as identified by being
                    // accompanied by an AutoCDC auxiliary table, additionally validate that the
                    // target table supports row level mutations. This is a relevant validation for
                    // the auxiliary table itself too, whose format for AutoCDC is fully derived
                    // from the target table.
                    auxiliaryTableSpec match {
                      case _: AutoCdcAuxiliaryTableSpec =>
                        requireAutoCdcTargetSupportsRowLevelOps(
                          targetTable = table,
                          targetTableCatalogEntity = catalogTableEntity
                        )
                    }

                    materializeAuxiliaryTable(
                      auxiliaryTableSpec = auxiliaryTableSpec,
                      isFullRefresh = isFullRefresh,
                      existingAuxiliaryTable = existingAuxiliaryTable,
                      // The auxiliary schema is derived from its target's, so it evolves under the
                      // target's effective case sensitivity.
                      caseSensitive = effectiveCaseSensitivityFor(
                        resolvedDataflowGraph, table.identifier, context),
                      context = context
                    )
                }
                tableWithMaterializationMetadata
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
   * @param resolvedDataflowGraph The resolved [[DataflowGraph]] used for table metadata.
   * @param table The table to be materialized.
   * @param inferredSchemas The schemas inferred from the resolved graph, keyed by table.
   * @param isFullRefresh Whether this table should be full refreshed or not.
   * @param auxiliaryTableSpecOpt The spec for the auxiliary table (if this table has one)
   * @param existingAuxiliaryTable The already-loaded auxiliary table for this target (if it has one
   *                               and it exists), used for AutoCDC config-drift validation. Loaded
   *                               once by the caller and shared with [[materializeAuxiliaryTable]].
   * @param context The context for the pipeline update.
   * @return The materialized graph [[Table]] (with additional metadata set) paired with the loaded
   *         DSv2 handle of the just created/evolved table.
   */
  private def materializeTable(
      resolvedDataflowGraph: DataflowGraph,
      table: Table,
      inferredSchemas: Map[TableIdentifier, StructType],
      isFullRefresh: Boolean,
      auxiliaryTableSpecOpt: Option[AuxiliaryTableSpec],
      existingAuxiliaryTable: Option[V2Table],
      context: PipelineUpdateContext): (Table, V2Table) = {
    logInfo(log"Materializing metadata for table ${MDC(LogKeys.TABLE_NAME, table.identifier)}.")
    // Get the DSv2 catalog handler and identifier for the table.
    val (catalog, identifier) =
      PipelinesCatalogUtils.resolveTableCatalog(context.spark, table.identifier)

    val outputSchema = table.specifiedSchema.getOrElse(
      inferredSchemas(table.identifier).asNullable
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

    val autoCdcAuxTableSpecOpt = auxiliaryTableSpecOpt.collect {
        case autoCdcSpec: AutoCdcAuxiliaryTableSpec => autoCdcSpec
      }
    val effectiveCaseSensitive = effectiveCaseSensitivityFor(
      resolvedDataflowGraph, table.identifier, context)
    val effectiveResolver = SchemaInferenceUtils.resolverFor(effectiveCaseSensitive)

    // For an incrementally-updated AutoCDC target, validate that the AutoCDC configuration recorded
    // on the auxiliary table has not drifted, BEFORE anything is created or evolved this run. These
    // checks read the auxiliary table, so they run whenever IT exists -- independent of whether the
    // target exists. That matters when a user drops and recreates the target without dropping the
    // internal auxiliary table: the target is then absent (so it is re-created below) but the stale
    // auxiliary table survives, and `materializeAuxiliaryTable`'s additive evolve would otherwise
    // silently overwrite the recorded key/SCD-type/track-history properties with this run's values.
    // Running here turns that into one clear drift error (remedy: full refresh).
    if (isTableIncrementallyUpdated) {
      autoCdcAuxTableSpecOpt.foreach {
        validateNoAutoCdcAuxConfigDrift(_, existingAuxiliaryTable, effectiveResolver)
      }
    }

    // Create the table if absent, otherwise evolve it (schema + properties).
    existingTableOpt match {
      case Some(existingTable) =>
        // The sequencing-type check needs the existing target schema (the type is embedded in the
        // target's `_cdc_metadata`), so it runs here, and BEFORE `evolveTable`: `evolveTable`
        // ALTERs the target (additively) in place, so a check that ran afterwards would leave the
        // target already mutated by a run it then rejects -- and the drift remedy could not undo
        // that. Running first means a rejected run leaves the target untouched, and surfaces the
        // change as an actionable SEQUENCING_TYPE_DRIFT rather than a generic
        // CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE from the schema merge.
        if (isTableIncrementallyUpdated) {
          autoCdcAuxTableSpecOpt.foreach { autoCdcSpec =>
            AutoCdcAuxiliaryTable.validateNoTargetSequencingTypeDrift(
              existingTargetSchema =
                CatalogV2Util.v2ColumnsToStructType(existingTable.columns()),
              targetTableIdentifier = autoCdcSpec.targetTableIdentifier,
              expectedScdType = autoCdcSpec.expectedScdType,
              expectedSequencingType = autoCdcSpec.expectedSequencingType,
              resolver = effectiveResolver
            )
          }
        }
        evolveTable(
          catalog = catalog,
          tableIdentifier = identifier,
          existingTable = existingTable,
          desiredSchema = outputSchema,
          properties = mergedProperties,
          mergeWithExistingSchema = isTableIncrementallyUpdated,
          caseSensitive = effectiveCaseSensitive
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

    val catalogTableEntity = catalog.loadTable(identifier)
    val tableWithMaterializationMetadata =
      table.copy(
        normalizedPath =
          Option(catalogTableEntity.properties().get(TableCatalog.PROP_LOCATION))
      )

    (tableWithMaterializationMetadata, catalogTableEntity)
  }

  /**
   * Validate that the AutoCDC target table is backed by a connector implementing
   * [[SupportsRowLevelOperations]], the DSv2 contract for the MERGE/UPDATE/DELETE-with-rewrite
   * operations the AutoCDC transformation relies on. Reuses the target handle already loaded by
   * [[materializeTable]], so it performs no additional catalog I/O. Only AutoCDC auxiliary specs
   * carry a MERGE-backed target; other auxiliary tables have no such requirement.
   *
   * @param targetTable              the target table graph entity, source of the identifier and
   *                                 declared format used in the error message.
   * @param targetTableCatalogEntity the target table's loaded DSv2 handle.
   */
  private def requireAutoCdcTargetSupportsRowLevelOps(
      targetTable: Table,
      targetTableCatalogEntity: V2Table): Unit = {
    if (!targetTableCatalogEntity.isInstanceOf[SupportsRowLevelOperations]) {
      throw new AnalysisException(
        errorClass = "AUTOCDC_TARGET_DOES_NOT_SUPPORT_MERGE",
        messageParameters = Map(
          "tableName" -> targetTable.identifier.quotedString,
          // Prefer the flow-declared format, falling back to the connector's provider property.
          "format" -> targetTable.format
            .orElse(Option(targetTableCatalogEntity.properties.get(TableCatalog.PROP_PROVIDER)))
            .getOrElse("<unknown>")
        )
      )
    }
  }

  /**
   * Materialize the auxiliary table according to the provided spec.
   *
   * @param auxiliaryTableSpec the spec describing the auxiliary table to create/evolve.
   * @param isFullRefresh whether the owning table is being fully refreshed.
   * @param existingAuxiliaryTable the already-loaded auxiliary table (if it exists), loaded once by
   *                               the caller and shared with the config-drift validation in
   *                               [[materializeTable]] rather than re-loaded here.
   * @param caseSensitive the effective case sensitivity of the flows writing to the auxiliary
   *                      table's TARGET, whose schema the auxiliary schema is derived from.
   * @param context the context for the pipeline update.
   */
  private def materializeAuxiliaryTable(
      auxiliaryTableSpec: AuxiliaryTableSpec,
      isFullRefresh: Boolean,
      existingAuxiliaryTable: Option[V2Table],
      caseSensitive: Boolean,
      context: PipelineUpdateContext): Unit = {
    // Get the DSv2 catalog handler and identifier for the aux table.
    val (catalog, auxiliaryTableIdentifier) =
      PipelinesCatalogUtils.resolveTableCatalog(context.spark, auxiliaryTableSpec.identifier)

    logInfo(
      log"Materializing auxiliary table " +
      log"${MDC(LogKeys.TABLE_NAME, auxiliaryTableSpec.identifier)}."
    )

    if (isFullRefresh) {
      // Intentionally DROP and not TRUNCATE on full refresh. The auxiliary table is an internal
      // table whose identity does not need to be preserved on full refresh, and has metadata
      // (ex. table properties) that should not persist between full refreshes.
      //
      // DROP + CREATE (rather than an atomic REPLACE) because REPLACE is not universally supported
      // by DSv2 catalogs. The non-atomicity is acceptable: a CREATE that fails after the DROP is
      // self-healing on the next run (a full refresh re-enters here; an incremental run recreates
      // via the create path below).
      logInfo(
        log"Dropping and recreating auxiliary table " +
        log"${MDC(LogKeys.TABLE_NAME, auxiliaryTableSpec.identifier)} as part of full refresh."
      )

      // [[dropTable]] is a no-op if the table does not exist.
      catalog.dropTable(auxiliaryTableIdentifier)

      createTable(
        catalog = catalog,
        tableIdentifier = auxiliaryTableIdentifier,
        schema = auxiliaryTableSpec.schema,
        properties = auxiliaryTableSpec.properties,
        transforms = Seq.empty
      )
    } else {
      // Uses the auxiliary-table snapshot loaded by the caller (see materializeDatasets), rather
      // than re-loading it here.
      existingAuxiliaryTable match {
        case Some(existingAuxTable) =>
          // NOTE: AutoCDC configuration-drift validation (key columns, SCD type, sequencing type,
          // track-history columns) intentionally runs in [[materializeTable]] BEFORE the target's
          // schema is evolved, not here -- see `validateNoAutoCdcAuxConfigDrift`. Validating here
          // would be too late: the target has already been ALTERed by the time an aux-owned check
          // could reject the run.
          evolveTable(
            catalog = catalog,
            tableIdentifier = auxiliaryTableIdentifier,
            existingTable = existingAuxTable,
            desiredSchema = auxiliaryTableSpec.schema,
            properties = auxiliaryTableSpec.properties,
            mergeWithExistingSchema = true,
            caseSensitive = caseSensitive
          )
        case None =>
          createTable(
            catalog = catalog,
            tableIdentifier = auxiliaryTableIdentifier,
            schema = auxiliaryTableSpec.schema,
            properties = auxiliaryTableSpec.properties,
            transforms = Seq.empty
          )
      }
    }
  }

  /**
   * Validate that an incrementally-updated AutoCDC flow's configuration has not drifted from what
   * its auxiliary table recorded. Called from [[materializeTable]] before the target and auxiliary
   * tables are created/evolved this run, so a rejected run leaves both untouched.
   *
   * Covers the checks that read the recorded configuration off the existing auxiliary table: key
   * columns, SCD type, and track-history columns. Runs whenever the auxiliary table exists,
   * independent of the target's existence (see the call site). If the auxiliary table does not
   * exist yet (first AutoCDC run), all three are skipped -- there is no recorded configuration to
   * drift from. The sequencing-type check is separate ([[AutoCdcAuxiliaryTable]]
   * `.validateNoTargetSequencingTypeDrift`) because it reads the target schema, not the aux table.
   *
   * @param autoCdcSpec the auxiliary-table spec carrying this run's expected AutoCDC configuration.
   * @param existingAuxiliaryTableOpt the already-loaded auxiliary table (if it exists), shared with
   *                                  the caller and [[materializeAuxiliaryTable]] to avoid a
   *                                  redundant load.
   * @param resolver the effective resolver of the flows writing to the AutoCDC target.
   */
  private def validateNoAutoCdcAuxConfigDrift(
      autoCdcSpec: AutoCdcAuxiliaryTableSpec,
      existingAuxiliaryTableOpt: Option[V2Table],
      resolver: Resolver): Unit = {
    existingAuxiliaryTableOpt.foreach { existingAuxiliaryTable =>
      AutoCdcAuxiliaryTable.validateNoKeyColumnDrift(
        existingAuxiliaryTable = existingAuxiliaryTable,
        targetTableIdentifier = autoCdcSpec.targetTableIdentifier,
        expectedKeyFields = autoCdcSpec.expectedKeyFields,
        resolver = resolver
      )
      AutoCdcAuxiliaryTable.validateNoScdTypeDrift(
        existingAuxiliaryTable = existingAuxiliaryTable,
        targetTableIdentifier = autoCdcSpec.targetTableIdentifier,
        expectedScdType = autoCdcSpec.expectedScdType
      )
      AutoCdcAuxiliaryTable.validateNoTrackHistoryDrift(
        existingAuxiliaryTable = existingAuxiliaryTable,
        targetTableIdentifier = autoCdcSpec.targetTableIdentifier,
        expectedTrackHistoryColumnNames = autoCdcSpec.expectedTrackHistoryColumnNames,
        resolver = resolver
      )
    }
  }

  /**
   * The effective `spark.sql.caseSensitive` for schema evolution of `tableIdentifier`, read from
   * the flows writing to it rather than from the session, so evolution stays consistent with the
   * flows whose schemas it is evolving (a pipeline-level `SET` never reaches the session). Fails if
   * those flows disagree; see [[SchemaInferenceUtils.effectiveCaseSensitivity]].
   */
  private def effectiveCaseSensitivityFor(
      resolvedDataflowGraph: DataflowGraph,
      tableIdentifier: TableIdentifier,
      context: PipelineUpdateContext): Boolean = {
    SchemaInferenceUtils.effectiveCaseSensitivity(
      tableIdentifier = tableIdentifier,
      flows = resolvedDataflowGraph.flowsTo.getOrElse(tableIdentifier, Seq.empty),
      sessionCaseSensitive = context.spark.sessionState.conf.caseSensitiveAnalysis
    )
  }

  /**
   * Loads the table at `identifier` from `catalog`, or `None` if it does not exist. A single
   * `loadTable` guarded by a `NoSuchTableException` catch, rather than a `tableExists` +
   * `loadTable` pair: one catalog round trip instead of two, and no window where the table can
   * disappear between the existence check and the load. A missing *namespace* is not treated as
   * "table absent" -- it surfaces as its own `NoSuchDatabaseException` rather than being swallowed.
   */
  private def loadTableIfExists(
      catalog: TableCatalog,
      identifier: Identifier): Option[V2Table] = {
    try Some(catalog.loadTable(identifier))
    catch { case _: NoSuchTableException => None }
  }

  /**
   * Creates the table at `identifier` with the given schema, properties, and partition/cluster
   * transforms. Used both for graph datasets and for internal auxiliary tables when no table yet
   * exists at the identifier.
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
   * Evolves the already-existing `existingTable` at `identifier` in place by diffing its schema and
   * properties, skipping the catalog `alterTable` entirely when nothing actually changes.
   * Partitioning/clustering cannot change in place, so no transforms are accepted here. Used both
   * for graph datasets and for internal auxiliary tables.
   *
   * @param existingTable           the currently materialized table.
   * @param desiredSchema           the schema the table should have as computed in the current
   *                                execution (for graph datasets, the user-specified or inferred
   *                                schema; for auxiliary tables, the derived schema). This is the
   *                                "incoming" side and may differ from `existingTable`'s recorded
   *                                schema due to schema evolution across runs.
   * @param properties              the declared table properties to (re)set on the table. Note
   *                                that properties absent here are NOT removed from the table (see
   *                                the TODO in the body).
   * @param mergeWithExistingSchema whether the effective schema is the merge of the existing and
   *                                desired schemas (additive evolution) rather than the desired
   *                                schema as-is.
   * @param caseSensitive           whether the additive schema merge treats field names differing
   *                                only in case as distinct columns. Callers should pass the
   *                                effective `spark.sql.caseSensitive` used to resolve the schema
   *                                being evolved. When `false`, an incoming column differing from
   *                                an existing one only in case is folded onto it rather than
   *                                added as a duplicate. Only affects the merge (i.e.
   *                                `mergeWithExistingSchema = true`); the subsequent diff always
   *                                keys columns on their exact names, so a case-only rename on a
   *                                non-merging path stays an explicit drop-then-add.
   */
  private def evolveTable(
      catalog: TableCatalog,
      tableIdentifier: Identifier,
      existingTable: V2Table,
      desiredSchema: StructType,
      properties: Map[String, String],
      mergeWithExistingSchema: Boolean,
      caseSensitive: Boolean): Unit = {
    val currentSchema = v2ColumnsToStructType(existingTable.columns())
    val targetSchema = if (mergeWithExistingSchema) {
      SchemaMergingUtils.mergeSchemas(currentSchema, desiredSchema, caseSensitive)
    } else {
      desiredSchema
    }
    // `diffSchemas` keys column identity on exact field names. On the incremental path the merge
    // above has already folded a case-only-differing incoming field onto the persisted one. On the
    // non-merging paths (materialized views, full refresh), `targetSchema` is the declared schema
    // as-is, where exact-name matching keeps a case-only rename visible as a schema change.
    val columnChanges = diffSchemas(currentSchema, targetSchema)

    val existingProperties = existingTable.properties()

    // TODO (SPARK-57670): Property removal is intentionally not handled here: a property dropped
    // from the table definition between runs is left in place rather than actually removed from the
    // corresponding catalog table entry. Removing it reliably is hard because we cannot distinguish
    // a user-declared property the user dropped from a catalog/engine-managed property (e.g. the
    // non-reserved `clusteringColumns`, or arbitrary catalog-internal keys) that must never be
    // removed, and there is no record of which keys the pipeline previously set.
    val propertiesToSet = properties.collect {
      case (k, v) if !Option(existingProperties.get(k)).contains(v) =>
        TableChange.setProperty(k, v)
    }

    val allTableChanges = columnChanges ++ propertiesToSet

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
