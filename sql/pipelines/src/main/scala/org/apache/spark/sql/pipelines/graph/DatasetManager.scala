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
import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.{
  CatalogV2Util,
  Identifier,
  TableCatalog,
  TableChange,
  TableInfo
}
import org.apache.spark.sql.connector.catalog.CatalogV2Util.v2ColumnsToStructType
import org.apache.spark.sql.connector.expressions.Expressions
import org.apache.spark.sql.pipelines.graph.QueryOrigin.ExceptionHelpers
import org.apache.spark.sql.pipelines.util.SchemaInferenceUtils.diffSchemas
import org.apache.spark.sql.pipelines.util.SchemaMergingUtils

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
      DatasetManager.constructFullRefreshSet(resolvedDataflowGraph.tables, context)
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
        // TODO: Publish persisted views to the metastore.
        }
        .getDataflowGraph
    } catch {
      case e: SparkException if e.getCause != null => throw e.getCause
    }

    materializedGraph
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
      context: PipelineUpdateContext
  ): Table = {
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

    val existingTableOpt = if (catalog.tableExists(identifier)) {
      Some(catalog.loadTable(identifier))
    } else {
      None
    }

    // Error if partitioning doesn't match
    if (existingTableOpt.isDefined) {
      val existingPartitioning = existingTableOpt.get.partitioning().toSeq
      if (existingPartitioning != partitioning) {
        throw new AnalysisException(
          errorClass = "CANNOT_UPDATE_PARTITION_COLUMNS",
          messageParameters = Map(
            "existingPartitionColumns" -> existingPartitioning.mkString(", "),
            "requestedPartitionColumns" -> partitioning.mkString(", ")
          )
        )
      }
    }

    // Wipe the data if we need to
    if ((isFullRefresh || !table.isStreamingTable) && existingTableOpt.isDefined) {
      context.spark.sql(s"TRUNCATE TABLE ${table.identifier.quotedString}")
    }

    // Alter the table if we need to
    if (existingTableOpt.isDefined) {
      val existingSchema = v2ColumnsToStructType(existingTableOpt.get.columns())

      val targetSchema = if (table.isStreamingTable && !isFullRefresh) {
        SchemaMergingUtils.mergeSchemas(existingSchema, outputSchema)
      } else {
        outputSchema
      }

      val columnChanges = diffSchemas(existingSchema, targetSchema)
      val setProperties = mergedProperties.map { case (k, v) => TableChange.setProperty(k, v) }
      catalog.alterTable(identifier, (columnChanges ++ setProperties).toArray: _*)
    }

    // Create the table if we need to
    if (existingTableOpt.isEmpty) {
      catalog.createTable(
        identifier,
        new TableInfo.Builder()
          .withProperties(mergedProperties.asJava)
          .withColumns(CatalogV2Util.structTypeToV2Columns(outputSchema))
          .withPartitions(partitioning.toArray)
          .build()
      )
    }

    table.copy(
      normalizedPath = Option(
        catalog.loadTable(identifier).properties().get(TableCatalog.PROP_LOCATION)
      )
    )
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
