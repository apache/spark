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

package org.apache.spark.sql.pipelines.utils

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{LocalTempView, PersistedView => PersistedViewType, UnresolvedRelation, ViewType}
import org.apache.spark.sql.classic.{DataFrame, SparkSession}
import org.apache.spark.sql.pipelines.graph.{
  DataflowGraph,
  FlowAnalysis,
  FlowFunction,
  GraphIdentifierManager,
  GraphRegistrationContext,
  PersistedView,
  QueryContext,
  QueryOrigin,
  Table,
  TemporaryView,
  UnresolvedFlow
}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A test class to simplify the creation of pipelines and datasets for unit testing.
 */
class TestGraphRegistrationContext(
    val spark: SparkSession,
    val sqlConf: Map[String, String] = Map.empty)
    extends GraphRegistrationContext(
      defaultCatalog = TestGraphRegistrationContext.DEFAULT_CATALOG,
      defaultDatabase = TestGraphRegistrationContext.DEFAULT_DATABASE,
      defaultSqlConf = sqlConf
    ) {

  // scalastyle:off
  // Disable scalastyle to ignore argument count.
  /** Registers a streaming table in this [[TestGraphRegistrationContext]] */
  def registerTable(
      name: String,
      query: Option[FlowFunction] = None,
      sqlConf: Map[String, String] = Map.empty,
      comment: Option[String] = None,
      specifiedSchema: Option[StructType] = None,
      partitionCols: Option[Seq[String]] = None,
      properties: Map[String, String] = Map.empty,
      baseOrigin: QueryOrigin = QueryOrigin.empty,
      format: Option[String] = None,
      catalog: Option[String] = None,
      database: Option[String] = None
  ): Unit = registerTable(
    name,
    query,
    sqlConf,
    comment,
    specifiedSchema,
    partitionCols,
    properties,
    baseOrigin,
    format,
    catalog,
    database,
    isStreamingTable = true
  )
  // scalastyle:on

  // scalastyle:off
  // Disable scalastyle to ignore argument count.
  /** Registers a materialized view in this [[TestGraphRegistrationContext]] */
  def registerMaterializedView(
      name: String,
      // Unlike for streaming tables, a materialized view MUST be defined alongside a query
      // function.
      query: FlowFunction,
      sqlConf: Map[String, String] = Map.empty,
      comment: Option[String] = None,
      specifiedSchema: Option[StructType] = None,
      partitionCols: Option[Seq[String]] = None,
      properties: Map[String, String] = Map.empty,
      baseOrigin: QueryOrigin = QueryOrigin.empty,
      format: Option[String] = None,
      catalog: Option[String] = None,
      database: Option[String] = None
): Unit = registerTable(
    name,
    Option(query),
    sqlConf,
    comment,
    specifiedSchema,
    partitionCols,
    properties,
    baseOrigin,
    format,
    catalog,
    database,
    isStreamingTable = false
  )
  // scalastyle:on

  // scalastyle:off
  // Disable scalastyle to ignore argument count.
  private def registerTable(
      name: String,
      query: Option[FlowFunction],
      sqlConf: Map[String, String],
      comment: Option[String],
      specifiedSchema: Option[StructType],
      partitionCols: Option[Seq[String]],
      properties: Map[String, String],
      baseOrigin: QueryOrigin,
      format: Option[String],
      catalog: Option[String],
      database: Option[String],
      isStreamingTable: Boolean
  ): Unit = {
    // scalastyle:on
    val tableIdentifier = GraphIdentifierManager.parseTableIdentifier(name, spark)
    registerTable(
      Table(
        identifier = GraphIdentifierManager.parseTableIdentifier(name, spark),
        comment = comment,
        specifiedSchema = specifiedSchema,
        partitionCols = partitionCols,
        properties = properties,
        baseOrigin = baseOrigin,
        format = format.orElse(Some("parquet")),
        normalizedPath = None,
        isStreamingTable = isStreamingTable
      )
    )

    if (query.isDefined) {
      registerFlow(
        new UnresolvedFlow(
          identifier = tableIdentifier,
          destinationIdentifier = tableIdentifier,
          func = query.get,
          queryContext = QueryContext(
            currentCatalog = catalog.orElse(Some(defaultCatalog)),
            currentDatabase = database.orElse(Some(defaultDatabase))
          ),
          sqlConf = sqlConf,
          once = false,
          origin = baseOrigin
        )
      )
    }
  }

  def registerPersistedView(
      name: String,
      query: FlowFunction,
      sqlConf: Map[String, String] = Map.empty,
      comment: Option[String] = None,
      origin: QueryOrigin = QueryOrigin.empty,
      catalog: Option[String] = None,
      database: Option[String] = None): Unit = {
    registerView(
      name = name,
      query = query,
      sqlConf = sqlConf,
      comment = comment,
      origin = origin,
      viewType = PersistedViewType,
      catalog = catalog,
      database = database)
  }

  def registerView(
      name: String,
      query: FlowFunction,
      sqlConf: Map[String, String] = Map.empty,
      comment: Option[String] = None,
      origin: QueryOrigin = QueryOrigin.empty,
      viewType: ViewType = LocalTempView,
      catalog: Option[String] = None,
      database: Option[String] = None
  ): Unit = {

    val viewIdentifier = GraphIdentifierManager
      .parseAndValidateTemporaryViewIdentifier(rawViewIdentifier = TableIdentifier(name))

    registerView(
      viewType match {
        case LocalTempView =>
          TemporaryView(
            identifier = viewIdentifier,
            comment = comment,
            origin = origin,
            properties = Map.empty
          )
        case _ =>
          PersistedView(
            identifier = viewIdentifier,
            comment = comment,
            origin = origin,
            properties = Map.empty
          )
      }
    )

    registerFlow(
      new UnresolvedFlow(
        identifier = viewIdentifier,
        destinationIdentifier = viewIdentifier,
        func = query,
        queryContext = QueryContext(
          currentCatalog = catalog.orElse(Some(defaultCatalog)),
          currentDatabase = database.orElse(Some(defaultDatabase))
        ),
        sqlConf = sqlConf,
        once = false,
        origin = origin
      )
    )
  }

  def registerFlow(
      destinationName: String,
      name: String,
      query: FlowFunction,
      once: Boolean = false,
      catalog: Option[String] = None,
      database: Option[String] = None
  ): Unit = {
    val flowIdentifier = GraphIdentifierManager.parseTableIdentifier(name, spark)
    val flowDestinationIdentifier =
      GraphIdentifierManager.parseTableIdentifier(destinationName, spark)

    registerFlow(
      new UnresolvedFlow(
        identifier = flowIdentifier,
        destinationIdentifier = flowDestinationIdentifier,
        func = query,
        queryContext = QueryContext(
          currentCatalog = catalog.orElse(Some(defaultCatalog)),
          currentDatabase = database.orElse(Some(defaultDatabase))
        ),
        sqlConf = Map.empty,
        once = once,
        origin = QueryOrigin()
      )
    )
  }

  /**
   * Creates a flow function from a logical plan that reads from a table with the given name.
   */
  def readFlowFunc(name: String): FlowFunction = {
    FlowAnalysis.createFlowFunctionFromLogicalPlan(UnresolvedRelation(TableIdentifier(name)))
  }

  /**
   * Creates a flow function from a logical plan that reads a stream from a table with the given
   * name.
   */
  def readStreamFlowFunc(name: String): FlowFunction = {
    FlowAnalysis.createFlowFunctionFromLogicalPlan(
      UnresolvedRelation(
        TableIdentifier(name),
        extraOptions = CaseInsensitiveStringMap.empty(),
        isStreaming = true
      )
    )
  }

  /**
   * Creates a flow function from a logical plan parsed from the given SQL text.
   */
  def sqlFlowFunc(spark: SparkSession, sql: String): FlowFunction = {
    FlowAnalysis.createFlowFunctionFromLogicalPlan(spark.sessionState.sqlParser.parsePlan(sql))
  }

  /**
   * Creates a flow function from a logical plan from the given DataFrame. This is meant for
   * DataFrames that don't read from tables within the pipeline.
   */
  def dfFlowFunc(df: DataFrame): FlowFunction = {
    FlowAnalysis.createFlowFunctionFromLogicalPlan(df.logicalPlan)
  }

  /**
   * Generates a dataflow graph from this pipeline definition and resolves it.
   * @return
   */
  def resolveToDataflowGraph(): DataflowGraph = toDataflowGraph.resolve()
}

object TestGraphRegistrationContext {
  val DEFAULT_CATALOG = "spark_catalog"
  val DEFAULT_DATABASE = "test_db"
}
