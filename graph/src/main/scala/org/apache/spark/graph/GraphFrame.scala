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

package org.apache.spark.graph

import org.apache.spark.annotation.Experimental
import org.apache.spark.graph.GraphErrors.{EDGE_PROPERTIES, EDGES, VERTEX_PROPERTIES, VERTICES}
import org.apache.spark.sql.{AnalysisException, Column, DataFrame}
import org.apache.spark.sql.catalyst.analysis.{caseInsensitiveResolution, caseSensitiveResolution, Resolver}
import org.apache.spark.sql.catalyst.util.QuotingUtils.quoteIdentifier
import org.apache.spark.sql.functions.{array, coalesce, col, count, countDistinct, explode, expr, lit, struct}
import org.apache.spark.sql.graph.GraphConf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ByteType, DataType, IntegerType, LongType, ShortType, StringType}
import org.apache.spark.storage.StorageLevel

/**
 * :: Experimental :: A representation of a graph by two DataFrames: one for vertices and one for
 * edges.
 *
 * A graph is a pair \( G = (V, E) \), where \( V \) is the set of vertices and \( E \) is the
 * list of edges. Each edge is a pair of vertices, so \( E \) is defined over \( V \times V \).
 * `GraphFrame` stores \( V \) in the `vertices` DataFrame and \( E \) in the `edges` DataFrame.
 *
 * The `GraphFrame` model assumes that the graph is valid: vertex IDs are unique and not NULL, and
 * every edge goes from an existing vertex ID to an existing vertex ID. This is never checked
 * implicitly: an invalid graph silently produces incorrect results instead of an exception. Call
 * `validate()` to check that a graph is valid. `validate()` runs several Spark jobs, so it is not
 * free.
 *
 * The `GraphFrame` model also assumes that the degree of every vertex, i.e. the number of its
 * adjacent edges, is less than `Integer.MAX_VALUE`: degree metrics return INT values. This is not
 * checked either. A larger degree fails with an overflow error in ANSI mode and silently
 * overflows otherwise. GraphFrames also use internally integer type for counting or indexing
 * neighbors. Correctness of algorithms is not guaranteed on graphs with max degree gretater than
 * 2 billions. Handling graphs with billion-scale hubs require another kind of infrastructure.
 *
 * Required vertex columns:
 *   - "id", the vertex ID. See `GraphFrame.ID`
 *
 * Required edge columns:
 *   - "src", the source vertex ID. See `GraphFrame.SRC`
 *   - "dst", the destination vertex ID. See `GraphFrame.DST`
 *
 * Optional edge columns:
 *   - "weight", the edge weight. See `GraphFrame.WEIGHT`
 *
 * All other columns are vertex or edge properties. Avoid using the reserved column names for
 * properties: this is not checked, but such columns may conflict with columns produced by graph
 * operations.
 *
 * The "id", "src" and "dst" columns must have the same data type, which is one of:
 *   - `ByteType` (BYTE, TINYINT)
 *   - `ShortType` (SHORT, SMALLINT)
 *   - `IntegerType` (INT, INTEGER)
 *   - `LongType` (LONG, BIGINT)
 *   - `StringType` (STRING), with the default UTF8_BINARY collation only
 *
 * No other data type can be used for vertex IDs. `StringType` IDs are fully supported, but prefer
 * `LongType` when possible: it uses significantly less memory and makes most algorithms from the
 * standard library faster. Some algorithms support only integral IDs and convert `StringType` IDs
 * implicitly, which can significantly affect performance.
 *
 * `GraphFrame` is a directed graph by construction. To represent an undirected graph, store every
 * edge in both directions: (src, dst) and (dst, src). The method `asUndirected()` does this for
 * an existing `GraphFrame`. Keep in mind that not all algorithms support both directed and
 * undirected graphs; this is controlled at the algorithm level by a corresponding argument.
 *
 * @groupname structure Basic graph structure information
 * @groupname centrality Centrality metrics
 * @groupname conversions Conversion operations
 * @groupname subgraph Subgraph selection
 * @groupname transformations Graph transformations
 * @groupname utils Graph utilities
 *
 * @since 4.5.0
 */
@Experimental
class GraphFrame private (
    @transient private val _vertices: DataFrame,
    @transient private val _edges: DataFrame)
    extends Serializable {

  import GraphFrame._

  protected def this() = this(null, null)

  /**
   * The vertices `DataFrame`: the "id" column and the vertex properties.
   *
   * @return
   *   the vertices `DataFrame`
   *
   * @group structure
   * @since 4.5.0
   */
  def vertices: DataFrame = {
    if (_vertices == null) {
      // The fields are transient: they are null after deserialization, e.g. inside a closure.
      throw GraphErrors.cannotInvokeInTransformationsError()
    }
    _vertices
  }

  /**
   * The edges `DataFrame`: the "src" and "dst" columns and the edge properties.
   *
   * @return
   *   the edges `DataFrame`
   *
   * @group structure
   * @since 4.5.0
   */
  def edges: DataFrame = {
    if (_edges == null) {
      // The fields are transient: they are null after deserialization, e.g. inside a closure.
      throw GraphErrors.cannotInvokeInTransformationsError()
    }
    _edges
  }

  /**
   * The column names of the vertices `DataFrame`, including "id".
   *
   * @return
   *   the vertex column names, in order
   *
   * @group structure
   * @since 4.5.0
   */
  def vertexColumns: Array[String] = vertices.columns

  /**
   * The column names of the edges `DataFrame`, including "src" and "dst".
   *
   * @return
   *   the edge column names, in order
   *
   * @group structure
   * @since 4.5.0
   */
  def edgeColumns: Array[String] = edges.columns

  /**
   * The out-degree of each vertex in the graph, returned as a DataFrame with two columns:
   *   - `id` the ID of the vertex
   *   - "outDegree" (integer) storing the out-degree of the vertex
   *
   * Note that vertices with 0 out-edges are not returned in the result.
   *
   * @group centrality
   * @since 4.5.0
   */
  def outDegrees: DataFrame =
    edges.groupBy(col(SRC).as(ID)).agg(count(lit(1)).cast(IntegerType).as("outDegree"))

  /**
   * The in-degree of each vertex in the graph, returned as a DataFrame with two columns:
   *   - `id` the ID of the vertex
   *   - "inDegree" (integer) storing the in-degree of the vertex
   *
   * Note that vertices with 0 in-edges are not returned in the result.
   *
   * @group centrality
   * @since 4.5.0
   */
  def inDegrees: DataFrame =
    edges.groupBy(col(DST).as(ID)).agg(count(lit(1)).cast(IntegerType).as("inDegree"))

  /**
   * The degree of each vertex in the graph, returned as a DataFrame with two columns:
   *   - `id` the ID of the vertex
   *   - "degree" (integer) the degree of the vertex
   *
   * Note that vertices with 0 edges are not returned in the result.
   *
   * @group centrality
   * @since 4.5.0
   */
  def degrees: DataFrame =
    edges
      .select(explode(array(col(SRC), col(DST))).as(ID))
      .groupBy(ID)
      .agg(count(lit(1)).cast(IntegerType).as("degree"))

  /**
   * The out-degree of each vertex per edge type, returned as a DataFrame with two columns:
   *   - `id` the ID of the vertex
   *   - "outDegrees" a struct with a field for each edge type, storing the out-degree count
   *
   * Edge types are discovered automatically.
   *
   * @param edgeTypeCol
   *   Name of the column in edges DataFrame that contains edge types
   *
   * @group centrality
   * @since 4.5.0
   */
  @throws[AnalysisException]
  def typeOutDegree(edgeTypeCol: String): DataFrame =
    typeDegrees(col(SRC), edgeTypeCol, None, "outDegrees")

  /**
   * The out-degree of each vertex per edge type, returned as a DataFrame with two columns:
   *   - `id` the ID of the vertex
   *   - "outDegrees" a struct with a field for each edge type, storing the out-degree count
   *
   * @param edgeTypeCol
   *   Name of the column in edges DataFrame that contains edge types
   * @param edgeTypes
   *   Sequence of edge type values.
   *
   * @group centrality
   * @since 4.5.0
   */
  @throws[AnalysisException]
  def typeOutDegree(edgeTypeCol: String, edgeTypes: Seq[Any]): DataFrame =
    typeDegrees(col(SRC), edgeTypeCol, Some(edgeTypes), "outDegrees")

  /**
   * The in-degree of each vertex per edge type, returned as a DataFrame with two columns:
   *   - `id` the ID of the vertex
   *   - "inDegrees" a struct with a field for each edge type, storing the in-degree count
   *
   * Edge types are discovered automatically.
   *
   * @param edgeTypeCol
   *   Name of the column in edges DataFrame that contains edge types
   *
   * @group centrality
   * @since 4.5.0
   */
  @throws[AnalysisException]
  def typeInDegree(edgeTypeCol: String): DataFrame =
    typeDegrees(col(DST), edgeTypeCol, None, "inDegrees")

  /**
   * The in-degree of each vertex per edge type, returned as a DataFrame with two columns:
   *   - `id` the ID of the vertex
   *   - "inDegrees" a struct with a field for each edge type, storing the in-degree count
   *
   * @param edgeTypeCol
   *   Name of the column in edges DataFrame that contains edge types
   * @param edgeTypes
   *   Sequence of edge type values.
   *
   * @group centrality
   * @since 4.5.0
   */
  @throws[AnalysisException]
  def typeInDegree(edgeTypeCol: String, edgeTypes: Seq[Any]): DataFrame =
    typeDegrees(col(DST), edgeTypeCol, Some(edgeTypes), "inDegrees")

  /**
   * The total degree of each vertex per edge type (both in and out), returned as a DataFrame with
   * two columns:
   *   - `id` the ID of the vertex
   *   - "degrees" a struct with a field for each edge type, storing the total degree count
   *
   * Edge types are discovered automatically.
   *
   * @param edgeTypeCol
   *   Name of the column in edges DataFrame that contains edge types
   *
   * @group centrality
   * @since 4.5.0
   */
  @throws[AnalysisException]
  def typeDegree(edgeTypeCol: String): DataFrame =
    typeDegrees(explode(array(col(SRC), col(DST))), edgeTypeCol, None, "degrees")

  /**
   * The total degree of each vertex per edge type (both in and out), returned as a DataFrame with
   * two columns:
   *   - `id` the ID of the vertex
   *   - "degrees" a struct with a field for each edge type, storing the total degree count
   *
   * @param edgeTypeCol
   *   Name of the column in edges DataFrame that contains edge types
   * @param edgeTypes
   *   Sequence of edge type values.
   *
   * @group centrality
   * @since 4.5.0
   */
  @throws[AnalysisException]
  def typeDegree(edgeTypeCol: String, edgeTypes: Seq[Any]): DataFrame =
    typeDegrees(explode(array(col(SRC), col(DST))), edgeTypeCol, Some(edgeTypes), "degrees")

  private def typeDegrees(
      endpoint: Column,
      edgeTypeCol: String,
      edgeTypes: Option[Seq[Any]],
      outputCol: String): DataFrame = {
    val grouped = edges
      .select(endpoint.as(ID), col(edgeTypeCol).as(EDGE_TYPE))
      .groupBy(ID)
    val pivoted = edgeTypes match {
      case Some(types) => grouped.pivot(EDGE_TYPE, types)
      case None => grouped.pivot(EDGE_TYPE)
    }
    val counts = pivoted.agg(count(lit(1)))
    // The pivot column names are the edge type values, which can contain special characters or
    // be equal to "id". Rename the columns by position to refer to them safely.
    val typeNames = counts.columns.tail.toSeq
    val tmpNames = typeNames.indices.map(i => s"_$i")
    val typeCounts = typeNames.zip(tmpNames).map { case (typeName, tmpName) =>
      coalesce(col(tmpName), lit(0)).cast(IntegerType).as(typeName)
    }
    counts
      .toDF(ID +: tmpNames: _*)
      .select(col(ID), struct(typeCounts: _*).as(outputCol))
  }

  /**
   * Filters vertices by a condition and drops the edges adjacent to the removed vertices.
   *
   * The result is lazy: every action on it, including running an algorithm, re-applies the filter
   * and two LEFT SEMI joins on the edges. Consider persisting the result if you use it more than
   * once.
   *
   * @param condition
   *   a BOOLEAN column. Only the vertices that satisfy the condition, and the edges between them,
   *   are kept.
   * @return
   *   a new (lazy) `GraphFrame`
   *
   * @group subgraph
   * @since 4.5.0
   */
  def filterVertices(condition: Column): GraphFrame = {
    val vv = vertices.filter(condition)
    val ee = edges
      .join(vv, vv(ID) === edges(SRC), "left_semi")
      .join(vv, vv(ID) === edges(DST), "left_semi")
    GraphFrame(vv, ee)
  }

  /**
   * Filters vertices by a SQL condition and drops the edges adjacent to the removed vertices.
   *
   * The result is lazy: every action on it, including running an algorithm, re-applies the filter
   * and two LEFT SEMI joins on the edges. Consider persisting the result if you use it more than
   * once.
   *
   * @param conditionExpr
   *   a SQL expression that evaluates to BOOLEAN, e.g. "age > 18". Only the vertices that satisfy
   *   the condition, and the edges between them, are kept.
   * @return
   *   a new (lazy) `GraphFrame`
   *
   * @group subgraph
   * @since 4.5.0
   */
  def filterVertices(conditionExpr: String): GraphFrame = filterVertices(expr(conditionExpr))

  /**
   * Filters edges by a condition. All vertices are kept, so the result can contain isolated
   * vertices; use `dropIsolatedVertices()` to remove them.
   *
   * @param condition
   *   a BOOLEAN column. Only the edges that satisfy the condition are kept.
   * @return
   *   a new (lazy) `GraphFrame`
   *
   * @group subgraph
   * @since 4.5.0
   */
  def filterEdges(condition: Column): GraphFrame = GraphFrame(vertices, edges.filter(condition))

  /**
   * Filters edges by a SQL condition. All vertices are kept, so the result can contain isolated
   * vertices; use `dropIsolatedVertices()` to remove them.
   *
   * @param conditionExpr
   *   a SQL expression that evaluates to BOOLEAN, e.g. "weight > 0.5". Only the edges that
   *   satisfy the condition are kept.
   * @return
   *   a new (lazy) `GraphFrame`
   *
   * @group subgraph
   * @since 4.5.0
   */
  def filterEdges(conditionExpr: String): GraphFrame = filterEdges(expr(conditionExpr))

  /**
   * Drops isolated vertices. A vertex is isolated if it has no adjacent edges, neither incoming
   * nor outgoing.
   *
   * The result is lazy: every action on it re-runs a LEFT SEMI join of the vertices with all edge
   * endpoints. Consider persisting the result if you use it more than once.
   *
   * @return
   *   a new (lazy) `GraphFrame` with the same edges
   *
   * @group subgraph
   * @since 4.5.0
   */
  def dropIsolatedVertices(): GraphFrame = {
    val edgeVertexIds = edges.select(explode(array(col(SRC), col(DST))).as(ID))
    GraphFrame(vertices.join(edgeVertexIds, Seq(ID), "left_semi"), edges)
  }

  /**
   * Converts the graph to an undirected one by adding a reversed copy of every edge: for each
   * edge (src, dst), the result contains both (src, dst) and (dst, src) with the same properties.
   *
   * The result has twice as many edges. Self-loops and edges that already exist in both
   * directions are duplicated as well; deduplicate the edges first if that matters for your
   * algorithm. The "src" and "dst" columns come first in the result. Vertices are unchanged.
   *
   * @return
   *   a new (lazy) `GraphFrame`
   *
   * @group transformations
   * @since 4.5.0
   */
  def asUndirected(): GraphFrame = {
    val attributes = edges.columns
      .filter(c => c != SRC && c != DST)
      .map(c => col(quoteIdentifier(c)))
      .toSeq
    val forward = edges.select(Seq(col(SRC), col(DST)) ++ attributes: _*)
    val backward = edges.select(Seq(col(DST).as(SRC), col(SRC).as(DST)) ++ attributes: _*)
    GraphFrame(vertices, forward.union(backward))
  }

  /**
   * Adds properties to the vertices with a LEFT join on the "id" column. Vertices without
   * matching properties get NULL values, and rows of `properties` without a matching vertex are
   * ignored. Edges are unchanged.
   *
   * `properties` is checked before the join: it must contain the "id" column exactly once, with
   * the same data type as the vertex IDs, and no other column that already exists in the vertices
   * `DataFrame`. Column names are compared according to `spark.sql.caseSensitive`.
   *
   * Warning! Duplicate IDs in `properties` duplicate the matching vertices and make the graph
   * invalid. This is not checked: call `validate()` if needed.
   *
   * @param properties
   *   a `DataFrame` with the "id" column and the vertex properties to add
   * @return
   *   a new (lazy) `GraphFrame`
   * @throws org.apache.spark.sql.AnalysisException
   *   if `properties` does not contain the "id" column, contains it more than once, has a
   *   different "id" type, or has a column that already exists in the vertices
   * @throws org.apache.spark.SparkIllegalArgumentException
   *   if `properties` is null
   *
   * @group transformations
   * @since 4.5.0
   */
  @throws[AnalysisException]
  def joinVertices(properties: DataFrame): GraphFrame = {
    checkProperties(properties, Seq(ID), vertices, VERTEX_PROPERTIES)
    GraphFrame(vertices.join(properties, Seq(ID), "left"), edges)
  }

  /**
   * Adds properties to the edges with a LEFT join on the "src" and "dst" columns. Edges without
   * matching properties get NULL values, and rows of `properties` without a matching edge are
   * ignored. If several edges connect the same (src, dst) pair, each of them gets the properties.
   * Vertices are unchanged.
   *
   * Warning! Duplicate (src, dst) pairs in `properties` duplicate the matching edges. This is not
   * checked and the graph with parallel edges is considered as "valid".
   *
   * @param properties
   *   a `DataFrame` with the "src" and "dst" columns and the edge properties to add
   * @return
   *   a new (lazy) `GraphFrame`
   * @throws org.apache.spark.sql.AnalysisException
   *   if `properties` does not contain the "src" or "dst" column, contains one of them more than
   *   once, has a different "src" or "dst" type, or has a column that already exists in the edges
   * @throws org.apache.spark.SparkIllegalArgumentException
   *   if `properties` is null
   *
   * @group transformations
   * @since 4.5.0
   */
  @throws[AnalysisException]
  def joinEdges(properties: DataFrame): GraphFrame = {
    checkProperties(properties, Seq(SRC, DST), edges, EDGE_PROPERTIES)
    GraphFrame(vertices, edges.join(properties, Seq(SRC, DST), "left"))
  }

  /**
   * A string representation of the graph schema, with "id" first among the vertex columns and
   * "src" and "dst" first among the edge columns. It does not run any Spark job.
   *
   * @return
   *   a string in the form "GraphFrame(v:[id: bigint, ...], e:[src: bigint, dst: bigint, ...])"
   *
   * @group utils
   * @since 4.5.0
   */
  override def toString: String = {
    val vCols = (ID +: vertices.columns.filter(_ != ID).toSeq)
      .map(c => col(quoteIdentifier(c)))
    val eCols = (SRC +: DST +: edges.columns.filter(c => c != SRC && c != DST).toSeq)
      .map(c => col(quoteIdentifier(c)))
    val v = vertices.select(vCols: _*).toString
    val e = edges.select(eCols: _*).toString
    "GraphFrame(v:" + v + ", e:" + e + ")"
  }

  /**
   * Checks that the graph is valid: vertex IDs are unique and not NULL, and the "src" and "dst"
   * of every edge are not NULL and are existing vertex IDs. Equivalent to
   * `validate(true, level)`, where `level` is the "spark.graph.storageLevel" configuration or, if
   * it is not set, the "spark.sql.defaultCacheStorageLevel" configuration.
   *
   * This runs several Spark jobs over the vertices and the edges, so it can be expensive on large
   * graphs. See `validate(checkVertices, intermediateStorageLevel)` for the cost of each check.
   *
   * @throws org.apache.spark.SparkRuntimeException
   *   if the graph is invalid
   *
   * @group utils
   * @since 4.5.0
   */
  def validate(): Unit =
    validate(
      checkVertices = true,
      intermediateStorageLevel = GraphConf.storageLevel(vertices.sparkSession))

  /**
   * Checks that the graph is valid, with control over the cost.
   *
   * The checks run in order and stop at the first failure:
   *   - NULL IDs: the "id" column of the vertices and the "src" and "dst" columns of the edges
   *     must not contain NULLs. This is a light check. It always runs, because NULL IDs silently
   *     produce wrong results and NULL join keys can make joins in algorithms very expensive.
   *   - Unique vertex IDs: two Spark jobs over the vertices. Always runs.
   *   - Existing edge endpoints: the "src" and "dst" of every edge must be existing vertex IDs.
   *     Runs only if `checkVertices` is true. It takes a distinct over all edge endpoints and a
   *     join with the vertices, which is usually the expensive part on large graphs.
   *
   * Intermediate results are persisted with `intermediateStorageLevel` and unpersisted before the
   * method returns, also when it throws. The input `DataFrame`s are not persisted.
   *
   * @param checkVertices
   *   whether to check that the "src" and "dst" of every edge are existing vertex IDs
   * @param intermediateStorageLevel
   *   the storage level for intermediate results
   * @throws org.apache.spark.SparkRuntimeException
   *   if the graph is invalid
   *
   * @group utils
   * @since 4.5.0
   */
  def validate(checkVertices: Boolean, intermediateStorageLevel: StorageLevel): Unit = {
    if (!vertices.where(col(ID).isNull).isEmpty) {
      throw GraphErrors.nullIdsError(VERTICES, Seq(ID))
    }
    if (!edges.where(col(SRC).isNull || col(DST).isNull).isEmpty) {
      throw GraphErrors.nullIdsError(EDGES, Seq(SRC, DST))
    }

    val vertexIds = vertices.select(col(ID)).persist(intermediateStorageLevel)
    try {
      val distinctVertexCount = vertexIds.select(countDistinct(col(ID))).first().getLong(0)
      val vertexCount = vertexIds.count()
      if (distinctVertexCount != vertexCount) {
        throw GraphErrors.duplicateVerticesError(vertexCount - distinctVertexCount, ID)
      }
      if (checkVertices) {
        val edgeVertexIds = edges
          .select(col(SRC).as(ID))
          .union(edges.select(col(DST).as(ID)))
          .distinct()
          .persist(intermediateStorageLevel)
        try {
          val edgeVertexCount = edgeVertexIds.count()
          if (edgeVertexCount > distinctVertexCount) {
            throw GraphErrors.edgeVerticesCountMismatchError(edgeVertexCount, distinctVertexCount)
          }
          val missingVertexCount = edgeVertexIds.join(vertexIds, Seq(ID), "left_anti").count()
          if (missingVertexCount > 0) {
            throw GraphErrors.edgeVertexNotInVerticesError(missingVertexCount, SRC, DST)
          }
        } finally {
          edgeVertexIds.unpersist()
        }
      }
    } finally {
      vertexIds.unpersist()
    }
  }
}

/**
 * :: Experimental :: Companion object with the column name constants and the `apply` factory
 * method.
 *
 * @since 4.5.0
 */
@Experimental
object GraphFrame extends Serializable {

  /**
   * The name of the vertex ID column: "id".
   *
   * @since 4.5.0
   */
  val ID: String = "id"

  /**
   * The name of the edge source column: "src".
   *
   * @since 4.5.0
   */
  val SRC: String = "src"

  /**
   * The name of the edge destination column: "dst".
   *
   * @since 4.5.0
   */
  val DST: String = "dst"

  /**
   * The name of the column that holds the edge data in results that combine vertices and edges:
   * "edge".
   *
   * @since 4.5.0
   */
  val EDGE: String = "edge"

  /**
   * The name of the optional edge weight column: "weight".
   *
   * @since 4.5.0
   */
  val WEIGHT: String = "weight"

  /** The name of the intermediate edge type column in the type degree metrics. */
  private val EDGE_TYPE: String = "edge_type"

  /**
   * The data types that can be used for the vertex ID and the edge source and destination
   * columns.
   */
  private val SUPPORTED_ID_TYPES: Seq[DataType] =
    Seq(ByteType, ShortType, IntegerType, LongType, StringType)

  /**
   * Creates a new `GraphFrame` from a vertices `DataFrame` and an edges `DataFrame`.
   *
   * Only the schemas are checked, so this does not run any Spark job. The data itself is not
   * checked (unique and non-NULL IDs, existing edge endpoints); call `validate()` for that.
   *
   * @param vertices
   *   the vertices `DataFrame`. It must contain the "id" column, of a supported data type,
   *   exactly once.
   * @param edges
   *   the edges `DataFrame`. It must contain the "src" and "dst" columns exactly once, with the
   *   same data type as the vertex "id".
   * @return
   *   a new `GraphFrame`
   * @throws org.apache.spark.sql.AnalysisException
   *   if a required column is missing, ambiguous, or has an unsupported or mismatched data type
   * @throws org.apache.spark.SparkIllegalArgumentException
   *   if `vertices` or `edges` is null
   *
   * @since 4.5.0
   */
  @throws[AnalysisException]
  def apply(vertices: DataFrame, edges: DataFrame): GraphFrame = {
    if (vertices == null) {
      throw GraphErrors.nullDataFrameError(VERTICES)
    }
    if (edges == null) {
      throw GraphErrors.nullDataFrameError(EDGES)
    }

    val resolver = columnResolver(vertices)
    checkRequiredColumn(ID, vertices.columns.toSeq, resolver)
    Seq(SRC, DST).foreach(checkRequiredColumn(_, edges.columns.toSeq, resolver))

    val idType = vertices.schema(ID).dataType
    val srcType = edges.schema(SRC).dataType
    val dstType = edges.schema(DST).dataType
    checkIdType(ID, idType, VERTICES)
    checkIdType(SRC, srcType, EDGES)
    checkIdType(DST, dstType, EDGES)
    if (idType != srcType || idType != dstType) {
      throw GraphErrors.idTypeMismatchError(ID, idType, SRC, srcType, DST, dstType)
    }

    new GraphFrame(vertices, edges)
  }

  private def columnResolver(df: DataFrame): Resolver = {
    if (df.sparkSession.conf.get(SQLConf.CASE_SENSITIVE.key).toBoolean) {
      caseSensitiveResolution
    } else {
      caseInsensitiveResolution
    }
  }

  private def checkRequiredColumn(
      name: String,
      columns: Seq[String],
      resolver: Resolver): Unit = {
    if (!columns.contains(name)) {
      throw GraphErrors.unresolvedColumnError(name, columns)
    }
    val numMatches = columns.count(resolver(_, name))
    if (numMatches > 1) {
      throw GraphErrors.ambiguousColumnError(name, numMatches)
    }
  }

  private def checkIdType(name: String, dataType: DataType, dataFrameKind: String): Unit = {
    if (!SUPPORTED_ID_TYPES.contains(dataType)) {
      throw GraphErrors.unsupportedIdTypeError(dataFrameKind, name, dataType, SUPPORTED_ID_TYPES)
    }
  }

  private def checkProperties(
      properties: DataFrame,
      keys: Seq[String],
      target: DataFrame,
      dataFrameKind: String): Unit = {
    if (properties == null) {
      throw GraphErrors.nullDataFrameError(dataFrameKind)
    }
    val resolver = columnResolver(target)
    val columns = properties.columns.toSeq
    keys.foreach(checkRequiredColumn(_, columns, resolver))
    keys.foreach { key =>
      val actualType = properties.schema(key).dataType
      val expectedType = target.schema(key).dataType
      if (actualType != expectedType) {
        throw GraphErrors.propertiesKeyTypeMismatchError(
          dataFrameKind,
          key,
          actualType,
          expectedType)
      }
    }
    val existingColumn = columns
      .filterNot(c => keys.exists(resolver(c, _)))
      .find(c => target.columns.exists(resolver(c, _)))
    existingColumn.foreach { c =>
      throw GraphErrors.columnAlreadyExistsError(c)
    }
  }
}
