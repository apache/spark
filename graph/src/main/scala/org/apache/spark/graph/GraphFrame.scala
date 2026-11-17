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
import org.apache.spark.sql.functions.{array, col, countDistinct, explode, expr}
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
 * @groupname conversions Conversion operations
 * @groupname subgraph Subgraph selection
 * @groupname transformations Graph transformations
 * @groupname utils Graph utilities
 *
 * @since 5.0.0
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
   * @since 5.0.0
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
   * @since 5.0.0
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
   * @since 5.0.0
   */
  def vertexColumns: Array[String] = vertices.columns

  /**
   * The column names of the edges `DataFrame`, including "src" and "dst".
   *
   * @return
   *   the edge column names, in order
   *
   * @group structure
   * @since 5.0.0
   */
  def edgeColumns: Array[String] = edges.columns

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
   * @since 5.0.0
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
   * @since 5.0.0
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
   * @since 5.0.0
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
   * @since 5.0.0
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
   * @since 5.0.0
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
   * @since 5.0.0
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
   * @since 5.0.0
   */
  @throws[AnalysisException]
  def joinVertices(properties: DataFrame): GraphFrame = {
    checkProperties(properties, Seq(ID), vertices, VERTEX_PROPERTIES, VERTICES)
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
   * @since 5.0.0
   */
  @throws[AnalysisException]
  def joinEdges(properties: DataFrame): GraphFrame = {
    checkProperties(properties, Seq(SRC, DST), edges, EDGE_PROPERTIES, EDGES)
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
   * @since 5.0.0
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
   * `validate(true, StorageLevel.MEMORY_AND_DISK)`.
   *
   * This runs several Spark jobs over the vertices and the edges, so it can be expensive on large
   * graphs. See `validate(checkVertices, intermediateStorageLevel)` for the cost of each check.
   *
   * @throws org.apache.spark.SparkRuntimeException
   *   if the graph is invalid
   *
   * @group utils
   * @since 5.0.0
   */
  def validate(): Unit =
    validate(checkVertices = true, intermediateStorageLevel = StorageLevel.MEMORY_AND_DISK)

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
   * @since 5.0.0
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
 * @since 5.0.0
 */
@Experimental
object GraphFrame extends Serializable {

  /**
   * The name of the vertex ID column: "id".
   *
   * @since 5.0.0
   */
  val ID: String = "id"

  /**
   * The name of the edge source column: "src".
   *
   * @since 5.0.0
   */
  val SRC: String = "src"

  /**
   * The name of the edge destination column: "dst".
   *
   * @since 5.0.0
   */
  val DST: String = "dst"

  /**
   * The name of the column that holds the edge data in results that combine vertices and edges:
   * "edge".
   *
   * @since 5.0.0
   */
  val EDGE: String = "edge"

  /**
   * The name of the optional edge weight column: "weight".
   *
   * @since 5.0.0
   */
  val WEIGHT: String = "weight"

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
   * @since 5.0.0
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
    checkRequiredColumn(ID, vertices.columns.toSeq, resolver, VERTICES)
    Seq(SRC, DST).foreach(checkRequiredColumn(_, edges.columns.toSeq, resolver, EDGES))

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
      resolver: Resolver,
      dataFrameKind: String): Unit = {
    if (!columns.contains(name)) {
      throw GraphErrors.missingRequiredColumnError(dataFrameKind, name, columns)
    }
    val matchingColumns = columns.filter(resolver(_, name))
    if (matchingColumns.length > 1) {
      throw GraphErrors.ambiguousRequiredColumnError(dataFrameKind, name, matchingColumns)
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
      dataFrameKind: String,
      targetKind: String): Unit = {
    if (properties == null) {
      throw GraphErrors.nullDataFrameError(dataFrameKind)
    }
    val resolver = columnResolver(target)
    val columns = properties.columns.toSeq
    keys.foreach(checkRequiredColumn(_, columns, resolver, dataFrameKind))
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
    val conflictingColumns = columns
      .filterNot(c => keys.exists(resolver(c, _)))
      .filter(c => target.columns.exists(resolver(c, _)))
    if (conflictingColumns.nonEmpty) {
      throw GraphErrors.propertiesColumnConflictError(
        dataFrameKind,
        targetKind,
        conflictingColumns)
    }
  }
}
