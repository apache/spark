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

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.graph.GraphFrame.{DST, ID, SRC}
import org.apache.spark.graph.IndexedGraph.{ATTR, LONG_DST, LONG_ID, LONG_SRC}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, Encoder, Encoders, Row}
import org.apache.spark.sql.catalyst.util.QuotingUtils.quoteIdentifier
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.graph.ParquetCheckpointer
import org.apache.spark.sql.internal.SQLConf

/**
 * The helper for running GraphX algorithms on a `GraphFrame`. GraphX is used only as an internal
 * API for algorithms. A public or generic GraphFrame <-> GraphX conversion is rejected on purpose
 * because its ownership is unclear. This helper runs one GraphX algorithm per call and handles
 * the indexing, the memory and the storage of the result.
 *
 * The helper works with the provided checkpointer. When a call returns, normally or with an
 * error, the RDDs that GraphX cached for this call are unpersisted. Only the vertex result comes
 * back: it is written into the same checkpointer that stores the indexed view of the graph.
 *
 * Rules for callers (!!!):
 *   - The result and the long ID mapping are released by `checkpointer.close()`: materialize the
 *     result before closing it.
 *   - Do not set `spark.graphx.pregel.checkpointInterval`: GraphX checkpoints cut the lineage, so
 *     some cached RDDs are not found and stay cached, and the checkpoint files are not removed. A
 *     warning is logged when it is set.
 *   - Works only with classic Spark: GraphX needs the `SparkContext`.
 */
private[graph] object GraphXAdapter extends Logging {

  /**
   * Which columns become the GraphX attribute of a vertex or an edge, and how.
   *
   * @param columns
   *   the vertex or edge columns to pass to GraphX; only these columns are read
   * @param extract
   *   builds the attribute from a `Row` with the given columns, in the given order. It runs on
   *   executors, so it must be serializable. Null values are passed as they are
   * @tparam A
   *   the type of the attribute
   */
  case class Attr[A](columns: Seq[String], extract: Row => A)

  object Attr {

    /** No attribute: no columns are read and the attribute is `()`. */
    val unit: Attr[Unit] = Attr(Nil, _ => ())
  }

  /** The config that makes GraphX Pregel checkpoint its RDDs. */
  private val PREGEL_CHECKPOINT_INTERVAL: String = "spark.graphx.pregel.checkpointInterval"

  // GraphX names the partition RDDs of its vertex and edge RDDs with these prefixes.
  private val GRAPHX_RDD_NAME_PREFIXES: Seq[String] = Seq("VertexRDD", "EdgeRDD")

  private val VALUE: String = "value"

  /**
   * Runs `algorithm` on the GraphX view of `graph` and returns its vertex result with the
   * original vertex IDs.
   *
   * The call is eager. Non-integral IDs are indexed first, which is one Spark job that writes the
   * long ID mapping into `checkpointer` (see `IndexedGraph`). Then the algorithm runs and its
   * result is written into `checkpointer` by one more job.
   *
   * The graph is not checked, as in `IndexedGraph`: for non-integral IDs, an edge whose endpoint
   * is not in the vertices is dropped. For integral IDs, GraphX adds the missing endpoint as a
   * vertex with a `null` attribute.
   *
   * @param graph
   *   the graph
   * @param checkpointer
   *   the checkpointer of the run. It stores the long ID mapping and the result, and must not be
   *   closed before the result is materialized
   * @param vertices
   *   the vertex attribute; `Attr.unit` for none
   * @param edges
   *   the edge attribute; `Attr.unit` for none
   * @param resultColumn
   *   the name of the result column; it must not be `ID`
   * @param resultEncoder
   *   the encoder of the result; a product type becomes a struct column
   * @param algorithm
   *   the GraphX call. It returns the result per long vertex ID, for example the `vertices` of
   *   the graph returned by a `graphx.lib` algorithm. Vertices that it does not return are not in
   *   the result. Do not cache anything that must outlive the call: the cached GraphX RDDs
   *   derived from the graph are unpersisted when the call returns; anything else that it caches,
   *   it must release itself
   * @tparam VD
   *   the type of the vertex attribute
   * @tparam ED
   *   the type of the edge attribute
   * @tparam T
   *   the type of the result
   * @return
   *   the `ID` and `resultColumn` columns, where `ID` is the original vertex ID with its original
   *   type. The data is stored in `checkpointer` and is released by its `close()`
   * @throws org.apache.spark.sql.AnalysisException
   *   `COLUMN_ALREADY_EXISTS` if `resultColumn` is `ID`
   * @throws org.apache.spark.SparkRuntimeException
   *   `GRAPH_INVALID.NULL_IDS` if a vertex or an edge has a null ID
   */
  @throws[AnalysisException]
  def run[VD: ClassTag, ED: ClassTag, T](
      graph: GraphFrame,
      checkpointer: ParquetCheckpointer,
      vertices: Attr[VD],
      edges: Attr[ED],
      resultColumn: String,
      resultEncoder: Encoder[T])(algorithm: Graph[VD, ED] => RDD[(VertexId, T)]): DataFrame = {
    if (SQLConf.get.resolver(resultColumn, ID)) {
      throw GraphErrors.columnAlreadyExistsError(resultColumn)
    }
    val spark = graph.vertices.sparkSession
    val sc = spark.sparkContext
    warnIfPregelCheckpoints(sc)

    val idType = graph.vertices.schema(ID).dataType
    val indexed = IndexedGraph(graph, checkpointer)
    val vertexRdd = toVertexRdd(indexed, vertices)
    val edgeRdd = toEdgeRdd(indexed, edges)
    try {
      val gx = Graph(vertexRdd, edgeRdd, null.asInstanceOf[VD])
      val result = spark
        .createDataset(algorithm(gx))(Encoders.tuple(Encoders.scalaLong, resultEncoder))
        .toDF(LONG_ID, VALUE)
      val translated = if (indexed.isIntegral) {
        result.select(col(LONG_ID).cast(idType).as(ID), col(VALUE).as(resultColumn))
      } else {
        indexed.vertices
          .join(result, LONG_ID)
          .select(col(ID), col(VALUE).as(resultColumn))
      }
      // The push is eager, so the result does not read the GraphX RDDs released below.
      checkpointer.push(translated, "graphx-result")
    } finally {
      unpersistGraphXRdds(sc, Seq(vertexRdd, edgeRdd))
    }
  }

  private def warnIfPregelCheckpoints(sc: SparkContext): Unit = {
    val interval = sc.getReadOnlyConf.getInt(PREGEL_CHECKPOINT_INTERVAL, -1)
    if (interval > 0) {
      logWarning(
        log"${MDC(LogKeys.CONFIG, PREGEL_CHECKPOINT_INTERVAL)} is " +
          log"${MDC(LogKeys.CHECKPOINT_INTERVAL, interval)}: the checkpoints of GraphX cut " +
          log"the lineage, so some cached GraphX RDDs may not be released, and the checkpoint " +
          log"files are not removed.")
    }
  }

  private def toVertexRdd[VD](indexed: IndexedGraph, attr: Attr[VD]): RDD[(VertexId, VD)] = {
    val extract = attr.extract
    val hasAttr = attr.columns.nonEmpty
    indexed
      .vertices(attr.columns)
      .select(col(LONG_ID) +: attrColumn(hasAttr): _*)
      .rdd
      .map { row =>
        if (row.isNullAt(0)) {
          throw GraphErrors.nullIdsError(GraphErrors.VERTICES, Seq(ID))
        }
        (row.getLong(0), extract(if (hasAttr) row.getStruct(1) else Row.empty))
      }
  }

  private def toEdgeRdd[ED](indexed: IndexedGraph, attr: Attr[ED]): RDD[Edge[ED]] = {
    val extract = attr.extract
    val hasAttr = attr.columns.nonEmpty
    indexed
      .edges(attr.columns)
      .select(Seq(col(LONG_SRC), col(LONG_DST)) ++ attrColumn(hasAttr): _*)
      .rdd
      .map { row =>
        if (row.isNullAt(0) || row.isNullAt(1)) {
          throw GraphErrors.nullIdsError(GraphErrors.EDGES, Seq(SRC, DST))
        }
        Edge(
          row.getLong(0),
          row.getLong(1),
          extract(if (hasAttr) row.getStruct(2) else Row.empty))
      }
  }

  private def attrColumn(hasAttr: Boolean): Seq[Column] =
    if (hasAttr) Seq(col(quoteIdentifier(ATTR))) else Nil

  /**
   * Unpersists the cached GraphX RDDs that are derived from `roots`. GraphX caches RDDs that its
   * algorithms never unpersist, such as the parent vertices in `mapVertices`, so unpersisting the
   * input and the result graphs is not enough. Other cached RDDs, including GraphX RDDs of other
   * runs, do not depend on `roots`, which this run created, and are kept.
   */
  private def unpersistGraphXRdds(sc: SparkContext, roots: Seq[RDD[_]]): Unit = {
    val rootIds = roots.map(_.id).toSet
    // The parents of an RDD are created before it, so an RDD older than all the roots cannot
    // depend on them.
    val minRootId = rootIds.min
    val candidates = sc.getPersistentRDDs.values
      .filter(rdd => rdd.id > minRootId && isGraphXRdd(rdd))
      .toSeq
      .sortBy(_.id)
    // Iterative, because the lineage of Pregel is as long as the number of iterations.
    val derived = mutable.Set.empty[Int]
    candidates.foreach { candidate =>
      val visited = mutable.Set.empty[Int]
      val stack = mutable.Stack[RDD[_]](candidate)
      var found = false
      while (!found && stack.nonEmpty) {
        val rdd = stack.pop()
        if (rootIds.contains(rdd.id) || derived.contains(rdd.id)) {
          found = true
        } else if (rdd.id > minRootId && visited.add(rdd.id)) {
          rdd.dependencies.foreach(dep => stack.push(dep.rdd))
        }
      }
      if (found) {
        derived += candidate.id
      }
    }
    candidates.filter(rdd => derived.contains(rdd.id)).foreach(_.unpersist(blocking = false))
  }

  private def isGraphXRdd(rdd: RDD[_]): Boolean =
    Option(rdd.name).exists(name => GRAPHX_RDD_NAME_PREFIXES.exists(name.startsWith))
}
