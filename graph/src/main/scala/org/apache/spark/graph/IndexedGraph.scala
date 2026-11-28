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

import org.apache.spark.graph.GraphFrame.{DST, ID, SRC}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.util.QuotingUtils.quoteIdentifier
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, hash, lit, pmod, row_number, shiftleft, struct}
import org.apache.spark.sql.graph.GraphCheckpointer
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ByteType, DataType, IntegerType, LongType, ShortType}

/**
 * A view of a graph with `LongType` vertex IDs, for algorithms that work on long IDs only.
 *
 * Integral IDs are cast to `LongType`: nothing is stored and every method is a projection. Other
 * IDs are numbered once, and the mapping from the original ID to the long ID is written into the
 * checkpointer of the run. Everything returned by this class is lazy and reads that mapping, so
 * all of it agrees on the long IDs, whatever the partitioning or AQE do later.
 *
 * The long IDs depend only on the set of vertex IDs and on the number of partitions of the
 * mapping, not on the partitioning of the input or on AQE: the same vertices give the same long
 * IDs in another query or another application. They are unique but not dense: do not use them as
 * array indices.
 *
 * Rules for callers (!!!):
 *   - The mapping is a checkpoint of the given checkpointer and is released with it. Everything
 *     returned by this class reads it, so materialize what must outlive the checkpointer before
 *     `close()`. Reading it after the release fails with user facing error (!).
 *   - Do not return anything from this class, or derived from it, to the end user without
 *     materializing it first.
 *   - The graph is assumed valid and is not checked: an edge whose endpoint is not in the
 *     vertices is dropped by `edges`, and a duplicated vertex ID gets several long IDs. See the
 *     contract in `GraphFrames`
 */
private[graph] class IndexedGraph private (graph: GraphFrame, mapping: Option[DataFrame]) {
  import IndexedGraph._

  /** True if the original IDs are integral and are cast instead of being numbered. */
  def isIntegral: Boolean = mapping.isEmpty

  /**
   * The vertex IDs.
   *
   * @return
   *   the `LONG_ID` and `ID` columns, where `ID` is the original vertex ID
   */
  def vertices: DataFrame = vertices(Nil)

  /**
   * The vertex IDs and the given vertex columns.
   *
   * The columns are a `Seq` and not varargs on purpose: with varargs, `vertices("x")` would
   * compile to `vertices.apply("x")`, a `Column` of the `DataFrame`.
   *
   * NOTE: columns should exists in the graph from which this instance is derived, otherwise users
   * will see `AnalysisException`.
   *
   * @param columns
   *   the vertex columns to put into `ATTR`; may include `ID`
   * @return
   *   the `LONG_ID` and `ID` columns, where `ID` is the original vertex ID, and the `ATTR` struct
   *   of the given columns if there are any
   */
  def vertices(columns: Seq[String]): DataFrame = {
    val attr = attrColumn(columns)
    mapping match {
      case None =>
        graph.vertices.select(Seq(col(ID).cast(LongType).as(LONG_ID), col(ID)) ++ attr: _*)
      case Some(ids) if attr.isEmpty =>
        ids.select(col(LONG_ID), col(ID))
      case Some(ids) =>
        ids
          .join(graph.vertices.select(col(ID) +: attr: _*), ID)
          .select(col(LONG_ID), col(ID), col(ATTR))
    }
  }

  /**
   * The edges with the long IDs of their endpoints.
   *
   * @return
   *   the `SRC`, `LONG_SRC`, `DST` and `LONG_DST` columns, where `SRC` and `DST` are the original
   *   IDs
   */
  def edges: DataFrame = edges(Nil)

  /**
   * The edges with the long IDs of their endpoints and the given edge columns.
   *
   * Note: tha call is lazy!
   *
   * @param columns
   *   the edge columns to put into `ATTR`; may include `SRC` and `DST`
   * @return
   *   the `SRC`, `LONG_SRC`, `DST` and `LONG_DST` columns, where `SRC` and `DST` are the original
   *   IDs, and the `ATTR` struct of the given columns if there are any
   */
  def edges(columns: Seq[String]): DataFrame = {
    val attr = attrColumn(columns)
    val packed = graph.edges.select(Seq(col(SRC), col(DST)) ++ attr: _*)
    val withLongIds = mapping match {
      case None =>
        packed
          .withColumn(LONG_SRC, col(SRC).cast(LongType))
          .withColumn(LONG_DST, col(DST).cast(LongType))
      case Some(ids) =>
        // The mapping is partitioned by ID, so neither join shuffles it.
        packed
          .join(ids.select(col(ID).as(SRC), col(LONG_ID).as(LONG_SRC)), SRC)
          .join(ids.select(col(ID).as(DST), col(LONG_ID).as(LONG_DST)), DST)
    }
    val attrOut = if (attr.isEmpty) Nil else Seq(col(ATTR))
    withLongIds.select(Seq(col(SRC), col(LONG_SRC), col(DST), col(LONG_DST)) ++ attrOut: _*)
  }

  private def attrColumn(columns: Seq[String]): Seq[Column] = {
    if (columns.isEmpty) {
      Nil
    } else {
      Seq(struct(columns.map(c => col(quoteIdentifier(c))): _*).as(ATTR))
    }
  }
}

private[graph] object IndexedGraph {

  /** The name of the column of the vertex long ID: "new_id". */
  val LONG_ID: String = "new_id"

  /** The name of the column of the edge source long ID: "new_src". */
  val LONG_SRC: String = "new_src"

  /** The name of the column of the edge destination long ID: "new_dst". */
  val LONG_DST: String = "new_dst"

  /** The name of the struct column of the requested vertex or edge columns: "attr". */
  val ATTR: String = "attr"

  // The bits of the rank within a bucket, as in monotonically_increasing_id.
  private val BUCKET_SHIFT: Int = 33

  private val INTEGRAL_ID_TYPES: Seq[DataType] = Seq(ByteType, ShortType, IntegerType, LongType)

  /**
   * Indexes `graph` with `spark.sql.shuffle.partitions` partitions of the mapping. See the
   * overload with `numPartitions`.
   *
   * @param graph
   *   the graph
   * @param checkpointer
   *   the checkpointer of the run, which owns the mapping
   * @return
   *   the indexed graph
   */
  def apply(graph: GraphFrame, checkpointer: GraphCheckpointer): IndexedGraph = {
    val spark = graph.vertices.sparkSession
    apply(graph, checkpointer, spark.conf.get(SQLConf.SHUFFLE_PARTITIONS.key).toInt)
  }

  /**
   * Indexes `graph`. For non-integral IDs, runs a Spark job that writes the mapping into
   * `checkpointer`, as one checkpoint partitioned by the original ID. The job shuffles the IDs
   * twice: once to number them and once to partition the mapping.
   *
   * Important (!!!):
   *   - checkpointer must (!) outlive this view!
   *
   * @param graph
   *   the graph
   * @param checkpointer
   *   the checkpointer of the run, which owns the mapping. It must not be closed before
   *   everything derived from the result is materialized. A Parquet one is expected: a
   *   persist-based one keeps the long IDs only while no block of the mapping is lost.
   * @param numPartitions
   *   the number of partitions of the mapping. The long IDs depend on it
   * @return
   *   the indexed graph
   */
  def apply(
      graph: GraphFrame,
      checkpointer: GraphCheckpointer,
      numPartitions: Int): IndexedGraph = {
    val idType = graph.vertices.schema(ID).dataType
    if (INTEGRAL_ID_TYPES.contains(idType)) {
      new IndexedGraph(graph, None)
    } else {
      // The long ID is the hash bucket of the ID in the high bits and the rank of the ID within
      // its bucket in the low bits. It is computed by a window, not from the physical partition
      // (as monotonically_increasing_id does), because Spark can drop a repartition whose input
      // already has its layout and then coalesce that input with AQE. It costs one more shuffle
      // of the IDs: pushPartitioned shuffles them again by ID.
      val bucket = pmod(hash(col(ID)), lit(numPartitions))
      val rank = row_number().over(Window.partitionBy(bucket).orderBy(col(ID))) - 1
      val numbered = graph.vertices
        .select(col(ID))
        .withColumn(LONG_ID, shiftleft(bucket.cast(LongType), BUCKET_SHIFT) + rank)
      val ids = checkpointer.pushPartitioned(numbered, "long-ids", ID, numPartitions)
      new IndexedGraph(graph, Some(ids))
    }
  }
}
