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

package org.apache.spark.graphframes.lib

import java.io.IOException

import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.GraphFrame._
import org.apache.spark.graphframes.Logging
import org.apache.spark.graphframes.WithIntermediateStorageLevel
import org.apache.spark.graphframes.WithLocalCheckpoints
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.graphframes.GraphFrameInternals

/**
 * Implements a Pregel-like bulk-synchronous message-passing API based on DataFrame operations.
 *
 * See <a href="https://doi.org/10.1145/1807167.1807184">Malewicz et al., Pregel: a system for
 * large-scale graph processing</a> for a detailed description of the Pregel algorithm.
 *
 * You can construct a Pregel instance using either this constructor or
 * `GraphFrame.pregel`, then use builder pattern to describe the operations, and then call [[run]]
 * to start a run. It returns a DataFrame of vertices from the last iteration.
 *
 * When a run starts, it expands the vertices DataFrame using column expressions defined by
 * [[withVertexColumn]]. Those additional vertex properties can be changed during Pregel
 * iterations. In each Pregel iteration, there are three phases:
 *   - Given each edge triplet, generate messages and specify target vertices to send, described
 *     by [[sendMsgToDst]] and [[sendMsgToSrc]].
 *   - Aggregate messages by target vertex IDs, described by [[aggMsgs]].
 *   - Update additional vertex properties based on aggregated messages and states from previous
 *     iteration, described by [[withVertexColumn]].
 *
 * Please find what columns you can reference at each phase in the method API docs.
 *
 * You can control the number of iterations by [[setMaxIter]] and check API docs for advanced
 * controls.
 *
 * Example code for Page Rank:
 *
 * {{{
 *   val edges = ...
 *   val vertices = GraphFrame.fromEdges(edges).outDegrees.cache()
 *   val numVertices = vertices.count()
 *   val graph = GraphFrame(vertices, edges)
 *   val alpha = 0.15
 *   val ranks = graph.pregel
 *     .withVertexColumn("rank", lit(1.0 / numVertices),
 *       coalesce(Pregel.msg, lit(0.0)) * (1.0 - alpha) + alpha / numVertices)
 *     .sendMsgToDst(Pregel.src("rank") / Pregel.src("outDegree"))
 *     .aggMsgs(sum(Pregel.msg))
 *     .run()
 * }}}
 *
 * Migration note: pre 0.12 users that used edge columns in Pregel expressions should explicitly
 * specify these columns using [[org.apache.spark.graphframes.lib.Pregel#requiredDstColumns]]. In
 * 0.11 and earlier there was an unspecified bug that leads all the edge columns are always kept
 * and persisted that created a bug memory pressure (2 columns in O(|E|) rows in the form of
 * `StructType`). That behavior is considered as bug and starting from 0.12 edge columns are not
 * kept by default.
 *
 * See `GraphFrame.pregel` and
 * <a href="https://doi.org/10.1145/1807167.1807184">Malewicz et al., Pregel: a system for
 * large-scale graph processing</a>.
 *
 * @param graph
 *   The graph that Pregel will run on.
 */
class Pregel(val graph: GraphFrame)
    extends Logging
    with WithLocalCheckpoints
    with WithIntermediateStorageLevel {

  private val withVertexColumnList = collection.mutable.ListBuffer.empty[(String, Column, Column)]

  private var maxIter: Int = 10
  private var checkpointInterval = 2
  private var earlyStopping = false
  private var stopIfAllNonActiveVertices = false
  private var skipMessagesFromNonActiveVertices = false
  private var initialActiveVertexExpression = lit(true)
  private var updateActiveVertexExpression = lit(true)

  private val sendMsgs = collection.mutable.ListBuffer.empty[(Column, Column)]
  private var aggMsgsCol: Column = null

  // Required columns for source and destination vertices in triplets
  // When empty, all columns are selected (default behavior)
  private val requiredSrcColumnsList = collection.mutable.ListBuffer.empty[String]
  private val requiredDstColumnsList = collection.mutable.ListBuffer.empty[String]

  // Required columns for edges
  // When empty, only src and dst are selected
  private val requiredEdgeColumnsList = collection.mutable.ListBuffer.empty[String]

  /** Sets the max number of iterations (default: 10). */
  def setMaxIter(value: Int): this.type = {
    maxIter = value
    this
  }

  /**
   * Sets the number of iterations between two checkpoints (default: 2).
   *
   * This is an advanced control to balance query plan optimization and checkpoint data I/O cost.
   * In most cases, you should keep the default value.
   *
   * Checkpoint is disabled if this is set to 0.
   */
  def setCheckpointInterval(value: Int): this.type = {
    checkpointInterval = value
    this
  }

  /**
   * Should Pregel stop earlier in case of no new messages to send?
   *
   * Early stopping allows to terminate Pregel before reaching maxIter by checking is there any
   * non-null message or not. While in some cases it may gain significant performance boost, it
   * other cases it can tend to performance degradation, because checking is messages DataFrame is
   * empty or not is an action and requires materialization of the Spark Plan with some additional
   * computations.
   *
   * In the case when user can assume a good value of maxIter it is recommended to leave this
   * value to the default "false". In the case when it is hard to estimate an amount of iterations
   * required for convergence, it is recommended to set this value to "false" to avoid iterating
   * over convergence until reaching maxIter. When this value is "true", maxIter can be set to a
   * bigger value without risks.
   *
   * @param value
   *   should Pregel checks for the termination condition on each step
   * @return
   */
  def setEarlyStopping(value: Boolean): this.type = {
    earlyStopping = value
    this
  }

  /**
   * Should Pregel stop earlier in case all the vertices are marked as non active.
   *
   * This feature allows to terminate Pregel before reaching maxIter by checking are there active
   * vertex left. A good example of activity check is PageRank: (see Malewicz, Grzegorz, et al.
   * "Pregel: a system for large-scale graph processing." Proceedings of the 2010 ACM SIGMOD
   * International Conference on Management of data. 2010., a part about voting to halt)
   *   - after each iteration we are checking is the change in rank less than tolerance and if so,
   *     we can mark vertex as non active
   *   - if all the vertices are non active, we stop iterations
   * @param value
   *   should Pregel stop earlier by vertices voting
   * @return
   */
  def setStopIfAllNonActiveVertices(value: Boolean): this.type = {
    stopIfAllNonActiveVertices = value
    this
  }

  /**
   * Set the initial expression for the active/non-active flag per vertex.
   *
   * In most of the cases the default expression (true for all the vertices) should works fine.
   * For some cases it makes sense to set a custom expression. A good example is
   * multiple-landmarks shortest-paths algorithm:
   *   - the only initially active vertices in that case should be landmarkds, because only this
   *     vertices initially have non-null distances but all the other vertices have null distances
   *     and there is no reason to mark them active initially.
   * @param expression
   *   an initial expression that will be used to create an active-flag vertex column
   * @return
   */
  def setInitialActiveVertexExpression(expression: Column): this.type = {
    initialActiveVertexExpression = expression
    this
  }

  /**
   * Set an expression that will be used after each superstep to update the active-flag vertex
   * column.
   *
   * An example is PageRank algorithm: in that case such an expression may looks like abs(old_rank -
   * new_rank) >= tolerance
   *
   * @param expression
   *   an expression, that will be used after each superstep to update the active-flag vertex
   *   column
   * @return
   */
  def setUpdateActiveVertexExpression(expression: Column): this.type = {
    updateActiveVertexExpression = expression
    this
  }

  /**
   * With a true value, Pregel will not generate messages from vertices, marked as non active.
   *
   * For example, for Shortest Paths, there is no reason to pass distances from vertices, for that
   * these distances did not change at the latest iteration. It allows significantly reduce an
   * amount of generated messages.
   *
   * Be careful, for algorithms like Label Propagation or Pregel, even if the vertex is not
   * active, we still need to generate messages, otherwise algorithm will return an incorrect
   * result!
   *
   * @param value
   *   should Pregel skip generation of messages for non active vertices.
   * @return
   */
  def setSkipMessagesFromNonActiveVertices(value: Boolean): this.type = {
    skipMessagesFromNonActiveVertices = value
    this
  }

  /**
   * Defines an additional vertex column at the start of run and how to update it in each
   * iteration.
   *
   * You can call it multiple times to add more than one additional vertex columns.
   *
   * @param colName
   *   the name of the additional vertex column. It cannot be an existing vertex column in the
   *   graph.
   * @param initialExpr
   *   the expression to initialize the additional vertex column. You can reference all original
   *   vertex columns in this expression.
   * @param updateAfterAggMsgsExpr
   *   the expression to update the additional vertex column after messages aggregation. You can
   *   reference all original vertex columns, additional vertex columns, and the aggregated
   *   message column using `Pregel.msg`. If the vertex received no messages, the message
   *   column would be null.
   */
  def withVertexColumn(
      colName: String,
      initialExpr: Column,
      updateAfterAggMsgsExpr: Column): this.type = {
    // TODO: check if this column exists.
    require(
      colName != null && colName != ID && colName != Pregel.MSG_COL_NAME,
      "additional column name cannot be null and cannot be the same name with ID column or " +
        "msg column.")
    require(initialExpr != null, "additional column should provide a nonnull initial expression.")
    require(
      updateAfterAggMsgsExpr != null,
      "additional column should provide a nonnull " +
        "updateAfterAggMsgs expression.")
    withVertexColumnList += Tuple3(colName, initialExpr, updateAfterAggMsgsExpr)
    this
  }

  /**
   * Defines a message to send to the source vertex of each edge triplet.
   *
   * You can call it multiple times to send more than one messages.
   *
   * @param msgExpr
   *   the expression of the message to send to the source vertex given a (src, edge, dst)
   *   triplet. Source/destination vertex properties and edge properties are nested under columns
   *   `src`, `dst`, and `edge`, respectively. You can reference them using `Pregel.src`,
   *   `Pregel.dst`, and `Pregel.edge`. Null messages are not included in message
   *   aggregation.
   */
  def sendMsgToSrc(msgExpr: Column): this.type = {
    sendMsgs += Tuple2(Pregel.src(ID), msgExpr)
    this
  }

  /**
   * Defines a message to send to the destination vertex of each edge triplet.
   *
   * You can call it multiple times to send more than one messages.
   *
   * @param msgExpr
   *   the message expression to send to the destination vertex given a (`src`, `edge`, `dst`)
   *   triplet. Source/destination vertex properties and edge properties are nested under columns
   *   `src`, `dst`, and `edge`, respectively. You can reference them using `Pregel.src`,
   *   `Pregel.dst`, and `Pregel.edge`. Null messages are not included in message
   *   aggregation.
   */
  def sendMsgToDst(msgExpr: Column): this.type = {
    sendMsgs += Tuple2(Pregel.dst(ID), msgExpr)
    this
  }

  /**
   * Specifies which source vertex columns are required when constructing triplets.
   *
   * By default, all source vertex columns are included in triplets, which can create large
   * intermediate datasets for algorithms with significant state (e.g., cycle detection, random
   * walks). Use this method to reduce memory usage by specifying only the columns that are
   * actually needed by the sendMsgToSrc and sendMsgToDst expressions.
   *
   * The ID column and the active flag column (if used) are always included automatically.
   *
   * @param colName
   *   the first required source vertex column name
   * @param colNames
   *   additional required source vertex column names
   */
  def requiredSrcColumns(colName: String, colNames: String*): this.type = {
    requiredSrcColumnsList.clear()
    requiredSrcColumnsList += colName
    requiredSrcColumnsList ++= colNames
    this
  }

  /**
   * Specifies which destination vertex columns are required when constructing triplets.
   *
   * By default, all destination vertex columns are included in triplets, which can create large
   * intermediate datasets for algorithms with significant state (e.g., cycle detection, random
   * walks). Use this method to reduce memory usage by specifying only the columns that are
   * actually needed by the sendMsgToSrc and sendMsgToDst expressions.
   *
   * The ID column and the active flag column (if used) are always included automatically.
   *
   * @param colName
   *   the first required destination vertex column name
   * @param colNames
   *   additional required destination vertex column names
   */
  def requiredDstColumns(colName: String, colNames: String*): this.type = {
    requiredDstColumnsList.clear()
    requiredDstColumnsList += colName
    requiredDstColumnsList ++= colNames
    this
  }

  /**
   * Specifies which edge columns are required when constructing triplets.
   *
   * By default, only the source and destination ID columns from edges are included in triplets.
   * Use this method to include additional edge properties that are needed by the sendMsgToSrc and
   * sendMsgToDst expressions.
   *
   * @param colName
   *   the first required edge column name
   * @param colNames
   *   additional required edge column names
   */
  def requiredEdgeColumns(colName: String, colNames: String*): this.type = {
    requiredEdgeColumnsList.clear()
    requiredEdgeColumnsList += colName
    requiredEdgeColumnsList ++= colNames
    this
  }

  /**
   * Defines how messages are aggregated after grouped by target vertex IDs.
   *
   * @param aggExpr
   *   the message aggregation expression, such as `sum(Pregel.msg)`. You can reference the
   *   message column by `Pregel.msg` and the vertex ID by `GraphFrame.ID`, while the latter is
   *   usually not used.
   */
  def aggMsgs(aggExpr: Column): this.type = {
    aggMsgsCol = aggExpr
    this
  }

  /**
   * Runs the defined Pregel algorithm.
   *
   * @return
   *   the result vertex DataFrame from the final iteration including both original and additional
   *   columns.
   */
  def run(): DataFrame = {
    require(
      sendMsgs.length > 0,
      "We need to set at least one message expression for pregel running.")
    require(aggMsgsCol != null, "We need to set aggMsgs for pregel running.")
    require(maxIter >= 1, "The max iteration number should be >= 1.")
    require(
      checkpointInterval >= 0,
      "The checkpoint interval should be >= 0, 0 indicates no checkpoint.")
    require(
      withVertexColumnList.size > 0,
      "There should be at least one additional vertex columns for updating.")

    val sendMsgsColList = sendMsgs.toList.map { case (id, msg) =>
      struct(id.as(ID), msg.as("msg"))
    }

    val initVertexCols = withVertexColumnList.toList.map { case (colName, initExpr, _) =>
      initExpr.as(colName)
    }
    val updateVertexCols = withVertexColumnList.toList.map { case (colName, _, updateExpr) =>
      updateExpr.as(colName)
    }

    var lastRoundPersistent: scala.collection.mutable.Queue[DataFrame] =
      scala.collection.mutable.Queue[DataFrame]()

    val initialAttributes = graph.vertices.columns.map(col).toSeq

    var currentVertices = graph.vertices.select(
      ((initialAttributes :+ initialActiveVertexExpression.alias(
        Pregel.ACTIVE_FLAG_COL)) ++ initVertexCols): _*)

    // Automatic optimization: detect if destination vertex state is needed by analyzing
    // the MESSAGE expressions only (not the target ID expressions, since dst.id is always
    // available from the edge). If no message expression references dst.* columns,
    // we can skip the second join entirely.
    // Additionally, if the only dst field referenced is "id", we can still skip since
    // dst.id is available from the edge's dst column.
    val messageExpressions = sendMsgs.toList.map { case (_, msgExpr) => msgExpr }
    val allDstRefs = messageExpressions.flatMap { expr =>
      GraphFrameInternals.extractColumnReferences(graph.spark, expr).get(DST)
    }
    val dstPrefixReferenced = allDstRefs.nonEmpty
    val dstFieldsReferenced = allDstRefs.flatten.toSet

    // We need the dst join if dst is referenced AND fields other than just "id" are accessed
    val needsDstState =
      dstPrefixReferenced && (dstFieldsReferenced.isEmpty || dstFieldsReferenced != Set(ID))
    if (!needsDstState) {
      logDebug(
        "Optimization: skipping second join (dst state not required by message expressions)")
    }

    val edges = (if (requiredEdgeColumnsList.isEmpty) {
                   graph.edges
                     .select(col(SRC).alias("edge_src"), col(DST).alias("edge_dst"))
                 } else {
                   graph.edges
                     .select(
                       col(SRC).alias("edge_src"),
                       col(DST).alias("edge_dst"),
                       struct(
                         requiredEdgeColumnsList.head,
                         requiredEdgeColumnsList.tail.toSeq: _*).as(EDGE))
                 }).repartition(col("edge_src")).persist(intermediateStorageLevel)

    var iteration = 1

    val shouldCheckpoint = checkpointInterval > 0

    if (
      shouldCheckpoint &&
      graph.spark.sparkContext.getCheckpointDir.isEmpty &&
      !useLocalCheckpoints) {
      // Spark Connect workaround
      graph.spark.conf.getOption("spark.checkpoint.dir") match {
        case Some(d) => graph.spark.sparkContext.setCheckpointDir(d)
        case None =>
          throw new IOException(
            "Checkpoint directory is not set. Please set it first using sc.setCheckpointDir()" +
              "or by specifying the conf 'spark.checkpoint.dir'.")
      }
    }

    // Columns to include in triplet structs (ID + active flag always included if specified)
    val srcCols =
      if (requiredSrcColumnsList.isEmpty) Seq(col("*"))
      else (Seq(ID, Pregel.ACTIVE_FLAG_COL) ++ requiredSrcColumnsList).distinct.map(col)
    val dstCols =
      if (requiredDstColumnsList.isEmpty) Seq(col("*"))
      else (Seq(ID, Pregel.ACTIVE_FLAG_COL) ++ requiredDstColumnsList).distinct.map(col)

    breakable {
      while (iteration <= maxIter) {
        logInfo(s"start Pregel iteration $iteration / $maxIter")
        val currRoundPersistent = scala.collection.mutable.Queue[DataFrame]()
        currRoundPersistent.enqueue(currentVertices.persist(intermediateStorageLevel))

        // Prune non-active vertices early if skipMessagesFromNonActiveVertices
        // is enabled and we don't need the dst state.
        val srcVertices =
          if (!needsDstState && skipMessagesFromNonActiveVertices) {
            currentVertices.filter(col(Pregel.ACTIVE_FLAG_COL))
          } else {
            currentVertices
          }

        // Build triplets: start with src vertex state joined with edges
        val srcWithEdges = srcVertices
          .select(struct(srcCols: _*).as(SRC))
          .join(edges, Pregel.src(ID) === col("edge_src"))

        // Only perform the second join (adding dst vertex state) if needed
        var tripletsDF = if (needsDstState) {
          srcWithEdges
            .join(
              currentVertices.select(struct(dstCols: _*).as(DST)),
              col("edge_dst") === Pregel.dst(ID))
            .drop(col("edge_src"), col("edge_dst"))
        } else {
          // Skip second join - dst state not needed by any message expression.
          // Create a minimal dst struct with just the id from edge_dst for sendMsgToDst to work.
          srcWithEdges
            .withColumn(DST, struct(col("edge_dst").as(ID)))
            .drop(col("edge_src"), col("edge_dst"))
        }

        // Only prune here if we didn't prune above.
        if (needsDstState && skipMessagesFromNonActiveVertices) {
          tripletsDF = tripletsDF.filter(
            Pregel.src(Pregel.ACTIVE_FLAG_COL) || Pregel.dst(Pregel.ACTIVE_FLAG_COL))
        }

        val msgDF: DataFrame = tripletsDF
          .select(explode(array(sendMsgsColList: _*)).as("msg"))
          .select(col("msg.id"), col("msg.msg").as(Pregel.MSG_COL_NAME))
          .filter(Pregel.msg.isNotNull)

        if (earlyStopping && msgDF.isEmpty) {
          logInfo(
            s"there are no more non-null messages; Pregel stops earlier at iteration $iteration")
          while (lastRoundPersistent.nonEmpty) {
            lastRoundPersistent.dequeue().unpersist()
          }
          lastRoundPersistent = currRoundPersistent
          break()
        }

        val newAggMsgDF = msgDF
          .groupBy(ID)
          .agg(aggMsgsCol.as(Pregel.MSG_COL_NAME))

        val verticesWithMsg = currentVertices.join(newAggMsgDF, Seq(ID), "left_outer")

        currentVertices = verticesWithMsg.select(
          ((initialAttributes :+ updateActiveVertexExpression.alias(
            Pregel.ACTIVE_FLAG_COL)) ++ updateVertexCols): _*)

        if (shouldCheckpoint && iteration % checkpointInterval == 0) {
          if (useLocalCheckpoints) {
            currentVertices = currentVertices.localCheckpoint(eager = false)
          } else {
            currentVertices = currentVertices.checkpoint(eager = false)
          }
        } else {
          // checkpointing do persistence and we do not need to do it again
          currRoundPersistent.enqueue(currentVertices.persist(intermediateStorageLevel))
        }

        if (stopIfAllNonActiveVertices) {
          if (currentVertices.filter(col(Pregel.ACTIVE_FLAG_COL)).isEmpty) {
            logInfo(
              s"all the verties are non-active; Pregel stops earlier at iteration $iteration")
            while (lastRoundPersistent.nonEmpty) {
              lastRoundPersistent.dequeue().unpersist()
            }
            lastRoundPersistent = currRoundPersistent
            break()
          }
        }

        if (!earlyStopping && !stopIfAllNonActiveVertices) {
          // we need to call materialize
          currentVertices.count()
        }

        while (lastRoundPersistent.nonEmpty) {
          lastRoundPersistent.dequeue().unpersist()
        }
        lastRoundPersistent = currRoundPersistent

        iteration += 1
      }
    }

    val res = currentVertices.persist(intermediateStorageLevel)
    res.count()
    while (lastRoundPersistent.nonEmpty) {
      lastRoundPersistent.dequeue().unpersist()
    }
    edges.unpersist()
    System.gc()
    res
  }

}

/**
 * Constants and utilities for the Pregel algorithm.
 */
object Pregel extends Serializable {

  /**
   * A constant column name for generated and aggregated messages.
   *
   * The vertices DataFrame must not contain this column.
   */
  val MSG_COL_NAME = "_pregel_msg_"

  /**
   * A constant column name for active vertex flag.
   */
  val ACTIVE_FLAG_COL = "_pregel_is_active"

  /**
   * References the message column in aggregating messages and updating additional vertex columns.
   *
   */
  val msg: Column = col(MSG_COL_NAME)

  /**
   * References a source vertex column in generating messages to send.
   *
   * @param colName
   *   the vertex column name.
   */
  def src(colName: String): Column = col(GraphFrame.SRC + "." + colName)

  /**
   * References a destination vertex column in generating messages to send.
   *
   * @param colName
   *   the vertex column name.
   */
  def dst(colName: String): Column = col(GraphFrame.DST + "." + colName)

  /**
   * References an edge column in generating messages to send.
   *
   * @param colName
   *   the edge column name.
   */
  def edge(colName: String): Column = col(GraphFrame.EDGE + "." + colName)
}
