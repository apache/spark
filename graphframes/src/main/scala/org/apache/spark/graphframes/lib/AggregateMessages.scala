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

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.struct
import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.Logging
import org.apache.spark.graphframes.WithIntermediateStorageLevel

/**
 * This is a primitive for implementing graph algorithms. This method aggregates messages from the
 * neighboring edges and vertices of each vertex.
 *
 * For each triplet (source vertex, edge, destination vertex) in [[GraphFrame.triplets]], this can
 * send a message to the source and/or destination vertices.
 *   - `AggregateMessages.sendToSrc()` sends a message to the source vertex of each triplet
 *   - `AggregateMessages.sendToDst()` sends a message to the destination vertex of each triplet
 *   - `AggregateMessages.agg` specifies an aggregation function for aggregating the messages sent
 *     to each vertex. It also runs the aggregation, computing a DataFrame with one row for each
 *     vertex which receives > 0 messages. The DataFrame has 2 columns:
 *     - vertex column ID (named [[GraphFrame.ID]])
 *     - aggregate from messages sent to vertex (with the name given to the `Column` specified in
 *       `AggregateMessages.agg()`)
 *
 * When specifying the messages and aggregation function, the user may reference columns using:
 *   - [[AggregateMessages.src]]: column for source vertex of edge
 *   - [[AggregateMessages.edge]]: column for edge
 *   - [[AggregateMessages.dst]]: column for destination vertex of edge
 *   - [[AggregateMessages.msg]]: message sent to vertex (for aggregation function)
 *
 * Note: If you use this operation to write an iterative algorithm, you may want to use
 * `checkpoint()` (`localCheckpoint()`) as a workaround for caching issues.
 *
 * @example
 *   We can use this function to compute the in-degree of each vertex
 *   {{{
 * val g: GraphFrame = Graph.textFile("twittergraph")
 * val inDeg: DataFrame =
 *   g.aggregateMessages().sendToDst(lit(1)).agg(sum(AggregateMessagesBuilder.msg))
 *   }}}
 */
class AggregateMessages private[graphframes] (private val g: GraphFrame)
    extends Arguments
    with Serializable
    with WithIntermediateStorageLevel
    with Logging {

  import org.apache.spark.graphframes.GraphFrame.DST
  import org.apache.spark.graphframes.GraphFrame.ID
  import org.apache.spark.graphframes.GraphFrame.SRC

  private var msgToSrc: Seq[Column] = Vector()

  /** Send message to source vertex */
  def sendToSrc(value: Column, values: Column*): this.type = {
    msgToSrc = value +: values
    this
  }

  // Python API compatibility
  def sendToSrc(value: Column): this.type = {
    msgToSrc :+= value
    this
  }
  def sendToSrc(value: String): this.type = {
    msgToSrc :+= expr(value)
    this
  }

  /** Send message to source vertex, specifying SQL expression as a String */
  def sendToSrc(value: String, values: String*): this.type =
    sendToSrc(expr(value), values.map(expr): _*)

  private var msgToDst: Seq[Column] = Vector()

  /** Send message to destination vertex */
  def sendToDst(value: Column, values: Column*): this.type = {
    msgToDst = value +: values
    this
  }

  // Python API compatibility
  def sendToDst(value: String): this.type = {
    msgToDst :+= expr(value)
    this
  }
  def sendToDst(value: Column): this.type = {
    msgToDst :+= value
    this
  }

  /** Send message to destination vertex, specifying SQL expression as a String */
  def sendToDst(value: String, values: String*): this.type =
    sendToDst(expr(value), values.map(expr): _*)

  /**
   * Run the aggregation, returning the resulting DataFrame of aggregated messages. This is a lazy
   * operation, so the DataFrame will not be materialized until an action is executed on it.
   *
   * This returns a DataFrame with schema:
   *   - column "id": vertex ID
   *   - aggCol: aggregate result
   *   - aggCols: one column with the result of each additional defined aggregation
   * If you need to join this with the original [[GraphFrame.vertices]], you can run an inner join
   * of the form:
   * {{{
   *   val g: GraphFrame = ...
   *   val aggResult = g.AggregateMessagesBuilder.sendToSrc(msg).agg(aggFunc)
   *   aggResult.join(g.vertices, ID)
   * }}}
   */
  def agg(aggCol: Column, aggCols: Column*): DataFrame = {
    require(
      msgToSrc.nonEmpty || msgToDst.nonEmpty,
      "To run GraphFrame.aggregateMessages," +
        " messages must be sent to src, dst, or both.  Set using sendToSrc(), sendToDst().")
    val triplets = g.triplets

    def msgColumn(columns: Seq[Column], idColumn: Column): DataFrame = columns match {
      case Seq(c) => triplets.select(idColumn.as(ID), c.as(AggregateMessages.MSG_COL_NAME))
      case columns =>
        triplets.select(idColumn.as(ID), struct(columns: _*).as(AggregateMessages.MSG_COL_NAME))
    }

    val cachedVertices = g.vertices.persist(intermediateStorageLevel)

    val sentMsgsToSrc = msgToSrc.headOption.map { _ =>
      val msgsToSrc = msgColumn(msgToSrc, triplets(SRC)(ID))
      msgsToSrc
        .join(cachedVertices, ID)
        .select(msgsToSrc(AggregateMessages.MSG_COL_NAME), col(ID))
    }
    val sentMsgsToDst = msgToDst.headOption.map { _ =>
      val msgsToDst = msgColumn(msgToDst, triplets(DST)(ID))

      msgsToDst
        .join(cachedVertices, ID)
        .select(msgsToDst(AggregateMessages.MSG_COL_NAME), col(ID))
    }
    val unionMsgs = (sentMsgsToSrc, sentMsgsToDst) match {
      case (Some(toSrc), Some(toDst)) =>
        toSrc.unionAll(toDst)
      case (Some(toSrc), None) => toSrc
      case (None, Some(toDst)) => toDst
      case _ =>
        // Should never happen. Specify this case to avoid compilation warnings.
        throw new RuntimeException("AggregateMessages: No messages were specified to be sent.")
    }

    val cachedResult =
      unionMsgs.groupBy(ID).agg(aggCol, aggCols: _*).persist(intermediateStorageLevel)
    // materialize
    cachedResult.count()
    cachedVertices.unpersist()
    resultIsPersistent()
    cachedResult
  }

  // Python compatibility
  def agg(aggCol: Column): DataFrame = agg(aggCol, Seq.empty[Column]: _*)
  def agg(aggCol: String): DataFrame = agg(expr(aggCol), Seq.empty[Column]: _*)

  /**
   * Run the aggregation, specifying SQL expression as a String
   *
   * See the overloaded method documentation for more details.
   */
  def agg(aggCol: String, aggCols: String*): DataFrame =
    agg(expr(aggCol), aggCols.map(expr(_)): _*)
}

object AggregateMessages extends Logging with Serializable {

  /** Column name for aggregated messages, used in [[AggregateMessages.msg]] */
  val MSG_COL_NAME: String = "MSG"

  /** Reference for source column, used for specifying messages */
  def src: Column = col(GraphFrame.SRC)

  /** Reference for destination column, used for specifying messages */
  def dst: Column = col(GraphFrame.DST)

  /** Reference for edge column, used for specifying messages */
  def edge: Column = col(GraphFrame.EDGE)

  /** Reference for message column, used for specifying aggregation function */
  def msg: Column = col(MSG_COL_NAME)
}
