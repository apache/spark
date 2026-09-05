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

import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.Logging
import org.apache.spark.graphframes.WithCheckpointInterval
import org.apache.spark.graphframes.WithIntermediateStorageLevel
import org.apache.spark.graphframes.WithLocalCheckpoints
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * A class for performing multi-hop neighbor aggregation on a graph.
 *
 * AggregateNeighbors allows you to explore the graph up to a specified number of hops,
 * accumulating values along paths using customizable accumulator expressions. It supports both
 * stopping conditions (when to stop exploring) and target conditions (when to collect a result).
 * If no target condition is provided, collect all traversals that reached stopping condition. The
 * algorithm processes the graph in a breadth-first manner.
 *
 * Use the builder pattern to configure parameters, then call `run()` to execute.
 *
 * @param graph
 *   the GraphFrame on which to perform aggregation
 */
class AggregateNeighbors private[graphframes] (graph: GraphFrame)
    extends Serializable
    with Logging
    with WithIntermediateStorageLevel
    with WithCheckpointInterval
    with WithLocalCheckpoints {

  import AggregateNeighbors._

  private var startingVertices: Column = lit(true)

  private var maxHops: Int = 3
  private var stoppingCondition: Option[Column] = None
  private var targetCondition: Option[Column] = None
  private var accumulatorsNames: Seq[String] = Seq.empty
  private var accumulatorsInits: Seq[Column] = Seq.empty
  private var accumulatorsUpdates: Seq[Column] = Seq.empty

  private var requiredVertexAttributes: Seq[String] = Seq.empty
  private var requiredEdgeAttributes: Seq[String] = Seq.empty

  private var edgeFilter: Column = lit(true)
  private var removeLoops: Boolean = false

  /**
   * Specifies which vertices to start the aggregation from.
   *
   * Only vertices satisfying the given column expression will be used as seeds for the traversal.
   * The default is `true` (all vertices are seeds).
   *
   * @param value
   *   a Boolean Column expression that selects seed vertices
   * @return
   *   this AggregateNeighbors instance for method chaining
   */
  def setStartingVertices(value: Column): this.type = {
    this.startingVertices = value
    this
  }

  /**
   * Sets the maximum number of hops to explore from the starting vertices.
   *
   * The algorithm will stop after this many iterations even if the stopping condition hasn't been
   * reached for all paths. The default is 3. Be aware that high value of max hops on a dense
   * graph will lead to a huge memory load and potential OOM errors.
   *
   * @param value
   *   positive integer for maximum hop count
   * @return
   *   this AggregateNeighbors instance for method chaining
   */
  def setMaxHops(value: Int): this.type = {
    require(value > 0, "maxHops should be positive.")
    this.maxHops = value
    this
  }

  /**
   * Sets a condition that, when true for a vertex, stops further exploration along that path.
   *
   * The column expression can refer to:
   *   - accumulator columns
   *   - source vertex attributes via `srcAttr("attrName")`
   *   - destination vertex attributes via `dstAttr("attrName")`
   *   - edge attributes via `edgeAttr("attrName")`
   *
   * Either `setStoppingCondition` or `setTargetCondition` must be called. When both are provided
   * only accumulators that reach `targetCondition` are saved to results.
   *
   * @param value
   *   a Boolean Column expression that triggers stopping
   * @return
   *   this AggregateNeighbors instance for method chaining
   */
  def setStoppingCondition(value: Column): this.type = {
    this.stoppingCondition = Some(value)
    this
  }

  /**
   * Sets a condition that, when true for a vertex, marks it as a target and saves the accumulator
   * values.
   *
   * Either `setStoppingCondition` or `setTargetCondition` must be called. When both are provided
   * only accumulators that reach `targetCondition` are saved to results.
   *
   * @param value
   *   a Boolean Column expression that defines a target vertex
   * @return
   *   this AggregateNeighbors instance for method chaining
   */
  def setTargetCondition(value: Column): this.type = {
    this.targetCondition = Some(value)
    this
  }

  /**
   * Defines multiple accumulators used to compute values along each path.
   *
   * Each accumulator has a name, an initial value expression, and an update expression. The
   * update expression is evaluated at each hop and can refer to:
   *   - the accumulator's previous value (using the accumulator's column name)
   *   - source vertex attributes via `srcAttr("attrName")`
   *   - destination vertex attributes via `dstAttr("attrName")`
   *   - edge attributes via `edgeAttr("attrName")`
   *
   * @param names
   *   sequence of accumulator column names
   * @param inits
   *   sequence of Column expressions that give the initial value for each accumulator (evaluated
   *   on starting vertices)
   * @param updates
   *   sequence of Column expressions that define how to update the accumulator when traversing an
   *   edge
   * @return
   *   this AggregateNeighbors instance for method chaining
   */
  def setAccumulators(names: Seq[String], inits: Seq[Column], updates: Seq[Column]): this.type = {
    require(
      inits.size == updates.size && updates.size == names.size,
      "Inits, updates and names must have the same size.")
    this.accumulatorsNames = names
    this.accumulatorsInits = inits
    this.accumulatorsUpdates = updates
    this
  }

  /**
   * Adds a single accumulator to those already configured.
   *
   * This method can be called multiple times to add several accumulators.
   *
   * @param name
   *   accumulator column name
   * @param init
   *   Column expression for the accumulator's initial value
   * @param update
   *   Column expression that updates the accumulator when traversing an edge
   * @return
   *   this AggregateNeighbors instance for method chaining
   */
  def addAccumulator(name: String, init: Column, update: Column): this.type = {
    this.accumulatorsNames :+= name
    this.accumulatorsInits :+= init
    this.accumulatorsUpdates :+= update
    this
  }

  /**
   * Specifies which vertex attributes should be carried through the traversal.
   *
   * By default, all vertex columns are carried. Specifying a subset can improve performance by
   * reducing the amount of data shuffled.
   *
   * @param values
   *   sequence of vertex column names to keep
   * @return
   *   this AggregateNeighbors instance for method chaining
   */
  def setRequiredVertexAttributes(values: Seq[String]): this.type = {
    this.requiredVertexAttributes = values
    this
  }

  /**
   * Specifies which edge attributes should be carried through the traversal.
   *
   * By default, all edge columns are carried. Specifying a subset can improve performance by
   * reducing the amount of data shuffled.
   *
   * @param values
   *   sequence of edge column names to keep
   * @return
   *   this AggregateNeighbors instance for method chaining
   */
  def setRequiredEdgeAttributes(values: Seq[String]): this.type = {
    this.requiredEdgeAttributes = values
    this
  }

  /**
   * Filters which edges can be traversed during the aggregation.
   *
   * Only edges satisfying the given column expression are considered. The default is `true` (all
   * edges are traversable).
   *
   * @param value
   *   a Boolean Column expression that filters edges
   * @return
   *   this AggregateNeighbors instance for method chaining
   */
  def setEdgeFilter(value: Column): this.type = {
    this.edgeFilter = value
    this
  }

  /**
   * Controls whether self-loops (edges where src == dst) are excluded.
   *
   * @param value
   *   if true, self-loop edges are filtered out
   * @return
   *   this AggregateNeighbors instance for method chaining
   */
  def setRemoveLoops(value: Boolean): this.type = {
    this.removeLoops = value
    this
  }

  /**
   * Executes the configured neighbor aggregation and returns the result DataFrame.
   *
   * The result contains one row per (starting vertex, target vertex) pair that satisfied either
   * the stopping condition or the target condition. Columns are:
   *   - `id`: the vertex ID of the target (or stopping) vertex
   *   - `hop`: the number of hops taken to reach it
   *   - one column for each accumulator, holding its final value
   *
   * @return
   *   DataFrame with aggregation results
   */
  def run(): DataFrame = {
    require(maxHops > 0, "maxHops must be greater than 0")
    if (maxHops > 10) {
      logWarn(s"maxHops is very large ($maxHops). This might be performance-intensive.")
    }
    require(accumulatorsNames.nonEmpty, "At least one accumulator must be added")
    require(
      stoppingCondition.orElse(targetCondition).isDefined,
      "Any of target or stopping conditions should be provided")

    val reqAttrs = (if (requiredVertexAttributes.isEmpty) {
                      graph.vertices.columns.toSeq
                    } else {
                      requiredVertexAttributes
                    }).map(col(_))

    val reqEdgeAttr = (if (requiredEdgeAttributes.isEmpty) {
                         graph.edges.columns.toSeq
                       } else {
                         requiredEdgeAttributes
                       }).map(col(_))

    val verticesWithAttributes = graph.vertices.select(
      col(GraphFrame.ID).alias("dst_id"),
      struct(reqAttrs: _*).alias(dstAttributes))

    // "right" side of the join for each iteration
    val edgesBase = graph.edges
    val edgesFiltered = if (removeLoops) {
      edgesBase.filter(col(GraphFrame.SRC) =!= col(GraphFrame.DST))
    } else {
      edgesBase
    }
    val semiTriplets = edgesFiltered
      .select(
        col(GraphFrame.SRC),
        col(GraphFrame.DST),
        struct(reqEdgeAttr: _*).alias(edgeAttributes))
      .join(verticesWithAttributes, col("dst_id") === col(GraphFrame.DST), "left")
      .repartition(col(GraphFrame.SRC)) // to avoid shuffle;
      .persist(intermediateStorageLevel)

    // memory-tracking
    val persistenceQueue = collection.mutable.Queue.empty[DataFrame]

    val statesColumns =
      (accumulatorsNames ++ Seq(
        srcAttributes,
        "src_id",
        currentPathLenColName,
        stoppingCondColName)).map(col(_))
    val finishedColumns =
      (accumulatorsNames ++ Seq("src_id", currentPathLenColName)).map(col(_))

    // holder of the current state of accumulators
    var states: DataFrame = graph.vertices
      .filter(startingVertices)
      .withColumns(accumulatorsNames.zip(accumulatorsInits).toMap)
      .withColumn(srcAttributes, struct(reqAttrs: _*))
      .withColumnRenamed(GraphFrame.ID, "src_id")
      .withColumn(currentPathLenColName, lit(0))
      .withColumn(stoppingCondColName, lit(false))
      .select(statesColumns: _*)
      .persist(intermediateStorageLevel)

    // holder of the finished accumulators
    var finished: DataFrame = states
      .filter(col(stoppingCondColName))
      .select(finishedColumns: _*)
      .withColumnRenamed("src_id", GraphFrame.ID)
      .persist(intermediateStorageLevel)

    var collected = finished.count()

    persistenceQueue.enqueue(states)
    persistenceQueue.enqueue(finished)

    var converged = states.isEmpty
    var iter = 0

    while ((!converged) && (iter < maxHops)) {
      iter += 1
      // get full triplets by joining states (frontier) with semiTriplets
      val fullTriplets =
        states.join(semiTriplets, col("src_id") === col(GraphFrame.SRC)).filter(edgeFilter)

      var colsToSelect = accumulatorsUpdates
        .zip(accumulatorsNames)
        .map(r => r._1.alias(r._2))
        .toSeq

      // Build expressions for stopping and targeting using single Column conditions
      val isTargetExpr = targetCondition.getOrElse(lit(false))
      // We are stopping if any of them are reached
      val shouldStopExpr = stoppingCondition.getOrElse(lit(false)) || isTargetExpr

      colsToSelect = colsToSelect :+ shouldStopExpr.alias(stoppingCondColName)
      colsToSelect = colsToSelect :+ isTargetExpr.alias("_is_target")
      colsToSelect = colsToSelect :+ lit(iter).alias(currentPathLenColName) :+
        col(GraphFrame.DST).alias("src_id") :+
        col(dstAttributes).alias(srcAttributes)
      val updatedStates = fullTriplets.select(colsToSelect: _*)

      var newStates = updatedStates.filter(!col(stoppingCondColName)).select(statesColumns: _*)
      var newFinished = if (targetCondition.isDefined) {
        finished.unionByName(
          updatedStates
            .filter(col("_is_target"))
            .select(finishedColumns: _*)
            .withColumnRenamed("src_id", GraphFrame.ID))
      } else {
        finished.unionByName(
          updatedStates
            .filter(col(stoppingCondColName))
            .select(finishedColumns: _*)
            .withColumnRenamed("src_id", GraphFrame.ID))
      }

      if ((checkpointInterval > 0) && (iter % checkpointInterval == 0)) {
        if (useLocalCheckpoints) {
          newStates = newStates.localCheckpoint()
          newFinished = newFinished.localCheckpoint()
        } else {
          newStates = newStates.checkpoint()
          newFinished = newFinished.checkpoint()
        }
      }

      newStates = newStates.persist(intermediateStorageLevel)
      newFinished = newFinished.persist(intermediateStorageLevel)

      persistenceQueue.enqueue(newStates)
      persistenceQueue.enqueue(newFinished)

      // materialize to unpersist
      collected = newFinished.count()
      converged = newStates.isEmpty

      // unpersist
      persistenceQueue.dequeue().unpersist(true)
      persistenceQueue.dequeue().unpersist(true)

      logInfo(s"iteration $iter, collected $collected rows")

      states = newStates
      finished = newFinished
    }

    persistenceQueue.dequeue().unpersist(true) // clear states
    semiTriplets.unpersist(true) // clear triplets

    resultIsPersistent()
    finished
  }
}

object AggregateNeighbors extends Serializable {
  private val stoppingCondColName: String = "_stopped"
  private val currentPathLenColName: String = "hop"
  private val srcAttributes: String = "src_attributes"
  private val dstAttributes: String = "dst_attributes"
  private val edgeAttributes: String = "edge_attributes"

  /**
   * Creates a column that references a source vertex attribute within accumulator update
   * expressions, stopping conditions, or target conditions.
   *
   * @param name
   *   the name of the source vertex attribute
   * @return
   *   a Column referencing the attribute
   */
  def srcAttr(name: String): Column = col(srcAttributes).getField(name)

  /**
   * Creates a column that references a destination vertex attribute within accumulator update
   * expressions, stopping conditions, or target conditions.
   *
   * @param name
   *   the name of the destination vertex attribute
   * @return
   *   a Column referencing the attribute
   */
  def dstAttr(name: String): Column = col(dstAttributes).getField(name)

  /**
   * Creates a column that references an edge attribute within accumulator update expressions,
   * stopping conditions, or target conditions.
   *
   * @param name
   *   the name of the edge attribute
   * @return
   *   a Column referencing the attribute
   */
  def edgeAttr(name: String): Column = col(edgeAttributes).getField(name)
}
