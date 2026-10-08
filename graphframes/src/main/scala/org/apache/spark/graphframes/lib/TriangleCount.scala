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
import org.apache.spark.graphframes.GraphFramesSparkVersionException
import org.apache.spark.graphframes.Logging
import org.apache.spark.graphframes.WithIntermediateStorageLevel
import org.apache.spark.graphframes.WithLgNomEntries
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
 * Triangle count implementation.
 *
 * This class provides two algorithms for counting triangles:
 *   - A direct version that computes exact triangle counts using set intersection of neighbor
 *     lists.
 *   - An approximate version based on the DataSketches library (Theta sketches), which trades off
 *     accuracy for performance on large-scale graphs.
 *
 * The output DataFrame contains two columns:
 *   - "id": the vertex id
 *   - "count": the number of triangles passing through the vertex
 */
class TriangleCount private[graphframes] (private val graph: GraphFrame)
    extends Arguments
    with Serializable
    with WithIntermediateStorageLevel
    with WithLgNomEntries {

  private var algorithm: String = "exact"
  private val supportedAlgorithms: Set[String] = Set("exact", "approx")

  private def supportedAlgorithmsRepr: String = supportedAlgorithms.mkString(", ")

  /**
   * Sets the triangle counting algorithm. Options are "exact" (default) or "approx".
   */
  def setAlgorithm(value: String): this.type = {
    require(
      supportedAlgorithms.contains(value),
      s"supported algorithms: ${supportedAlgorithmsRepr}")
    algorithm = value
    this
  }

  def run(): DataFrame = {
    if (algorithm == "exact") {
      TriangleCount.run(graph, intermediateStorageLevel)
    } else {
      TriangleCount.approximateRun(graph, intermediateStorageLevel, lgNomEntries)
    }
  }
}

private object TriangleCount extends Logging {
  import org.apache.spark.graphframes.GraphFrame._

  private def prepareGraph(graph: GraphFrame): GraphFrame = {
    // Dedup edges by flipping them to have SRC < DST
    // Remove self-loops
    val dedupedE = graph.edges
      .filter(col(SRC) =!= col(DST))
      .select(
        when(col(SRC) < col(DST), col(SRC)).otherwise(col(DST)).as(SRC),
        when(col(SRC) < col(DST), col(DST)).otherwise(col(SRC)).as(DST))
      .distinct()

    // Prepare the graph with no isolated vertices.
    GraphFrame(graph.vertices.select(ID), dedupedE).dropIsolatedVertices()
  }

  private def approximateRun(
      graph: GraphFrame,
      intermediateStorageLevel: StorageLevel,
      lgNomEntries: Int): DataFrame = {
    val spark = graph.vertices.sparkSession
    val sparkVersion = spark.version

    if (sparkVersion.substring(0, 3) < "4.1") {
      throw new GraphFramesSparkVersionException("4.1.0")
    }

    val thetaSketchAgg = (colName: String) => expr(s"theta_sketch_agg($colName, $lgNomEntries)")
    val thetaSketchIntersect = (colLeft: String, colRight: String) =>
      expr(s"theta_sketch_estimate(theta_intersection($colLeft, $colRight))")

    val g2 = prepareGraph(graph)

    val verticesWithNeighbors = g2.aggregateMessages
      .setIntermediateStorageLevel(intermediateStorageLevel)
      .sendToSrc(AggregateMessages.dst(ID))
      .sendToDst(AggregateMessages.src(ID))
      .agg(thetaSketchAgg(AggregateMessages.MSG_COL_NAME).alias("neighbors"))
      .persist(intermediateStorageLevel)

    val triangles = verticesWithNeighbors
      .select(col(ID), col("neighbors").alias("src_set"))
      .join(g2.edges, col(ID) === col(SRC))
      .drop(ID)
      .join(
        verticesWithNeighbors.select(col(ID), col("neighbors").alias("dst_set")),
        col(ID) === col(DST))
      .drop(ID)
      // Count of common neighbors of SRC and DST
      .withColumn("triplets", thetaSketchIntersect("src_set", "dst_set"))
      .filter(col("triplets") > lit(0))
      .persist(intermediateStorageLevel)

    val srcTriangles = triangles.groupBy(SRC).agg(sum(col("triplets")).alias("src_triplets"))
    val dstTriangles = triangles.groupBy(DST).agg(sum(col("triplets")).alias("dst_triplets"))

    val result = graph.vertices
      .join(srcTriangles, col(ID) === col(SRC), "left_outer")
      .join(dstTriangles, col(ID) === col(DST), "left_outer")
      // Each triangle counted twice, so divide by 2.
      .withColumn(
        COUNT_ID,
        floor(
          (coalesce(col("src_triplets"), lit(0)) + coalesce(col("dst_triplets"), lit(0))) / lit(
            2)))
      .select(col(ID), col(COUNT_ID))

    result.persist(intermediateStorageLevel)
    result.count()
    verticesWithNeighbors.unpersist()
    triangles.unpersist()
    resultIsPersistent()
    result
  }

  private def run(graph: GraphFrame, intermediateStorageLevel: StorageLevel): DataFrame = {
    val g2 = prepareGraph(graph)

    val verticesWithNeighbors = g2.aggregateMessages
      .setIntermediateStorageLevel(intermediateStorageLevel)
      .sendToSrc(AggregateMessages.dst(ID))
      .sendToDst(AggregateMessages.src(ID))
      .agg(collect_set(AggregateMessages.msg).alias("neighbors"))
      .persist(intermediateStorageLevel)

    val triangles = verticesWithNeighbors
      .select(col(ID), col("neighbors").alias("src_set"))
      .join(g2.edges, col(ID) === col(SRC))
      .drop(ID)
      .join(
        verticesWithNeighbors.select(col(ID), col("neighbors").alias("dst_set")),
        col(ID) === col(DST))
      .drop(ID)
      // Count of common neighbors of SRC and DST
      .withColumn("triplets", array_size(array_intersect(col("src_set"), col("dst_set"))))
      .filter(col("triplets") > lit(0))
      .persist(intermediateStorageLevel)

    val srcTriangles = triangles.groupBy(SRC).agg(sum(col("triplets")).alias("src_triplets"))
    val dstTriangles = triangles.groupBy(DST).agg(sum(col("triplets")).alias("dst_triplets"))

    val result = graph.vertices
      .join(srcTriangles, col(ID) === col(SRC), "left_outer")
      .join(dstTriangles, col(ID) === col(DST), "left_outer")
      // Each triangle counted twice, so divide by 2.
      .withColumn(
        COUNT_ID,
        floor(
          (coalesce(col("src_triplets"), lit(0)) + coalesce(col("dst_triplets"), lit(0))) / lit(
            2)))

    result.persist(intermediateStorageLevel)
    result.count()
    verticesWithNeighbors.unpersist()
    triangles.unpersist()
    resultIsPersistent()
    result
  }

  private val COUNT_ID = "count"
}
