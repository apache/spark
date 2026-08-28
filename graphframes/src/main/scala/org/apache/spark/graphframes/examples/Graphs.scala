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

package org.apache.spark.graphframes.examples

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.graphframes.GraphFrame
import org.apache.spark.graphframes.GraphFrame._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.randn
import org.apache.spark.sql.functions.udf

class Graphs private[graphframes] () {
  // Note: this cannot be values: we are creating and destroying spark contexts during the tests,
  // and turning these into vals means we would hold onto a potentially destroyed spark context.
  private def spark: SparkSession = SparkSession.builder().getOrCreate()

  /**
   * Returns an empty GraphFrame of the given ID type.
   */
  def empty[T: TypeTag]: GraphFrame = {
    val _spark = spark
    import _spark.implicits._
    val vertices = Seq.empty[Tuple1[T]].toDF(ID)
    val edges = Seq.empty[(T, T)].toDF(SRC, DST)
    GraphFrame(vertices, edges)
  }

  /**
   * Returns a chain graph of the given size with Long ID type. The vertex IDs are 0, 1, ..., n-1,
   * and the edges are (0, 1), (1, 2), ...., (n-2, n-1).
   */
  def chain(n: Long): GraphFrame = {
    require(n >= 0, s"Chain graph size must be nonnegative but got $n.")
    val vertices = spark.range(n).toDF(ID)
    val edges = spark
      .range(n - 1L)
      .toDF(ID)
      .select(col(ID).as(SRC), (col(ID) + 1L).as(DST))
    GraphFrame(vertices, edges)
  }

  /**
   * Graph of friends in a social network.
   */
  def friends: GraphFrame = {
    // For the same reason as above, this cannot be a value.
    // Vertex DataFrame
    val v = spark
      .createDataFrame(
        List(
          ("a", "Alice", 34),
          ("b", "Bob", 36),
          ("c", "Charlie", 30),
          ("d", "David", 29),
          ("e", "Esther", 32),
          ("f", "Fanny", 36),
          ("g", "Gabby", 60)))
      .toDF("id", "name", "age")
    // Edge DataFrame
    val e = spark
      .createDataFrame(
        List(
          ("a", "b", "friend"),
          ("b", "c", "follow"),
          ("c", "b", "follow"),
          ("f", "c", "follow"),
          ("e", "f", "follow"),
          ("e", "d", "friend"),
          ("d", "a", "friend"),
          ("a", "e", "friend")))
      .toDF("src", "dst", "relationship")
    // Create a GraphFrame
    GraphFrame(v, e)
  }

  /**
   * Two densely connected blobs (vertices 0->n-1 and n->2n-1) connected by a single edge (0->n)
   * @param blobSize
   *   the size of each blob.
   * @return
   */
  def twoBlobs(blobSize: Int): GraphFrame = {
    val n = blobSize
    val edges1 = for (v1 <- 0 until n; v2 <- 0 until n) yield (v1.toLong, v2.toLong, s"$v1-$v2")
    val edges2 = for {
      v1 <- n until (2 * n)
      v2 <- n until (2 * n)
    } yield (v1.toLong, v2.toLong, s"$v1-$v2")
    val edges = edges1 ++ edges2 ++ Seq((0L, n.toLong, s"0-$n"))
    val vertices = (0 until (2 * n)).map { v => (v.toLong, s"$v", v) }
    val e = spark.createDataFrame(edges).toDF("src", "dst", "e_attr1")
    val v = spark.createDataFrame(vertices).toDF("id", "v_attr1", "v_attr2")
    GraphFrame(v, e)
  }

  /**
   * Returns a star graph with Long ID type, consisting of a central element indexed 0 (the root)
   * and the n other leaf vertices 1, 2, ..., n.
   * @param n
   *   the number of leaves
   */
  def star(n: Long): GraphFrame = {
    require(n >= 0L)
    val vertices = spark.range(n + 1L).toDF(ID)
    val edges = spark.range(1L, n + 1L).toDF(DST).withColumn(SRC, lit(0L))
    GraphFrame(vertices, edges)
  }

  /**
   * Some synthetic data that sits in Spark.
   *
   * No description available.
   * @return
   */
  def ALSSyntheticData(): GraphFrame = {
    val sc = spark.sparkContext
    val data = sc.parallelize(als_data.toIndexedSeq).map { line =>
      val fields = line.split(",")
      (fields(0).toLong * 2, fields(1).toLong * 2 + 1, fields(2).toDouble)
    }
    val edges = spark.createDataFrame(data).toDF("src", "dst", "weight")
    val vs =
      data.flatMap(r => r._1 :: r._2 :: Nil).collect().distinct.map(x => Tuple1(x)).toIndexedSeq
    val vertices = spark.createDataFrame(vs).toDF("id")
    GraphFrame(vertices, edges)
  }

  private lazy val als_data =
    """
      |1,1,5.0
      |1,2,1.0
      |1,3,5.0
      |1,4,1.0
      |2,1,5.0
      |2,2,1.0
      |2,3,5.0
      |2,4,1.0
      |3,1,1.0
      |3,2,5.0
      |3,3,1.0
      |3,4,5.0
      |4,1,1.0
      |4,2,5.0
      |4,3,1.0
      |4,4,5.0
    """.stripMargin.split("\n").map(_.trim).filterNot(_.isEmpty)

  /**
   * This method generates a grid Ising model with random parameters.
   *
   * Ising models are probabilistic graphical models over binary variables x,,i,,. Each binary
   * variable x,,i,, corresponds to one vertex, and it may take values -1 or +1. The probability
   * distribution P(X) (over all x,,i,,) is parameterized by vertex factors a,,i,, and edge
   * factors b,,ij,,:
   * {{{
   *  P(X) = (1/Z) * exp[ \sum_i a_i x_i + \sum_{ij} b_{ij} x_i x_j ]
   * }}}
   * where Z is the normalization constant (partition function). See
   * [[https://en.wikipedia.org/wiki/Ising_model Wikipedia]] for more information on Ising models.
   *
   * Each vertex is parameterized by a single scalar a,,i,,. Each edge is parameterized by a
   * single scalar b,,ij,,.
   *
   * @param n
   *   Length of one side of the grid. The grid will be of size n x n.
   * @param vStd
   *   Standard deviation of normal distribution used to generate vertex factors "a". Default of
   *   1.0.
   * @param eStd
   *   Standard deviation of normal distribution used to generate edge factors "b". Default of
   *   1.0.
   * @return
   *   GraphFrame. Vertices have columns "id" and "a". Edges have columns "src", "dst", and "b".
   *   Edges are directed, but they should be treated as undirected in any algorithms run on this
   *   model. Vertex IDs are of the form "i,j". E.g., vertex "1,3" is in the second row and fourth
   *   column of the grid.
   */
  def gridIsingModel(spark: SparkSession, n: Int, vStd: Double, eStd: Double): GraphFrame = {
    require(n >= 1, s"Grid graph must have size >= 1, but was given invalid value n = $n")

    // To create grid
    // Avoid Cartesian join due to SPARK-15425: use generator since n should be small
    val res = for { x <- Range(0, n); y <- Range(0, n) } yield (x, y)
    val coordinates = spark.createDataFrame(res).toDF("i", "j")

    // Create SQL expression for converting coordinates (i,j) to a string ID "i,j"
    val toIDudf = udf { (i: Int, j: Int) => i.toString + "," + j.toString }

    // Create the vertex DataFrame
    //  Create SQL expression for converting coordinates (i,j) to a string ID "i,j"
    val vIDcol = toIDudf(col("i"), col("j"))
    //  Add random parameters generated from a normal distribution
    val seed = 12345
    val vertices = coordinates
      .withColumn("id", vIDcol) // vertex IDs "i,j"
      .withColumn("a", randn(seed.toLong) * vStd) // Ising parameter for vertex

    // Create the edge DataFrame
    //  Create SQL expression for converting coordinates (i,j+1) and (i+1,j) to string IDs
    val rightIDcol = toIDudf(col("i"), col("j") + 1)
    val downIDcol = toIDudf(col("i") + 1, col("j"))
    val horizontalEdges = coordinates
      .filter(col("j") =!= n - 1)
      .select(vIDcol.as("src"), rightIDcol.as("dst"))
    val verticalEdges = coordinates
      .filter(col("i") =!= n - 1)
      .select(vIDcol.as("src"), downIDcol.as("dst"))
    val allEdges = horizontalEdges.union(verticalEdges)
    //  Add random parameters from a normal distribution
    val edges =
      allEdges.withColumn("b", randn(seed.toLong + 1L) * eStd) // Ising parameter for edge

    // Create the GraphFrame
    val g = GraphFrame(vertices, edges)

    // Materialize graph as workaround for SPARK-13333
    g.vertices.cache().count()
    g.edges.cache().count()

    g
  }

  /** Version of `gridIsingModel` with vStd, eStd set to 1.0. */
  def gridIsingModel(spark: SparkSession, n: Int): GraphFrame =
    gridIsingModel(spark, n, 1.0, 1.0)

}

/** Example GraphFrames for testing the API */
object Graphs extends Graphs
