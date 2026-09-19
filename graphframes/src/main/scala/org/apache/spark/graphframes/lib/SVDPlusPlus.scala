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
import org.apache.spark.graphframes.InvalidGraphException
import org.apache.spark.graphframes.Logging
import org.apache.spark.graphframes.WithMaxIter
import org.apache.spark.graphx.{lib => graphxlib}
import org.apache.spark.graphx.Edge
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/**
 * Arguments for SVD++ algorithm.
 *
 * This class implements the SVD++ algorithm for Collaborative Filtering, primarily used for
 * Recommender Systems (Link Prediction).
 *
 * Based on Yehuda Koren's paper
 * <a href="https://dl.acm.org/citation.cfm?id=1401944">"Factorization Meets the Neighborhood: a
 * Multifaceted Collaborative Filtering Model"</a> (2008).
 *
 * ==Problem Definition==
 * The algorithm predicts unknown ratings in a user-item system. It accounts for:
 *   - Explicit preferences (user ratings).
 *   - Implicit feedback (the history of items a user has interacted with).
 *   - User and Item biases.
 *
 * The prediction rule for a rating `r_ui` (user `u`, item `i`) is:
 * {{{
 * r_ui = mu + b_u + b_i + q_i^T * (p_u + |N(u)|^-0.5 * sum(y_j for j in N(u)))
 * }}}
 * Where `N(u)` is the set of items user `u` has interacted with (implicit feedback).
 *
 * ==Input Requirements==
 * !!! IMPORTANT !!! The input graph MUST be a **Directed Bipartite Graph** representing
 * interactions:
 *   - **Vertices**: A mix of Users and Items.
 *   - **Edges**: Directed strictly from **User (src) -> Item (dst)**.
 *   - **Edge Attribute**: A numeric column (default "weight") representing the rating.
 *
 * DO NOT use this on general/undirected graphs (e.g., social networks), as the algorithm relies
 * on the asymmetry between Users (who provide feedback) and Items (who receive it).
 *
 * ==Output Model (Node Embeddings)==
 * The algorithm returns a DataFrame of vertices with the trained model parameters. These
 * parameters function as embeddings:
 *
 *   - `column1` (Array[Double]): **Primary Latent Factors (Explicit Embedding)**.
 *   - For Users: Represents preferences (`p_u`).
 *   - For Items: Represents characteristics (`q_i`).
 *   - `column2` (Array[Double]): **Implicit Factors (Implicit Embedding)**.
 *   - For Items: Represents the influence of the item (`y_i`) on a user's profile based on
 *     viewing history.
 *   - For Users: Generally unused/zero.
 *   - `column3` (Double): **Bias**.
 *   - For Users: User bias (`b_u`).
 *   - For Items: Item bias (`b_i`).
 *   - `column4` (Double): **Implicit Normalization Term**.
 *   - For Users: Precomputed `|N(u)|^-0.5`.
 *   - For Items: Unused.
 *
 * ==Parameter Tuning Guide==
 *
 * Constraints:
 *   - `minValue` / `maxValue`: Hard bounds for predicted ratings. Predictions outside this range
 *     are clipped. Set these to your rating scale limits (e.g., 1.0 and 5.0).
 *
 * Learning Rates (Step sizes for Gradient Descent):
 *   - `gamma1`: Learning rate for **Biases** (`b_u`, `b_i`).
 *   - `gamma2`: Learning rate for **Embeddings/Factors** (`p_u`, `q_i`, `y_j`). > Tip: Increase
 *     if convergence is too slow. Decrease if the loss explodes (NaN).
 *
 * Regularization (Preventing Overfitting):
 *   - `gamma6`: Regularization for **Biases**.
 *   - `gamma7`: Regularization for **Embeddings/Factors**. > Tip: Increase these if the model
 *     performs well on training data but poorly on test data.
 */
class SVDPlusPlus private[graphframes] (private val graph: GraphFrame)
    extends Arguments
    with WithMaxIter
    with Logging {
  private var _rank: Int = 10
  private var _minVal: Double = 0.0
  private var _maxVal: Double = 5.0
  private var _gamma1: Double = 0.007
  private var _gamma2: Double = 0.007
  private var _gamma6: Double = 0.005
  private var _gamma7: Double = 0.015

  private var _loss: Option[Double] = None

  def rank(value: Int): this.type = {
    _rank = value
    this
  }

  def minValue(value: Double): this.type = {
    _minVal = value
    this
  }

  def maxValue(value: Double): this.type = {
    _maxVal = value
    this
  }

  def gamma1(value: Double): this.type = {
    _gamma1 = value
    this
  }

  def gamma2(value: Double): this.type = {
    _gamma2 = value
    this
  }

  def gamma6(value: Double): this.type = {
    _gamma6 = value
    this
  }

  def gamma7(value: Double): this.type = {
    _gamma7 = value
    this
  }

  def run(): DataFrame = {
    import SVDPlusPlus.COLUMN_WEIGHT

    if (!graph.edges.columns.contains(COLUMN_WEIGHT)) {
      throw new InvalidGraphException(s"SVD++ requires a weight column $COLUMN_WEIGHT")
    }
    val conf = new graphxlib.SVDPlusPlus.Conf(
      rank = _rank,
      maxIters = maxIter.getOrElse(2),
      minVal = _minVal,
      maxVal = _maxVal,
      gamma1 = _gamma1,
      gamma2 = _gamma2,
      gamma6 = _gamma6,
      gamma7 = _gamma7)

    val g = if (graph.hasIntegralIdType) {
      graph
    } else {
      val iVertices = graph.indexedVertices
      val iEdges = graph.indexedEdges.select(
        col(GraphFrame.LONG_SRC).alias(GraphFrame.SRC),
        col(GraphFrame.LONG_DST).alias(GraphFrame.DST),
        col(GraphFrame.ATTR).getField(COLUMN_WEIGHT).alias(COLUMN_WEIGHT))

      GraphFrame(iVertices, iEdges)
    }

    val (df, l) = SVDPlusPlus.run(g, conf)
    val result = if (graph.hasIntegralIdType) {
      df.persist()
    } else {
      val iV = graph.indexedVertices
      df.withColumnRenamed(GraphFrame.ID, GraphFrame.LONG_ID)
        .join(iV, GraphFrame.LONG_ID)
        .drop(GraphFrame.LONG_ID)
        .persist()
    }
    _loss = Some(l)

    // materialize
    result.count()

    // unpersist
    df.unpersist()
    resultIsPersistent()
    result
  }

  def loss: Double = {
    // We could use types instead to make sure that it is never accessed before being run.
    _loss.getOrElse(throw new Exception("The algorithm has not been run yet"))
  }
}

object SVDPlusPlus {

  private def run(graph: GraphFrame, conf: graphxlib.SVDPlusPlus.Conf): (DataFrame, Double) = {
    val edges = graph.edges.select(GraphFrame.SRC, GraphFrame.DST, COLUMN_WEIGHT).rdd.map { row =>
      val src = row.getAs[Number](0).longValue()
      val dst = row.getAs[Number](1).longValue()
      val w = row.getAs[Number](2).doubleValue()
      Edge(src, dst, w)
    }
    val (gx, res) = graphxlib.SVDPlusPlus.run(edges, conf)
    val gf = GraphXConversions.fromGraphX(
      graph,
      gx,
      vertexNames = Seq(COLUMN1, COLUMN2, COLUMN3, COLUMN4))
    val vertices = gf.vertices.persist()
    vertices.count()
    gx.unpersist()
    (vertices, res)
  }

  /**
   * Name for input edge DataFrame column containing edge weights.
   *
   * Note: This column name may change in the future!
   */
  val COLUMN_WEIGHT = "weight"

  /**
   * Name for output vertexDataFrame column containing first parameter of learned model, of type
   * `Array[Double]`.
   *
   * Note: This column name may change in the future!
   */
  val COLUMN1 = "column1"

  /**
   * Name for output vertexDataFrame column containing second parameter of learned model, of type
   * `Array[Double]`.
   *
   * Note: This column name may change in the future!
   */
  val COLUMN2 = "column2"

  /**
   * Name for output vertexDataFrame column containing third parameter of learned model, of type
   * `Double`.
   *
   * Note: This column name may change in the future!
   */
  val COLUMN3 = "column3"

  /**
   * Name for output vertexDataFrame column containing fourth parameter of learned model, of type
   * `Double`.
   *
   * Note: This column name may change in the future!
   */
  val COLUMN4 = "column4"
}
