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

package org.apache.spark.ml.clustering

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.clustering.{PowerIterationClustering => MLlibPowerIterationClustering}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types._

/**
 * Common params for PowerIterationClustering
 */
private[clustering] trait PowerIterationClusteringParams extends Params with HasMaxIter
  with HasWeightCol {

  /**
   * The number of clusters to create (k). Must be &gt; 1. Default: 2.
   * @group param
   */
  @Since("2.4.0")
  final val k = new IntParam(this, "k", "The number of clusters to create. " +
    "Must be > 1.", ParamValidators.gt(1))

  /** @group getParam */
  @Since("2.4.0")
  def getK: Int = $(k)

  /**
   * Param for the initialization algorithm. This can be either "random" to use a random vector
   * as vertex properties, or "degree" to use a normalized sum of similarities with other vertices.
   * Default: random.
   * @group expertParam
   */
  @Since("2.4.0")
  final val initMode = {
    val allowedParams = ParamValidators.inArray(Array("random", "degree"))
    new Param[String](this, "initMode", "The initialization algorithm. This can be either " +
      "'random' to use a random vector as vertex properties, or 'degree' to use a normalized sum " +
      "of similarities with other vertices.  Supported options: 'random' and 'degree'.",
      allowedParams)
  }

  /** @group expertGetParam */
  @Since("2.4.0")
  def getInitMode: String = $(initMode)

  /**
   * Param for the name of the input column for source vertex IDs.
   * Default: "src"
   * @group param
   */
  @Since("2.4.0")
  val srcCol = new Param[String](this, "srcCol", "Name of the input column for source vertex IDs.",
    (value: String) => value.nonEmpty)

  /** @group getParam */
  @Since("2.4.0")
  def getSrcCol: String = getOrDefault(srcCol)

  /**
   * Name of the input column for destination vertex IDs.
   * Default: "dst"
   * @group param
   */
  @Since("2.4.0")
  val dstCol = new Param[String](this, "dstCol",
    "Name of the input column for destination vertex IDs.",
    (value: String) => value.nonEmpty)

  /** @group getParam */
  @Since("2.4.0")
  def getDstCol: String = $(dstCol)

  setDefault(srcCol -> "src", dstCol -> "dst")
}

/**
 * :: Experimental ::
 * Power Iteration Clustering (PIC), a scalable graph clustering algorithm developed by
 * <a href=http://www.cs.cmu.edu/~frank/papers/icml2010-pic-final.pdf>Lin and Cohen</a>. From
 * the abstract: PIC finds a very low-dimensional embedding of a dataset using truncated power
 * iteration on a normalized pair-wise similarity matrix of the data.
 *
 * This class is not yet an Estimator/Transformer, use `assignClusters` method to run the
 * PowerIterationClustering algorithm.
 *
 * @see <a href=http://en.wikipedia.org/wiki/Spectral_clustering>
 * Spectral clustering (Wikipedia)</a>
 */
@Since("2.4.0")
@Experimental
class PowerIterationClustering private[clustering] (
    @Since("2.4.0") override val uid: String)
  extends PowerIterationClusteringParams with DefaultParamsWritable {

  setDefault(
    k -> 2,
    maxIter -> 20,
    initMode -> "random")

  @Since("2.4.0")
  def this() = this(Identifiable.randomUID("PowerIterationClustering"))

  /** @group setParam */
  @Since("2.4.0")
  def setK(value: Int): this.type = set(k, value)

  /** @group expertSetParam */
  @Since("2.4.0")
  def setInitMode(value: String): this.type = set(initMode, value)

  /** @group setParam */
  @Since("2.4.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /** @group setParam */
  @Since("2.4.0")
  def setSrcCol(value: String): this.type = set(srcCol, value)

  /** @group setParam */
  @Since("2.4.0")
  def setDstCol(value: String): this.type = set(dstCol, value)

  /** @group setParam */
  @Since("2.4.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)

  /**
   * Run the PIC algorithm and returns a cluster assignment for each input vertex.
   *
   * @param dataset A dataset with columns src, dst, weight representing the affinity matrix,
   *                which is the matrix A in the PIC paper. Suppose the src column value is i,
   *                the dst column value is j, the weight column value is similarity s,,ij,,
   *                which must be nonnegative. This is a symmetric matrix and hence
   *                s,,ij,, = s,,ji,,. For any (i, j) with nonzero similarity, there should be
   *                either (i, j, s,,ij,,) or (j, i, s,,ji,,) in the input. Rows with i = j are
   *                ignored, because we assume s,,ij,, = 0.0.
   *
   * @return A dataset that contains columns of vertex id and the corresponding cluster for the id.
   *         The schema of it will be:
   *          - id: Long
   *          - cluster: Int
   */
  @Since("2.4.0")
  def assignClusters(dataset: Dataset[_]): DataFrame = {
    val w = if (!isDefined(weightCol) || $(weightCol).isEmpty) {
      lit(1.0)
    } else {
      SchemaUtils.checkNumericType(dataset.schema, $(weightCol))
      col($(weightCol)).cast(DoubleType)
    }

    SchemaUtils.checkColumnTypes(dataset.schema, $(srcCol), Seq(IntegerType, LongType))
    SchemaUtils.checkColumnTypes(dataset.schema, $(dstCol), Seq(IntegerType, LongType))
    val rdd: RDD[(Long, Long, Double)] = dataset.select(
      col($(srcCol)).cast(LongType),
      col($(dstCol)).cast(LongType),
      w).rdd.map {
      case Row(src: Long, dst: Long, weight: Double) => (src, dst, weight)
    }
    val algorithm = new MLlibPowerIterationClustering()
      .setK($(k))
      .setInitializationMode($(initMode))
      .setMaxIterations($(maxIter))
    val model = algorithm.run(rdd)

    import dataset.sparkSession.implicits._
    model.assignments.toDF
  }

  @Since("2.4.0")
  override def copy(extra: ParamMap): PowerIterationClustering = defaultCopy(extra)
}

@Since("2.4.0")
object PowerIterationClustering extends DefaultParamsReadable[PowerIterationClustering] {

  @Since("2.4.0")
  override def load(path: String): PowerIterationClustering = super.load(path)
}
