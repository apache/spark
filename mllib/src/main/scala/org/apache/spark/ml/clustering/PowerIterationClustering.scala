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
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.clustering.{PowerIterationClustering => MLlibPowerIterationClustering}
import org.apache.spark.mllib.clustering.PowerIterationClustering.Assignment
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}

/**
 * Common params for PowerIterationClustering
 */
private[clustering] trait PowerIterationClusteringParams extends Params with HasMaxIter
  with HasFeaturesCol with HasPredictionCol with HasWeightCol {

  /**
   * The number of clusters to create (k). Must be &gt; 1. Default: 2.
   * @group param
   */
  @Since("2.3.0")
  final val k = new IntParam(this, "k", "The number of clusters to create. " +
    "Must be > 1.", ParamValidators.gt(1))

  /** @group getParam */
  @Since("2.3.0")
  def getK: Int = $(k)

  /**
   * Param for the initialization algorithm. This can be either "random" to use a random vector
   * as vertex properties, or "degree" to use normalized sum of similarities with other vertices.
   * Default: random.
   * @group expertParam
   */
  @Since("2.3.0")
  final val initMode = {
    val allowedParams = ParamValidators.inArray(Array("random", "degree"))
    new Param[String](this, "initMode", "The initialization algorithm. This can be either " +
      "'random' to use a random vector as vertex properties, or 'degree' to use normalized sum " +
      "of similarities with other vertices.  Supported options: 'random' and 'degree'.",
      allowedParams)
  }

  /** @group expertGetParam */
  @Since("2.3.0")
  def getInitMode: String = $(initMode)

  /**
   * Param for the name of the input column for vertex IDs.
   * Default: "id"
   * @group param
   */
  @Since("2.3.0")
  val idCol = new Param[String](this, "idCol", "Name of the input column for vertex IDs.")

  /** @group getParam */
  @Since("2.3.0")
  def getIdCol: String = $(idCol)

  /**
   * Param for the name of the input column for neighbors in the adjacency list representation.
   * Default: "neighbors"
   * @group param
   */
  @Since("2.3.0")
  val neighborsCol = new Param[String](this, "neighborsCol",
    "Name of the input column for neighbors in the adjacency list representation.")

  /** @group getParam */
  @Since("2.3.0")
  def getNeighborsCol: String = $(neighborsCol)

  /**
   * Validates the input schema
   * @param schema input schema
   */
  protected def validateSchema(schema: StructType): Unit = {
    SchemaUtils.checkColumnType(schema, $(idCol), LongType)
    SchemaUtils.checkColumnType(schema, $(predictionCol), IntegerType)
  }
}

/**
 * :: Experimental ::
 * Power Iteration Clustering (PIC), a scalable graph clustering algorithm developed by
 * <a href=http://www.icml2010.org/papers/387.pdf>Lin and Cohen</a>. From the abstract:
 * PIC finds a very low-dimensional embedding of a dataset using truncated power
 * iteration on a normalized pair-wise similarity matrix of the data.
 *
 * PIC takes an affinity matrix between items (or vertices) as input.  An affinity matrix
 * is a symmetric matrix whose entries are non-negative similarities between items.
 * PIC takes this matrix (or graph) as an adjacency matrix.  Specifically, each input row includes:
 *  - `idCol`: vertex ID
 *  - `neighborsCol`: neighbors of vertex in `idCol`
 *  - `neighborWeightsCol`: non-negative weights of edges between the vertex in `idCol` and
 *                          each neighbor in `neighborsCol`
 * PIC returns a cluster assignment for each input vertex.  It appends a new column `predictionCol`
 * containing the cluster assignment in `[0,k)` for each row (vertex).
 *
 * Notes:
 *  - [[PowerIterationClustering]] is a transformer with an expensive [[transform]] operation.
 *    Transform runs the iterative PIC algorithm to cluster the whole input dataset.
 *  - Input validation: This validates that weights are non-negative but does NOT validate
 *    that the input matrix is symmetric.
 *
 * @see <a href=http://en.wikipedia.org/wiki/Spectral_clustering>
 * Spectral clustering (Wikipedia)</a>
 */
@Since("2.3.0")
@Experimental
class PowerIterationClustering private[clustering] (
    @Since("2.3.0") override val uid: String)
  extends Transformer with PowerIterationClusteringParams with DefaultParamsWritable {

  setDefault(
    k -> 2,
    maxIter -> 20,
    initMode -> "random",
    idCol -> "id",
    weightCol -> "weight",
    neighborsCol -> "neighbors")

  @Since("2.3.0")
  override def copy(extra: ParamMap): PowerIterationClustering = defaultCopy(extra)

  @Since("2.3.0")
  def this() = this(Identifiable.randomUID("PowerIterationClustering"))

  /** @group setParam */
  @Since("2.3.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("2.3.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("2.3.0")
  def setK(value: Int): this.type = set(k, value)

  /** @group expertSetParam */
  @Since("2.3.0")
  def setInitMode(value: String): this.type = set(initMode, value)

  /** @group setParam */
  @Since("2.3.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /** @group setParam */
  @Since("2.3.0")
  def setIdCol(value: String): this.type = set(idCol, value)

  /**
   * Sets the value of param [[weightCol]].
   * Default is "weight"
   *
   * @group setParam
   */
  @Since("2.3.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)

  /**
   * Sets the value of param [[neighborsCol]].
   * Default is "neighbors"
   *
   * @group setParam
   */
  @Since("2.3.0")
  def setNeighborsCol(value: String): this.type = set(neighborsCol, value)

  @Since("2.3.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val sparkSession = dataset.sparkSession
    val rdd: RDD[(Long, Long, Double)] =
      dataset.select(col($(idCol)), col($(neighborsCol)), col($(weightCol))).rdd.flatMap {
        case Row(id: Long, nbr: Vector, weight: Vector) =>
          require(nbr.size == weight.size,
            "The length of neighbor list must be equal to the the length of the weight list.")
          nbr.toArray.toIterator.zip(weight.toArray.toIterator)
            .map(x => (id, x._1.toLong, x._2))
      }
    val algorithm = new MLlibPowerIterationClustering()
      .setK($(k))
      .setInitializationMode($(initMode))
      .setMaxIterations($(maxIter))
    val model = algorithm.run(rdd)

    val rows: RDD[Row] = model.assignments.map {
      case assignment: Assignment => Row(assignment.id, assignment.cluster)
    }

    val schema = transformSchema(new StructType(Array(StructField($(idCol), LongType),
      StructField($(predictionCol), IntegerType))))
    val result = sparkSession.createDataFrame(rows, schema)

    dataset.join(result, "id")
  }

  @Since("2.3.0")
  override def transformSchema(schema: StructType): StructType = {
    validateSchema(schema)
    schema
  }

}

@Since("2.3.0")
object PowerIterationClustering extends DefaultParamsReadable[PowerIterationClustering] {

  @Since("2.3.0")
  override def load(path: String): PowerIterationClustering = super.load(path)
}

