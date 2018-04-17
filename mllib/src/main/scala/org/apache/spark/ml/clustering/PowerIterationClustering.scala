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
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.clustering.{PowerIterationClustering => MLlibPowerIterationClustering}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

/**
 * Common params for PowerIterationClustering
 */
private[clustering] trait PowerIterationClusteringParams extends Params with HasMaxIter
  with HasPredictionCol {

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
   * Param for the name of the input column for vertex IDs.
   * Default: "id"
   * @group param
   */
  @Since("2.4.0")
  val idCol = new Param[String](this, "idCol", "Name of the input column for vertex IDs.",
    (value: String) => value.nonEmpty)

  setDefault(idCol, "id")

  /** @group getParam */
  @Since("2.4.0")
  def getIdCol: String = getOrDefault(idCol)

  /**
   * Param for the name of the input column for neighbors in the adjacency list representation.
   * Default: "neighbors"
   * @group param
   */
  @Since("2.4.0")
  val neighborsCol = new Param[String](this, "neighborsCol",
    "Name of the input column for neighbors in the adjacency list representation.",
    (value: String) => value.nonEmpty)

  setDefault(neighborsCol, "neighbors")

  /** @group getParam */
  @Since("2.4.0")
  def getNeighborsCol: String = $(neighborsCol)

  /**
   * Param for the name of the input column for neighbors in the adjacency list representation.
   * Default: "similarities"
   * @group param
   */
  @Since("2.4.0")
  val similaritiesCol = new Param[String](this, "similaritiesCol",
    "Name of the input column for neighbors in the adjacency list representation.",
    (value: String) => value.nonEmpty)

  setDefault(similaritiesCol, "similarities")

  /** @group getParam */
  @Since("2.4.0")
  def getSimilaritiesCol: String = $(similaritiesCol)

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnTypes(schema, $(idCol), Seq(IntegerType, LongType))
    SchemaUtils.checkColumnTypes(schema, $(neighborsCol),
      Seq(ArrayType(IntegerType, containsNull = false),
        ArrayType(LongType, containsNull = false)))
    SchemaUtils.checkColumnTypes(schema, $(similaritiesCol),
      Seq(ArrayType(FloatType, containsNull = false),
        ArrayType(DoubleType, containsNull = false)))
    SchemaUtils.appendColumn(schema, $(predictionCol), IntegerType)
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
 *  - `similaritiesCol`: non-negative weights (similarities) of edges between the vertex
 *                       in `idCol` and each neighbor in `neighborsCol`
 * PIC returns a cluster assignment for each input vertex.  It appends a new column `predictionCol`
 * containing the cluster assignment in `[0,k)` for each row (vertex).
 *
 * Notes:
 *  - [[PowerIterationClustering]] is a transformer with an expensive [[transform]] operation.
 *    Transform runs the iterative PIC algorithm to cluster the whole input dataset.
 *  - Input validation: This validates that similarities are non-negative but does NOT validate
 *    that the input matrix is symmetric.
 *
 * @see <a href=http://en.wikipedia.org/wiki/Spectral_clustering>
 * Spectral clustering (Wikipedia)</a>
 */
@Since("2.4.0")
@Experimental
class PowerIterationClustering private[clustering] (
    @Since("2.4.0") override val uid: String)
  extends Transformer with PowerIterationClusteringParams with DefaultParamsWritable {

  setDefault(
    k -> 2,
    maxIter -> 20,
    initMode -> "random")

  @Since("2.4.0")
  def this() = this(Identifiable.randomUID("PowerIterationClustering"))

  /** @group setParam */
  @Since("2.4.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

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
  def setIdCol(value: String): this.type = set(idCol, value)

  /** @group setParam */
  @Since("2.4.0")
  def setNeighborsCol(value: String): this.type = set(neighborsCol, value)

  /** @group setParam */
  @Since("2.4.0")
  def setSimilaritiesCol(value: String): this.type = set(similaritiesCol, value)

  @Since("2.4.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)

    val sparkSession = dataset.sparkSession
    val idColValue = $(idCol)
    val rdd: RDD[(Long, Long, Double)] =
      dataset.select(
        col($(idCol)).cast(LongType),
        col($(neighborsCol)).cast(ArrayType(LongType, containsNull = false)),
        col($(similaritiesCol)).cast(ArrayType(DoubleType, containsNull = false))
      ).rdd.flatMap {
        case Row(id: Long, nbrs: Seq[_], sims: Seq[_]) =>
          require(nbrs.size == sims.size, s"The length of the neighbor ID list must be " +
            s"equal to the the length of the neighbor similarity list.  Row for ID " +
            s"$idColValue=$id has neighbor ID list of length ${nbrs.length} but similarity list " +
            s"of length ${sims.length}.")
          nbrs.asInstanceOf[Seq[Long]].zip(sims.asInstanceOf[Seq[Double]]).map {
            case (nbr, similarity) => (id, nbr, similarity)
          }
      }
    val algorithm = new MLlibPowerIterationClustering()
      .setK($(k))
      .setInitializationMode($(initMode))
      .setMaxIterations($(maxIter))
    val model = algorithm.run(rdd)

    val predictionsRDD: RDD[Row] = model.assignments.map { assignment =>
      Row(assignment.id, assignment.cluster)
    }

    val predictionsSchema = StructType(Seq(
      StructField($(idCol), LongType, nullable = false),
      StructField($(predictionCol), IntegerType, nullable = false)))
    val predictions = {
      val uncastPredictions = sparkSession.createDataFrame(predictionsRDD, predictionsSchema)
      dataset.schema($(idCol)).dataType match {
        case _: LongType =>
          uncastPredictions
        case otherType =>
          uncastPredictions.select(col($(idCol)).cast(otherType).alias($(idCol)))
      }
    }

    dataset.join(predictions, $(idCol))
  }

  @Since("2.4.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("2.4.0")
  override def copy(extra: ParamMap): PowerIterationClustering = defaultCopy(extra)
}

@Since("2.4.0")
object PowerIterationClustering extends DefaultParamsReadable[PowerIterationClustering] {

  @Since("2.4.0")
  override def load(path: String): PowerIterationClustering = super.load(path)
}
