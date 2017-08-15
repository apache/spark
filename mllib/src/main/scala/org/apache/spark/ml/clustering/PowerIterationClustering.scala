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
   * The number of clusters to create (k). Must be > 1. Default: 2.
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
   * as vertex properties, or "degree" to use normalized sum similarities. Default: random.
   */
  @Since("2.3.0")
  final val initMode = {
    val allowedParams = ParamValidators.inArray(Array("random", "degree"))
    new Param[String](this, "initMode", "The initialization algorithm. " +
      "Supported options: 'random' and 'degree'.", allowedParams)
  }

  /** @group expertGetParam */
  @Since("2.3.0")
  def getInitMode: String = $(initMode)

  /**
   * Param for the column name for ids returned by [[PowerIterationClustering.transform()]].
   * Default: "id"
   * @group param
   */
  val idCol = new Param[String](this, "id", "column name for ids.")

  /** @group getParam */
  def getIdCol: String = $(idCol)

  /**
   * Param for the column name for neighbors required by [[PowerIterationClustering.transform()]].
   * Default: "neighbor"
   * @group param
   */
  val neighborCol = new Param[String](this, "neighbor", "column name for neighbors.")

  /** @group getParam */
  def getNeighborCol: String = $(neighborCol)

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
 * Note that we implement [[PowerIterationClustering]] as a transformer. The [[transform]] is an
 * expensive operation, because it uses PIC algorithm to cluster the whole input dataset.
 *
 * @see <a href=http://en.wikipedia.org/wiki/Spectral_clustering Spectral clustering (Wikipedia)</a>
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
    neighborCol -> "neighbor")

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
   * Sets the value of param [[neighborCol]].
   * Default is "neighbor"
   *
   * @group setParam
   */
  @Since("2.3.0")
  def setNeighborCol(value: String): this.type = set(neighborCol, value)

  @Since("2.3.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val sparkSession = dataset.sparkSession
    val rdd: RDD[(Long, Long, Double)] =
      dataset.select(col($(idCol)), col($(neighborCol)), col($(weightCol))).rdd.flatMap {
        case Row(id: Long, nbr: Vector, weight: Vector) =>
        require(nbr.size == weight.size,
          "The length of neighbor list must be equal to the the length of the weight list.")
        val ids = Array.fill(nbr.size)(id)
        for (i <- 0 until ids.size) yield (ids(i), nbr(i).toLong, weight(i))}
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

