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

package org.apache.spark.ml.feature

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Since
import org.apache.spark.ml._
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg.{DenseMatrix => OldDenseMatrix, Vectors => OldVectors}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.VersionUtils.majorVersion

/**
 * Params for [[PCA]] and [[PCAModel]].
 */
private[feature] trait PCAParams extends Params with HasInputCol with HasOutputCol {

  /**
   * The number of principal components.
   * @group param
   */
  final val k: IntParam = new IntParam(this, "k", "the number of principal components (> 0)",
    ParamValidators.gt(0))

  /** @group getParam */
  def getK: Int = $(k)

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), new VectorUDT)
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    SchemaUtils.updateAttributeGroupSize(schema, $(outputCol), $(k))
  }
}

/**
 * PCA trains a model to project vectors to a lower dimensional space of the top `PCA!.k`
 * principal components.
 */
@Since("1.5.0")
class PCA @Since("1.5.0") (
    @Since("1.5.0") override val uid: String)
  extends Estimator[PCAModel] with PCAParams with DefaultParamsWritable {

  @Since("1.5.0")
  def this() = this(Identifiable.randomUID("pca"))

  /** @group setParam */
  @Since("1.5.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setK(value: Int): this.type = set(k, value)

  /**
   * Computes a [[PCAModel]] that contains the principal components of the input vectors.
   */
  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): PCAModel = {
    transformSchema(dataset.schema, logging = true)
    val input = dataset.select($(inputCol)).rdd.map {
      case Row(v: Vector) => OldVectors.fromML(v)
    }
    val pca = new feature.PCA(k = $(k))
    val pcaModel = pca.fit(input)
    copyValues(new PCAModel(uid, pcaModel.pc.asML, pcaModel.explainedVariance.asML)
      .setParent(this))
  }

  @Since("1.5.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): PCA = defaultCopy(extra)
}

@Since("1.6.0")
object PCA extends DefaultParamsReadable[PCA] {

  @Since("1.6.0")
  override def load(path: String): PCA = super.load(path)
}

/**
 * Model fitted by [[PCA]]. Transforms vectors to a lower dimensional space.
 *
 * @param pc A principal components Matrix. Each column is one principal component.
 * @param explainedVariance A vector of proportions of variance explained by
 *                          each principal component.
 */
@Since("1.5.0")
class PCAModel private[ml] (
    @Since("1.5.0") override val uid: String,
    @Since("2.0.0") val pc: DenseMatrix,
    @Since("2.0.0") val explainedVariance: DenseVector)
  extends Model[PCAModel] with PCAParams with MLWritable {

  import PCAModel._

  /** @group setParam */
  @Since("1.5.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /**
   * Transform a vector by computed Principal Components.
   *
   * @note Vectors to be transformed must be the same length as the source vectors given
   * to `PCA.fit()`.
   */
  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema, logging = true)

    val transposed = pc.transpose
    val transformer = udf { vector: Vector => transposed.multiply(vector) }
    dataset.withColumn($(outputCol), transformer(col($(inputCol))),
      outputSchema($(outputCol)).metadata)
  }

  @Since("1.5.0")
  override def transformSchema(schema: StructType): StructType = {
    var outputSchema = validateAndTransformSchema(schema)
    if ($(outputCol).nonEmpty) {
      outputSchema = SchemaUtils.updateAttributeGroupSize(outputSchema,
        $(outputCol), $(k))
    }
    outputSchema
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): PCAModel = {
    val copied = new PCAModel(uid, pc, explainedVariance)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("1.6.0")
  override def write: MLWriter = new PCAModelWriter(this)

  @Since("3.0.0")
  override def toString: String = {
    s"PCAModel: uid=$uid, k=${$(k)}"
  }
}

@Since("1.6.0")
object PCAModel extends MLReadable[PCAModel] {

  private[PCAModel] class PCAModelWriter(instance: PCAModel) extends MLWriter {

    private case class Data(pc: DenseMatrix, explainedVariance: DenseVector)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sparkSession)
      val data = Data(instance.pc, instance.explainedVariance)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).write.parquet(dataPath)
    }
  }

  private class PCAModelReader extends MLReader[PCAModel] {

    private val className = classOf[PCAModel].getName

    /**
     * Loads a [[PCAModel]] from data located at the input path. Note that the model includes an
     * `explainedVariance` member that is not recorded by Spark 1.6 and earlier. A model
     * can be loaded from such older data but will have an empty vector for
     * `explainedVariance`.
     *
     * @param path path to serialized model data
     * @return a [[PCAModel]]
     */
    override def load(path: String): PCAModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sparkSession, className)

      val dataPath = new Path(path, "data").toString
      val model = if (majorVersion(metadata.sparkVersion) >= 2) {
        val Row(pc: DenseMatrix, explainedVariance: DenseVector) =
          sparkSession.read.parquet(dataPath)
            .select("pc", "explainedVariance")
            .head()
        new PCAModel(metadata.uid, pc, explainedVariance)
      } else {
        // pc field is the old matrix format in Spark <= 1.6
        // explainedVariance field is not present in Spark <= 1.6
        val Row(pc: OldDenseMatrix) = sparkSession.read.parquet(dataPath).select("pc").head()
        new PCAModel(metadata.uid, pc.asML, new DenseVector(Array.emptyDoubleArray))
      }
      metadata.getAndSetParams(model)
      model
    }
  }

  @Since("1.6.0")
  override def read: MLReader[PCAModel] = new PCAModelReader

  @Since("1.6.0")
  override def load(path: String): PCAModel = super.load(path)
}
