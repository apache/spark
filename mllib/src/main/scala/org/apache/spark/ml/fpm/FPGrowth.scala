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

package org.apache.spark.ml.fpm

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.{DoubleParam, ParamMap, Params}
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasPredictionCol}
import org.apache.spark.ml.util._
import org.apache.spark.mllib.fpm.{FPGrowth => MLlibFPGrowth, FPGrowthModel => MLlibFPGrowthModel}
import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

/**
 * Common params for FPGrowth and FPGrowthModel
 */
private[fpm] trait FPGrowthParams extends Params with HasFeaturesCol with HasPredictionCol {

  /**
   * Validates and transforms the input schema.
   * @param schema input schema
   * @return output schema
   */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new ArrayType(StringType, false))
    SchemaUtils.appendColumn(schema, $(predictionCol), new ArrayType(StringType, false))
  }

  /**
   * the minimal support level of the frequent pattern
   * Default: 0.3
   * @group param
   */
  @Since("2.1.0")
  val minSupport: DoubleParam = new DoubleParam(this, "minSupport",
    "the minimal support level of the frequent pattern")

  /** @group getParam */
  @Since("2.1.0")
  def getMinSupport: Double = $(minSupport)

}

/**
 * :: Experimental ::
 * A parallel FP-growth algorithm to mine frequent itemsets.
 *
 * @see [[http://dx.doi.org/10.1145/1454008.1454027 Li et al., PFP: Parallel FP-Growth for Query
 *  Recommendation]]
 */
@Since("2.1.0")
@Experimental
class FPGrowth @Since("2.1.0") (
    @Since("2.1.0") override val uid: String)
  extends Estimator[FPGrowthModel] with FPGrowthParams with DefaultParamsWritable {

  @Since("2.1.0")
  def this() = this(Identifiable.randomUID("FPGrowth"))

  /** @group setParam */
  @Since("2.1.0")
  def setMinSupport(value: Double): this.type = set(minSupport, value)
  setDefault(minSupport -> 0.3)

  /** @group setParam */
  @Since("2.1.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("2.1.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  def fit(dataset: Dataset[_]): FPGrowthModel = {
    val data = dataset.select($(featuresCol)).rdd.map(r =>
      r.getSeq[String](0).toArray)
    val parentModel = new MLlibFPGrowth()
      .setMinSupport($(minSupport))
      .run(data)
    copyValues(new FPGrowthModel(uid, parentModel))
  }

  @Since("2.1.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): FPGrowth = defaultCopy(extra)
}


@Since("2.1.0")
object FPGrowth extends DefaultParamsReadable[FPGrowth] {

  @Since("2.1.0")
  override def load(path: String): FPGrowth = super.load(path)
}

/**
 * :: Experimental ::
 * Model fitted by FPGrowth.
 *
 * @param parentModel a model trained by spark.mllib.fpm.FPGrowth
 */
@Since("2.1.0")
@Experimental
class FPGrowthModel private[ml] (
    @Since("2.1.0") override val uid: String,
    private val parentModel: MLlibFPGrowthModel[_])
  extends Model[FPGrowthModel] with FPGrowthParams with MLWritable {

  /**
   * minimal confidence for generating Association Rule
   * Default: 0.8
   * @group param
   */
  @Since("2.1.0")
  val minConfidence: DoubleParam = new DoubleParam(this, "minConfidence",
    "minimal confidence for generating Association Rule")
  setDefault(minConfidence -> 0.8)

  /** @group getParam */
  @Since("2.1.0")
  def getMinConfidence: Double = $(minConfidence)

  /** @group setParam */
  @Since("2.1.0")
  def setMinConfidence(value: Double): this.type = set(minConfidence, value)

  @Since("2.1.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val associationRules = generateAssociationRules().rdd.map(r =>
      (r.getSeq[String](0), r.getSeq[String](1))
    ).collect()

    // For each rule, examine the input items and summarize the consequents
    val predictUDF = udf((items: Seq[String]) => associationRules.flatMap( r =>
      if (r._1.forall(items.contains(_))) r._2 else Array.empty[String]
    ).distinct)
    dataset.withColumn($(predictionCol), predictUDF(col($(featuresCol))))
  }

  @Since("2.1.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("2.1.0")
  override def copy(extra: ParamMap): FPGrowthModel = {
    val copied = new FPGrowthModel(uid, parentModel)
    copyValues(copied, extra)
  }

  private[fpm] def generateAssociationRules(): DataFrame = {
    val sqlContext = SparkSession.builder().getOrCreate()
    import sqlContext.implicits._
    val freqItemsets = parentModel.freqItemsets.map(f => (f.items.map(_.toString), f.freq))
      .toDF("items", "freq")

    val associationRules = new AssociationRules()
      .setMinConfidence($(minConfidence))
      .setMinSupport($(minSupport))
      .setItemsCol("items")
      .setFreqCol("freq")
    associationRules.run(freqItemsets)
  }

  @Since("2.1.0")
  override def write: MLWriter = new FPGrowthModel.FPGrowthModelWriter(this)

}


object FPGrowthModel extends MLReadable[FPGrowthModel] {
  @Since("2.1.0")
  override def read: MLReader[FPGrowthModel] = new FPGrowthModelReader

  @Since("2.1.0")
  override def load(path: String): FPGrowthModel = super.load(path)

  /** [[MLWriter]] instance for [[FPGrowthModel]] */
  private[FPGrowthModel]
  class FPGrowthModelWriter(instance: FPGrowthModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val dataPath = new Path(path, "data").toString
      instance.parentModel.save(sc, dataPath)
    }
  }

  private class FPGrowthModelReader extends MLReader[FPGrowthModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[FPGrowthModel].getName

    override def load(path: String): FPGrowthModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val mllibModel = MLlibFPGrowthModel.load(sc, dataPath)
      val model = new FPGrowthModel(metadata.uid, mllibModel)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }
}

