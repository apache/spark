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

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasPredictionCol}
import org.apache.spark.ml.util._
import org.apache.spark.mllib.fpm.{AssociationRules => MLlibAssociationRules,
FPGrowth => MLlibFPGrowth}
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.sql._
import org.apache.spark.sql.types._

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
    val inputType = schema($(featuresCol)).dataType
    require(inputType.isInstanceOf[ArrayType],
      s"The input column must be ArrayType, but got $inputType.")
    SchemaUtils.appendColumn(schema, $(predictionCol), schema($(featuresCol)).dataType)
  }

  /**
   * Minimal support level of the frequent pattern. [0.0, 1.0]. Any pattern that appears
   * more than (minSupport * size-of-the-dataset) times will be output
   * Default: 0.3
   * @group param
   */
  @Since("2.2.0")
  val minSupport: DoubleParam = new DoubleParam(this, "minSupport",
    "the minimal support level of the frequent pattern (Default: 0.3)",
    ParamValidators.inRange(0.0, 1.0))
  setDefault(minSupport -> 0.3)

  /** @group getParam */
  @Since("2.2.0")
  def getMinSupport: Double = $(minSupport)

  /**
   * Number of partitions used by parallel FP-growth
   * @group expertParam
   */
  @Since("2.2.0")
  val numPartitions: IntParam = new IntParam(this, "numPartitions",
    "Number of partitions used by parallel FP-growth", ParamValidators.gtEq[Int](1))

  /** @group expertGetParam */
  @Since("2.2.0")
  def getNumPartitions: Int = $(numPartitions)

  /**
   * minimal confidence for generating Association Rule
   * Default: 0.8
   * @group param
   */
  @Since("2.2.0")
  val minConfidence: DoubleParam = new DoubleParam(this, "minConfidence",
    "minimal confidence for generating Association Rule (Default: 0.8)",
    ParamValidators.inRange(0.0, 1.0))
  setDefault(minConfidence -> 0.8)

  /** @group getParam */
  @Since("2.2.0")
  def getMinConfidence: Double = $(minConfidence)

}

/**
 * :: Experimental ::
 * A parallel FP-growth algorithm to mine frequent itemsets.
 *
 * @see [[http://dx.doi.org/10.1145/1454008.1454027 Li et al., PFP: Parallel FP-Growth for Query
 *  Recommendation]]
 */
@Since("2.2.0")
@Experimental
class FPGrowth @Since("2.2.0") (
    @Since("2.2.0") override val uid: String)
  extends Estimator[FPGrowthModel] with FPGrowthParams with DefaultParamsWritable {

  @Since("2.2.0")
  def this() = this(Identifiable.randomUID("fpgrowth"))

  /** @group setParam */
  @Since("2.2.0")
  def setMinSupport(value: Double): this.type = set(minSupport, value)

  /** @group expertSetParam */
  @Since("2.2.0")
  def setNumPartitions(value: Int): this.type = set(numPartitions, value)

  /** @group setParam
   *  minConfidence has not effect during fitting.
   */
  @Since("2.2.0")
  def setMinConfidence(value: Double): this.type = set(minConfidence, value)

  /** @group setParam */
  @Since("2.2.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("2.2.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  override def fit(dataset: Dataset[_]): FPGrowthModel = {
    genericFit(dataset)
  }

  private def genericFit[T: ClassTag](dataset: Dataset[_]): FPGrowthModel = {
    val data = dataset.select($(featuresCol)).rdd.map(r => r.getSeq[T](0).toArray)
    val parentModel = new MLlibFPGrowth().setMinSupport($(minSupport)).run(data)
    val rows = parentModel.freqItemsets
      .map(f => (f.items, f.freq))
      .map(cols => Row(cols._1, cols._2))

    val dt = dataset.schema($(featuresCol)).dataType
    val fields = Array(StructField("items", dt, nullable = false),
      StructField("freq", LongType, nullable = false))
    val schema = StructType(fields)
    val frequentItems = dataset.sparkSession.createDataFrame(rows, schema).toDF("items", "freq")
    copyValues(new FPGrowthModel(uid, frequentItems)).setParent(this)
  }

  @Since("2.2.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): FPGrowth = defaultCopy(extra)
}


@Since("2.2.0")
object FPGrowth extends DefaultParamsReadable[FPGrowth] {

  @Since("2.2.0")
  override def load(path: String): FPGrowth = super.load(path)
}

/**
 * :: Experimental ::
 * Model fitted by FPGrowth.
 *
 * @param freqItemsets frequent items in the format of DataFrame("items", "freq")
 */
@Since("2.2.0")
@Experimental
class FPGrowthModel private[ml] (
    @Since("2.2.0") override val uid: String,
    val freqItemsets: DataFrame)
  extends Model[FPGrowthModel] with FPGrowthParams with MLWritable {

  /** @group setParam */
  @Since("2.2.0")
  def setMinConfidence(value: Double): this.type = set(minConfidence, value)

  /** @group setParam */
  @Since("2.2.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("2.2.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

   /**
   * Get association rules fitted by AssociationRules using the minConfidence. Returns a dataframe
   * with three fields, "antecedent", "consequent" and "confidence", where "antecedent" and
   * "consequent" are Array[String] and "confidence" is Double.
   */
  @Since("2.2.0")
  @transient lazy val getAssociationRules: DataFrame = {
    val freqItems = getFreqItemsets
    AssocaitionRules.getAssocationRulesFromFP(freqItems, "items", "freq", $(minConfidence))
  }

  /**
   * Get frequent items fitted by FPGrowth, in the format of DataFrame("items", "freq")
   */
  @Since("2.2.0")
  @transient val getFreqItemsets: DataFrame = freqItemsets

  @Since("2.2.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    genericTransform(dataset, getAssociationRules)
  }

  private def genericTransform[T](dataset: Dataset[_], associationRules: DataFrame): DataFrame = {
    // use unique id to perform the join and aggregateByKey
    val itemsRDD = dataset.select($(featuresCol)).rdd.map(r => r.getSeq[T](0))
      .distinct().zipWithUniqueId().map(_.swap).cache()
    val rulesRDD = associationRules.rdd.map(r => (r.getSeq[T](0), r.getSeq[T](1)))

    val itemsWithConsequents = itemsRDD.cartesian(rulesRDD).map {
      case ((id, items), (antecedent, consequent)) =>
        val itemSet = items.toSet
        val consequents = if (antecedent.forall(itemSet.contains(_))) consequent else Seq.empty
        (id, consequents)
    }.aggregateByKey(new ArrayBuffer[T])(
      (ar, seq) => ar ++= seq, (ar, seq) => ar ++= seq)

    val mappingRDD = itemsRDD.join(itemsWithConsequents)
      .map { case (id, (items, consequent)) => (items, consequent) }
      .map (cols => Row(cols._1, cols._2))
    val dt = dataset.schema($(featuresCol)).dataType
    val fields = Array($(featuresCol), $(predictionCol))
      .map(fieldName => StructField(fieldName, dt, nullable = true))
    val schema = StructType(fields)
    val mapping = dataset.sparkSession.createDataFrame(mappingRDD, schema)

    dataset.join(mapping, $(featuresCol))
  }

  @Since("2.2.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("2.2.0")
  override def copy(extra: ParamMap): FPGrowthModel = {
    val copied = new FPGrowthModel(uid, freqItemsets)
    copyValues(copied, extra).setParent(this.parent)
  }

  @Since("2.2.0")
  override def write: MLWriter = new FPGrowthModel.FPGrowthModelWriter(this)
}

object FPGrowthModel extends MLReadable[FPGrowthModel] {
  @Since("2.2.0")
  override def read: MLReader[FPGrowthModel] = new FPGrowthModelReader

  @Since("2.2.0")
  override def load(path: String): FPGrowthModel = super.load(path)

  /** [[MLWriter]] instance for [[FPGrowthModel]] */
  private[FPGrowthModel]
  class FPGrowthModelWriter(instance: FPGrowthModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val dataPath = new Path(path, "data").toString
      instance.freqItemsets.write.save(dataPath)
    }
  }

  private class FPGrowthModelReader extends MLReader[FPGrowthModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[FPGrowthModel].getName

    override def load(path: String): FPGrowthModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val frequentItems = sparkSession.read.load(dataPath)
      val model = new FPGrowthModel(metadata.uid, frequentItems)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }
}

private[fpm] object AssocaitionRules {

  /**
   * Computes the association rules with confidence above minConfidence.
   * @param dataset DataFrame("items", "freq") containing frequent itemset obtained from
   *                algorithms like [[FPGrowth]].
   * @param itemsCol column name for frequent itemsets
   * @param freqCol column name for frequent itemsets count
   * @param minConfidence minimum confidence for the result association rules
   * @return a DataFrame("antecedent", "consequent", "confidence") containing the association
   *         rules.
   */
  @Since("2.2.0")
  def getAssocationRulesFromFP[T: ClassTag](dataset: Dataset[_],
        itemsCol: String = "items",
        freqCol: String = "freq",
        minConfidence: Double = 0.8): DataFrame = {

    val freqItemSetRdd = dataset.select(itemsCol, freqCol).rdd
      .map(row => new FreqItemset(row.getSeq[T](0).toArray, row.getLong(1)))
    val rows = new MLlibAssociationRules()
      .setMinConfidence(minConfidence)
      .run(freqItemSetRdd)
      .map(r => Row(r.antecedent, r.consequent, r.confidence))

    val dt = dataset.schema(itemsCol).dataType
    val fields = Array("antecedent", "consequent")
      .map(fieldName => StructField(fieldName, dt, nullable = false)) ++
      Seq(StructField("confidence", DoubleType, nullable = false))
    val schema = StructType(fields)
    val mapping = dataset.sparkSession.createDataFrame(rows, schema)
    mapping
  }
}
