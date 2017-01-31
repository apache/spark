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
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.HasPredictionCol
import org.apache.spark.ml.util.{DefaultParamsReader, DefaultParamsWriter, _}
import org.apache.spark.mllib.fpm.{AssociationRules => MLlibAssociationRules}
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


/**
 * Common params for AssociationRules and AssociationRulesModel
 */
private[fpm] trait AssociationRulesParam extends Params with HasPredictionCol{

  /**
   * Param for items column name. Items must be array of Strings.
   * Default: "items"
   *
   * @group param
   */
  final val itemsCol: Param[String] = new Param[String](this, "itemsCol", "column name in the" +
    " DataFrame containing the items")


  /** @group getParam */
  @Since("2.2.0")
  final def getItemsCol: String = $(itemsCol)
  setDefault(itemsCol -> "items")
}

@Since("2.2.0")
object AssociationRules extends DefaultParamsReadable[AssociationRules] {

  @Since("2.2.0")
  override def load(path: String): AssociationRules = super.load(path)
}

/**
 * :: Experimental ::
 *
 * Generates association rules from frequent itemsets DataFrame("items", "freq"). This method only
 * generates association rules which have a single item as the consequent.
 */
@Since("2.2.0")
@Experimental
class AssociationRules(override val uid: String)
  extends Estimator[AssociationRulesModel] with AssociationRulesParam with HasPredictionCol {

  @Since("2.2.0")
  def this() = this(Identifiable.randomUID("AssociationRules"))

  /** @group setParam */
  @Since("2.2.0")
  def setItemsCol(value: String): this.type = set(itemsCol, value)

  /**
   * Param for frequency column name. Data type should be Long.
   * Default: "freq"
   *
   * @group param
   */
  final val freqCol: Param[String] = new Param[String](this, "freqCol", "frequency column name")


  /** @group getParam */
  @Since("2.2.0")
  final def getFreqCol: String = $(freqCol)

  /** @group setParam */
  @Since("2.2.0")
  def setFreqCol(value: String): this.type = set(freqCol, value)
  setDefault(freqCol -> "freq")

  /**
   * Param for minimum confidence, range [0.0, 1.0].
    *
    * @group param
   */
  final val minConfidence: DoubleParam = new DoubleParam(this, "minConfidence", "min confidence",
    ParamValidators.inRange(0.0, 1.0))

  /** @group getParam */
  @Since("2.2.0")
  final def getMinConfidence: Double = $(minConfidence)

  /** @group setParam */
  @Since("2.2.0")
  def setMinConfidence(value: Double): this.type = set(minConfidence, value)
  setDefault(minConfidence -> 0.8)

  /**
   * Computes the association rules with confidence above [[minConfidence]].
   *
   * @return a DataFrame("antecedent", "consequent", "confidence") containing the association
   *         rules.
   */
  @Since("2.2.0")
  override def fit(dataset: Dataset[_]): AssociationRulesModel = {
    val freqItemSetRdd = dataset.select($(itemsCol), $(freqCol)).rdd
      .map(row => new FreqItemset(row.getSeq[String](0).toArray, row.getLong(1)))

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val oldModel = new MLlibAssociationRules()
      .setMinConfidence($(minConfidence))
      .run(freqItemSetRdd)
      .map(r => (r.antecedent, r.consequent, r.confidence))
      .toDF("antecedent", "consequent", "confidence")
    copyValues(new AssociationRulesModel(uid, oldModel).setParent(this))
  }

  @Since("2.2.0")
  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(itemsCol), new ArrayType(StringType, false))
    SchemaUtils.checkColumnType(schema, $(freqCol), LongType)
    SchemaUtils.appendColumn(schema, $(predictionCol), new ArrayType(StringType, false))
  }

  override def copy(extra: ParamMap): AssociationRules = defaultCopy(extra)

}


/**
 * :: Experimental ::
 * Model fitted by AssociationRules.
 *
 * @param associationRules AssociationRules
 */
@Since("2.2.0")
@Experimental
class AssociationRulesModel private[ml] (
    @Since("2.2.0") override val uid: String,
    val associationRules: DataFrame)
  extends Model[AssociationRulesModel] with AssociationRulesParam with MLWritable {

  /** @group setParam */
  @Since("2.2.0")
  def setItemsCol(value: String): this.type = set(itemsCol, value)

  /** @group setParam */
  @Since("2.2.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  @Since("2.2.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val rules = associationRules.rdd.map(r =>
      (r.getSeq[String](0), r.getSeq[String](1))
    ).collect()

    // For each rule, examine the input items and summarize the consequents
    val predictUDF = udf((items: Seq[String]) => {
      val itemset = items.toSet
      rules.flatMap {
        r => if (r._1.forall(itemset.contains)) r._2 else Array.empty[String]
      }.distinct.filterNot(itemset.contains)
    })
    dataset.withColumn($(predictionCol), predictUDF(col($(itemsCol))))
  }

  @Since("2.2.0")
  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(itemsCol), new ArrayType(StringType, false))
    SchemaUtils.appendColumn(schema, $(predictionCol), new ArrayType(StringType, false))
  }

  @Since("2.2.0")
  override def copy(extra: ParamMap): AssociationRulesModel = {
    val copied = new AssociationRulesModel(uid, this.associationRules)
    copyValues(copied, extra)
  }

  @Since("2.2.0")
  override def write: MLWriter = new AssociationRulesModel.AssociationRulesModelWriter(this)
}

object AssociationRulesModel extends MLReadable[AssociationRulesModel] {
  @Since("2.2.0")
  override def read: MLReader[AssociationRulesModel] = new AssociationRulesModelReader

  @Since("2.2.0")
  override def load(path: String): AssociationRulesModel = super.load(path)

  /** [[MLWriter]] instance for [[AssociationRulesModel]] */
  private[AssociationRulesModel]
  class AssociationRulesModelWriter(instance: AssociationRulesModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val dataPath = new Path(path, "data").toString
      instance.associationRules.repartition(1).write.save(dataPath)
    }
  }

  private class AssociationRulesModelReader extends MLReader[AssociationRulesModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[AssociationRulesModel].getName

    override def load(path: String): AssociationRulesModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val associationRules = sparkSession.read.load(dataPath)
      val model = new AssociationRulesModel(metadata.uid, associationRules)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }
}
