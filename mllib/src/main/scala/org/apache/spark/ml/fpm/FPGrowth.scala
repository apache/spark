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

import scala.reflect.ClassTag

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.HasPredictionCol
import org.apache.spark.ml.util._
import org.apache.spark.mllib.fpm.{AssociationRules => MLlibAssociationRules,
  FPGrowth => MLlibFPGrowth}
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Common params for FPGrowth and FPGrowthModel
 */
private[fpm] trait FPGrowthParams extends Params with HasPredictionCol {

  /**
   * Items column name.
   * Default: "items"
   * @group param
   */
  @Since("2.2.0")
  val itemsCol: Param[String] = new Param[String](this, "itemsCol", "items column name")
  setDefault(itemsCol -> "items")

  /** @group getParam */
  @Since("2.2.0")
  def getItemsCol: String = $(itemsCol)

  /**
   * Minimal support level of the frequent pattern. [0.0, 1.0]. Any pattern that appears
   * more than (minSupport * size-of-the-dataset) times will be output in the frequent itemsets.
   * Default: 0.3
   * @group param
   */
  @Since("2.2.0")
  val minSupport: DoubleParam = new DoubleParam(this, "minSupport",
    "the minimal support level of a frequent pattern",
    ParamValidators.inRange(0.0, 1.0))
  setDefault(minSupport -> 0.3)

  /** @group getParam */
  @Since("2.2.0")
  def getMinSupport: Double = $(minSupport)

  /**
   * Number of partitions (at least 1) used by parallel FP-growth. By default the param is not
   * set, and partition number of the input dataset is used.
   * @group expertParam
   */
  @Since("2.2.0")
  val numPartitions: IntParam = new IntParam(this, "numPartitions",
    "Number of partitions used by parallel FP-growth", ParamValidators.gtEq[Int](1))

  /** @group expertGetParam */
  @Since("2.2.0")
  def getNumPartitions: Int = $(numPartitions)

  /**
   * Minimal confidence for generating Association Rule. minConfidence will not affect the mining
   * for frequent itemsets, but will affect the association rules generation.
   * Default: 0.8
   * @group param
   */
  @Since("2.2.0")
  val minConfidence: DoubleParam = new DoubleParam(this, "minConfidence",
    "minimal confidence for generating Association Rule",
    ParamValidators.inRange(0.0, 1.0))
  setDefault(minConfidence -> 0.8)

  /** @group getParam */
  @Since("2.2.0")
  def getMinConfidence: Double = $(minConfidence)

  /**
   * Validates and transforms the input schema.
   * @param schema input schema
   * @return output schema
   */
  @Since("2.2.0")
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    val inputType = schema($(itemsCol)).dataType
    require(inputType.isInstanceOf[ArrayType],
      s"The input column must be ArrayType, but got $inputType.")
    SchemaUtils.appendColumn(schema, $(predictionCol), schema($(itemsCol)).dataType)
  }
}

/**
 * :: Experimental ::
 * A parallel FP-growth algorithm to mine frequent itemsets. The algorithm is described in
 * <a href="http://dx.doi.org/10.1145/1454008.1454027">Li et al., PFP: Parallel FP-Growth for Query
 * Recommendation</a>. PFP distributes computation in such a way that each worker executes an
 * independent group of mining tasks. The FP-Growth algorithm is described in
 * <a href="http://dx.doi.org/10.1145/335191.335372">Han et al., Mining frequent patterns without
 * candidate generation</a>. Note null values in the itemsCol column are ignored during fit().
 *
 * @see <a href="http://en.wikipedia.org/wiki/Association_rule_learning">
 * Association rule learning (Wikipedia)</a>
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

  /** @group setParam */
  @Since("2.2.0")
  def setMinConfidence(value: Double): this.type = set(minConfidence, value)

  /** @group setParam */
  @Since("2.2.0")
  def setItemsCol(value: String): this.type = set(itemsCol, value)

  /** @group setParam */
  @Since("2.2.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  @Since("2.2.0")
  override def fit(dataset: Dataset[_]): FPGrowthModel = {
    transformSchema(dataset.schema, logging = true)
    genericFit(dataset)
  }

  private def genericFit[T: ClassTag](dataset: Dataset[_]): FPGrowthModel = {
    val data = dataset.select($(itemsCol))
    val items = data.where(col($(itemsCol)).isNotNull).rdd.map(r => r.getSeq[T](0).toArray)
    val mllibFP = new MLlibFPGrowth().setMinSupport($(minSupport))
    if (isSet(numPartitions)) {
      mllibFP.setNumPartitions($(numPartitions))
    }
    val parentModel = mllibFP.run(items)
    val rows = parentModel.freqItemsets.map(f => Row(f.items, f.freq))
    val schema = StructType(Seq(
      StructField("items", dataset.schema($(itemsCol)).dataType, nullable = false),
      StructField("freq", LongType, nullable = false)))
    val frequentItems = dataset.sparkSession.createDataFrame(rows, schema)
    copyValues(new FPGrowthModel(uid, frequentItems)).setParent(this)
  }

  @Since("2.2.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("2.2.0")
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
 * @param freqItemsets frequent itemsets in the format of DataFrame("items"[Array], "freq"[Long])
 */
@Since("2.2.0")
@Experimental
class FPGrowthModel private[ml] (
    @Since("2.2.0") override val uid: String,
    @Since("2.2.0") @transient val freqItemsets: DataFrame)
  extends Model[FPGrowthModel] with FPGrowthParams with MLWritable {

  /** @group setParam */
  @Since("2.2.0")
  def setMinConfidence(value: Double): this.type = set(minConfidence, value)

  /** @group setParam */
  @Since("2.2.0")
  def setItemsCol(value: String): this.type = set(itemsCol, value)

  /** @group setParam */
  @Since("2.2.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /**
   * Cache minConfidence and associationRules to avoid redundant computation for association rules
   * during transform. The associationRules will only be re-computed when minConfidence changed.
   */
  @transient private var _cachedMinConf: Double = Double.NaN

  @transient private var _cachedRules: DataFrame = _

  /**
   * Get association rules fitted using the minConfidence. Returns a dataframe
   * with three fields, "antecedent", "consequent" and "confidence", where "antecedent" and
   * "consequent" are Array[T] and "confidence" is Double.
   */
  @Since("2.2.0")
  @transient def associationRules: DataFrame = {
    if ($(minConfidence) == _cachedMinConf) {
      _cachedRules
    } else {
      _cachedRules = AssociationRules
        .getAssociationRulesFromFP(freqItemsets, "items", "freq", $(minConfidence))
      _cachedMinConf = $(minConfidence)
      _cachedRules
    }
  }

  /**
   * The transform method first generates the association rules according to the frequent itemsets.
   * Then for each transaction in itemsCol, the transform method will compare its items against the
   * antecedents of each association rule. If the record contains all the antecedents of a
   * specific association rule, the rule will be considered as applicable and its consequents
   * will be added to the prediction result. The transform method will summarize the consequents
   * from all the applicable rules as prediction. The prediction column has the same data type as
   * the input column(Array[T]) and will not contain existing items in the input column. The null
   * values in the itemsCol columns are treated as empty sets.
   * WARNING: internally it collects association rules to the driver and uses broadcast for
   * efficiency. This may bring pressure to driver memory for large set of association rules.
   */
  @Since("2.2.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    genericTransform(dataset)
  }

  private def genericTransform(dataset: Dataset[_]): DataFrame = {
    val rules: Array[(Seq[Any], Seq[Any])] = associationRules.select("antecedent", "consequent")
      .rdd.map(r => (r.getSeq(0), r.getSeq(1)))
      .collect().asInstanceOf[Array[(Seq[Any], Seq[Any])]]
    val brRules = dataset.sparkSession.sparkContext.broadcast(rules)

    val dt = dataset.schema($(itemsCol)).dataType
    // For each rule, examine the input items and summarize the consequents
    val predictUDF = udf((items: Seq[_]) => {
      if (items != null) {
        val itemset = items.toSet
        brRules.value.flatMap(rule =>
          if (items != null && rule._1.forall(item => itemset.contains(item))) {
            rule._2.filter(item => !itemset.contains(item))
          } else {
            Seq.empty
          }).distinct
      } else {
        Seq.empty
      }}, dt)
    dataset.withColumn($(predictionCol), predictUDF(col($(itemsCol))))
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

@Since("2.2.0")
object FPGrowthModel extends MLReadable[FPGrowthModel] {

  @Since("2.2.0")
  override def read: MLReader[FPGrowthModel] = new FPGrowthModelReader

  @Since("2.2.0")
  override def load(path: String): FPGrowthModel = super.load(path)

  /** [[MLWriter]] instance for [[FPGrowthModel]] */
  private[FPGrowthModel]
  class FPGrowthModelWriter(instance: FPGrowthModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val dataPath = new Path(path, "data").toString
      instance.freqItemsets.write.parquet(dataPath)
    }
  }

  private class FPGrowthModelReader extends MLReader[FPGrowthModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[FPGrowthModel].getName

    override def load(path: String): FPGrowthModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val frequentItems = sparkSession.read.parquet(dataPath)
      val model = new FPGrowthModel(metadata.uid, frequentItems)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }
}

private[fpm] object AssociationRules {

  /**
   * Computes the association rules with confidence above minConfidence.
   * @param dataset DataFrame("items"[Array], "freq"[Long]) containing frequent itemsets obtained
   *                from algorithms like [[FPGrowth]].
   * @param itemsCol column name for frequent itemsets
   * @param freqCol column name for appearance count of the frequent itemsets
   * @param minConfidence minimum confidence for generating the association rules
   * @return a DataFrame("antecedent"[Array], "consequent"[Array], "confidence"[Double])
   *         containing the association rules.
   */
  def getAssociationRulesFromFP[T: ClassTag](
        dataset: Dataset[_],
        itemsCol: String,
        freqCol: String,
        minConfidence: Double): DataFrame = {

    val freqItemSetRdd = dataset.select(itemsCol, freqCol).rdd
      .map(row => new FreqItemset(row.getSeq[T](0).toArray, row.getLong(1)))
    val rows = new MLlibAssociationRules()
      .setMinConfidence(minConfidence)
      .run(freqItemSetRdd)
      .map(r => Row(r.antecedent, r.consequent, r.confidence))

    val dt = dataset.schema(itemsCol).dataType
    val schema = StructType(Seq(
      StructField("antecedent", dt, nullable = false),
      StructField("consequent", dt, nullable = false),
      StructField("confidence", DoubleType, nullable = false)))
    val rules = dataset.sparkSession.createDataFrame(rows, schema)
    rules
  }
}
