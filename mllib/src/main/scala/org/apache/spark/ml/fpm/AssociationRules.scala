package org.apache.spark.ml.fpm

import org.apache.spark.annotation.Since
import org.apache.spark.ml.param.{ParamMap, DoubleParam, Params, Param}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.mllib.fpm.{FPGrowth, AssociationRules => MLlibAssociationRules}
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset}

/**
 * Created by yuhao on 6/15/16.
 */
class AssociationRules(override val uid: String) extends Params {

  @Since("2.1.0")
  def this() = this(Identifiable.randomUID("AssociationRules"))

  /**
   * Param for items column name.
   * @group param
   */
  final val itemsCol: Param[String] = new Param[String](this, "itemsCol", "items column name")


  /** @group getParam */
  final def getItemsCol: String = $(itemsCol)

  /** @group setParam */
  def setItemsCol(value: String): this.type = set(itemsCol, value)

  /**
   * Param for items column name.
   * @group param
   */
  final val freqCol: Param[String] = new Param[String](this, "freqCol", "items column name")


  /** @group getParam */
  final def getFreqCol: String = $(freqCol)

  /** @group setParam */
  def setFreqCol(value: String): this.type = set(freqCol, value)

  /**
   * Param for items column name.
   * @group param
   */
  final val minConfidence: DoubleParam = new DoubleParam(this, "minConfidence", "min Confidence")


  /** @group getParam */
  final def getMinConfidence: Double = $(minConfidence)

  /** @group setParam */
  def setMinConfidence(value: Double): this.type = set(minConfidence, value)

  setDefault(itemsCol -> "items", freqCol -> "items")

  def run(dataset: Dataset[_]): DataFrame = {
    val rdd = dataset.select($(itemsCol), $(freqCol)).rdd.map(row =>
      new FreqItemset(row.getAs[Array[Any]](0), row.getLong(1))
    )
    val sqlContext = SparkSession.builder().getOrCreate()
    import sqlContext.implicits._
    val associationRules = new MLlibAssociationRules().setMinConfidence($(minConfidence))
    associationRules.run(rdd)
      .map(r => (r.antecedent.map(_.toString), r.consequent.map(_.toString), r.confidence))
      .toDF("antecedent",  "consequent", "confidence")
  }

  override def copy(extra: ParamMap): AssociationRules = defaultCopy(extra)

}
