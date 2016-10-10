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

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.param.{DoubleParam, Param, ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.fpm.{AssociationRules => MLlibAssociationRules}
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * :: Experimental ::
 *
 * Generates association rules from frequent itemsets ("items", "freq"). This method only generates
 * association rules which have a single item as the consequent.
 *
 */
@Since("2.1.0")
@Experimental
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
   * Param for frequency column name.
   * @group param
   */
  final val freqCol: Param[String] = new Param[String](this, "freqCol", "frequency column name")


  /** @group getParam */
  final def getFreqCol: String = $(freqCol)

  /** @group setParam */
  def setFreqCol(value: String): this.type = set(freqCol, value)

  /**
   * Param for minimum confidence, range [0.0, 1.0].
   * @group param
   */
  final val minConfidence: DoubleParam = new DoubleParam(this, "minConfidence", "min confidence")

  /** @group getParam */
  final def getMinConfidence: Double = $(minConfidence)

  /** @group setParam */
  def setMinConfidence(value: Double): this.type = set(minConfidence, value)

  /**
   * Param for minimum support, range [0.0, 1.0].
   * @group param
   */
  final val minSupport: DoubleParam = new DoubleParam(this, "minSupport", "minimum support")

  /** @group getParam */
  final def getMinSupport: Double = $(minSupport)

  /** @group setParam */
  def setMinSupport(value: Double): this.type = set(minSupport, value)

  setDefault(itemsCol -> "items", freqCol -> "freq", minSupport -> 0.3, minConfidence -> 0.8)

  def run(dataset: Dataset[_]): DataFrame = {
    val freqItemSetRdd = dataset.select($(itemsCol), $(freqCol)).rdd
      .map(row => new FreqItemset(row.getSeq[String](0).toArray, row.getLong(1)))

    val sqlContext = SparkSession.builder().getOrCreate()
    import sqlContext.implicits._
    val associationRules = new MLlibAssociationRules().setMinConfidence($(minConfidence))
    associationRules
      .run(freqItemSetRdd)
      .map(r => (r.antecedent, r.consequent, r.confidence))
      .toDF("antecedent", "consequent", "confidence")
  }

  override def copy(extra: ParamMap): AssociationRules = defaultCopy(extra)

}
