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
package org.apache.spark.ml.evaluation

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.param.{IntParam, Param, ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared.{HasLabelCol, HasPredictionCol}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable, SchemaUtils}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, collect_list, row_number, udf}
import org.apache.spark.sql.types.LongType

/**
 * Evaluator for ranking.
 */
@Since("2.2.0")
@Experimental
final class RankingEvaluator @Since("2.2.0")(@Since("2.2.0") override val uid: String)
  extends Evaluator with HasPredictionCol with HasLabelCol with DefaultParamsWritable {

  @Since("2.2.0")
  def this() = this(Identifiable.randomUID("rankingEval"))

  @Since("2.2.0")
  val k = new IntParam(this, "k", "Top-K cutoff", (x: Int) => x > 0)

  /** @group getParam */
  @Since("2.2.0")
  def getK: Int = $(k)

  /** @group setParam */
  @Since("2.2.0")
  def setK(value: Int): this.type = set(k, value)

  setDefault(k -> 1)

  @Since("2.2.0")
  val metricName: Param[String] = {
    val allowedParams = ParamValidators.inArray(Array("mpr"))
    new Param(this, "metricName", "metric name in evaluation (mpr)", allowedParams)
  }

  /** @group getParam */
  @Since("2.2.0")
  def getMetricName: String = $(metricName)

  /** @group setParam */
  @Since("2.2.0")
  def setMetricName(value: String): this.type = set(metricName, value)

  /** @group setParam */
  @Since("2.2.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("2.2.0")
  def setLabelCol(value: String): this.type = set(labelCol, value)

  /**
   * Param for query column name.
   * @group param
   */
  val queryCol: Param[String] = new Param[String](this, "queryCol", "query column name")

  setDefault(queryCol, "query")

  /** @group getParam */
  @Since("2.2.0")
  def getQueryCol: String = $(queryCol)

  /** @group setParam */
  @Since("2.2.0")
  def setQueryCol(value: String): this.type = set(queryCol, value)

  setDefault(metricName -> "mpr")

  @Since("2.2.0")
  override def evaluate(dataset: Dataset[_]): Double = {
    val schema = dataset.schema
    SchemaUtils.checkNumericType(schema, $(predictionCol))
    SchemaUtils.checkNumericType(schema, $(labelCol))
    SchemaUtils.checkNumericType(schema, $(queryCol))

    val w = Window.partitionBy(col($(queryCol))).orderBy(col($(predictionCol)).desc)

    val topAtk: DataFrame = dataset
      .na.drop("all", Seq($(predictionCol)))
      .select(col($(predictionCol)), col($(labelCol)).cast(LongType), col($(queryCol)))
      .withColumn("rn", row_number().over(w)).where(col("rn") <= $(k))
      .drop("rn")
      .groupBy(col($(queryCol)))
      .agg(collect_list($(labelCol)).as("topAtk"))

    val mapToEmptyArray_ = udf(() => Array.empty[Long])

    val predictionAndLabels: DataFrame = dataset
      .join(topAtk, Seq($(queryCol)), "outer")
      .withColumn("topAtk", coalesce(col("topAtk"), mapToEmptyArray_()))
      .select($(labelCol), "topAtk")

    val metrics = new RankingMetrics(predictionAndLabels, "topAtk", $(labelCol))
    val metric = $(metricName) match {
      case "mpr" => metrics.meanPercentileRank
    }
    metric
  }

  @Since("2.2.0")
  override def isLargerBetter: Boolean = $(metricName) match {
    case "mpr" => false
  }

  @Since("2.2.0")
  override def copy(extra: ParamMap): RankingEvaluator = defaultCopy(extra)
}

@Since("2.2.0")
object RankingEvaluator extends DefaultParamsReadable[RankingEvaluator] {

  @Since("2.2.0")
  override def load(path: String): RankingEvaluator = super.load(path)

}
