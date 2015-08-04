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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{Estimator, Model, Pipeline, PipelineModel, PipelineStage, Transformer}

@Experimental
class Interaction(override val uid: String) extends Estimator[PipelineModel]
  with HasInputCols with HasOutputCol {
  def this() = this(Identifiable.randomUID("interaction"))

  /** @group setParam */
  def setInputCols(values: Array[String]): this.type = set(inputCols, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def fit(dataset: DataFrame): InteractionModel = {
    require(isDefined(inputCols), "Input cols must be defined first.")
    require(isDefined(outputCol), "Output col must be defined first.")
    require($(inputCols).length > 0, "Input cols must have non-zero length.")

    val encoderStages = ArrayBuffer[PipelineStage]()
    val tempColumns = ArrayBuffer[String]()
    val (factorCols, nonFactorCols) = $(inputCols)
      .partition(input => dataset.schema(input).dataType == StringType)

    val encodedFactors: Option[String] =
      if (factorCols.length > 0) {
        val indexedCols = factorCols.map { input =>
          val output = input + "_idx_" + uid
          encoderStages += new StringIndexer()
            .setInputCol(input)
            .setOutputCol(output)
          tempColumns += output
          output
        }
        val combinedIndexCol = "combined_idx_" + uid
        tempColumns += combinedIndexCol
        val encodedCol = if (nonFactorCols.length > 0) {
          "factors_" + uid
        } else {
          $(outputCol)
        }
        encoderStages += new IndexCombiner(indexedCols, combinedIndexCol)
        encoderStages += new OneHotEncoder()
          .setInputCol(combinedIndex)
          .setOutputCol(encodedCol)
        Some(encodedCol)
      } else {
        None
      }

    if (nonFactorCols.length > 0) {
      // TODO(ekl) scale encodedFactors if exists by these cols
      ???
    }

    encoderStages += new ColumnPruner(tempColumns.toSet)
    new Pipeline(uid)
      .setStages(encoderStages.toArray)
      .fit(dataset)
      .setParent(this)
  }
}

private class IndexCombiner(inputCols: Array[String], outputCol: String) extends Transformer {
  override val uid = Identifiable.randomUID("indexCombiner")

  override def transform(dataset: DataFrame): DataFrame = {
    val cardinalities = inputCols.map(col =>
      Attribute.fromStructField(dataset.schema(col)).asInstanceOf[NominalAttribute].values.length)
    val combiner = udf { cols: Array[Double] =>
      var offset = 1
      var res = cols(0)
      var i = 0
      while (i < cols.length) {
        res += cols(i) * offset
        offset *= cardinalities(i)
        i += 1
      }
      res
    }
    dataset.select("*", combiner(array(inputCols.map(dataset): _*)).as(outputCol))
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(schema.fields :+ StructField(outputCol, DoubleType, false))
  }

  override def copy(extra: ParamMap): ColumnPruner = defaultCopy(extra)
}
