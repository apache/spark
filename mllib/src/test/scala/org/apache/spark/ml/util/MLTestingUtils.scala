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

package org.apache.spark.ml.util

import org.apache.spark.ml.Model
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object MLTestingUtils {
  def checkCopy(model: Model[_]): Unit = {
    val copied = model.copy(ParamMap.empty)
      .asInstanceOf[Model[_]]
    assert(copied.parent.uid == model.parent.uid)
    assert(copied.parent == model.parent)
  }

  def generateDFWithNumericLabelCol(
    sqlContext: SQLContext,
    labelColName: String,
    featuresColName: String
  ): Map[NumericType, DataFrame] = {
    val df = sqlContext.createDataFrame(Seq(
      (0, Vectors.dense(0, 2, 3)),
      (1, Vectors.dense(0, 3, 1)),
      (0, Vectors.dense(0, 2, 2)),
      (1, Vectors.dense(0, 3, 9)),
      (0, Vectors.dense(0, 2, 6))
    )).toDF(labelColName, featuresColName)

    val types =
      Seq(ShortType, LongType, IntegerType, FloatType, ByteType, DoubleType, DecimalType(10, 0))
    types.map(t => t -> df.select(col(labelColName).cast(t), col(featuresColName))).toMap
  }

  def generateDFWithStringLabelCol(
    sqlContext: SQLContext,
    labelColName: String,
    featuresColName: String
  ): DataFrame =
    sqlContext.createDataFrame(Seq(
      ("0", Vectors.dense(0, 2, 3)),
      ("1", Vectors.dense(0, 3, 1)),
      ("0", Vectors.dense(0, 2, 2)),
      ("1", Vectors.dense(0, 3, 9)),
      ("0", Vectors.dense(0, 2, 6))
    )).toDF(labelColName, featuresColName)
}
