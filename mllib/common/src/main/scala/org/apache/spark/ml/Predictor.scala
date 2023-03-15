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

package org.apache.spark.ml

import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.sql.types.{DataType, DoubleType, StructType}

/**
 * (private[ml]) Trait for parameters for prediction (regression and classification).
 */
private[ml] trait PredictorParams
    extends Params
    with HasLabelCol
    with HasFeaturesCol
    with HasPredictionCol {

  /**
   * Validates and transforms the input schema with the provided param map.
   *
   * @param schema
   *   input schema
   * @param fitting
   *   whether this is in fitting
   * @param featuresDataType
   *   SQL DataType for FeaturesType. E.g., `VectorUDT` for vector features.
   * @return
   *   output schema
   */
  protected def validateAndTransformSchema(
      schema: StructType,
      fitting: Boolean,
      featuresDataType: DataType): StructType = {
    // TODO: Support casting Array[Double] and Array[Float] to Vector when FeaturesType = Vector
    SchemaUtils.checkColumnType(schema, $(featuresCol), featuresDataType)
    if (fitting) {
      SchemaUtils.checkNumericType(schema, $(labelCol))

      this match {
        case p: HasWeightCol =>
          if (isDefined(p.weightCol) && $(p.weightCol).nonEmpty) {
            SchemaUtils.checkNumericType(schema, $(p.weightCol))
          }
        case _ =>
      }
    }
    SchemaUtils.appendColumn(schema, $(predictionCol), DoubleType)
  }
}

