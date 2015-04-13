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

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.param._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}

@AlphaComponent
class OneHotEncoder(labelNames: Seq[String], includeFirst: Boolean = true) extends Transformer
  with HasInputCol {

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  private def outputColName(index: Int): String = {
    s"${get(inputCol)}_${labelNames(index)}"
  }

  override def transform(dataset: DataFrame, paramMap: ParamMap): DataFrame = {
    val map = this.paramMap ++ paramMap

    val startIndex = if (includeFirst) 0 else 1
    val cols = (startIndex until labelNames.length).map { index =>
      val colEncoder = udf { label: Double => if (index == label) 1.0 else 0.0 }
      colEncoder(dataset(map(inputCol))).as(outputColName(index))
    }

    dataset.select(Array(col("*")) ++ cols: _*)
  }

  override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    val map = this.paramMap ++ paramMap
    checkInputColumn(schema, map(inputCol), StringType)
    val inputFields = schema.fields
    val startIndex = if (includeFirst) 0 else 1
    val fields = (startIndex until labelNames.length).map { index =>
      val colName = outputColName(index)
      require(inputFields.forall(_.name != colName),
        s"Output column $colName already exists.")
      NominalAttribute.defaultAttr.withName(colName).toStructField()
    }

    val outputFields = inputFields ++ fields
    StructType(outputFields)
  }
}
