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

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.mllib.linalg.{Matrices, MatrixUDT}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * :: Experimental ::
 * Binarize a column of continuous features given a set of labels.
 */

@Experimental
final class LabelBinarizer @Since("2.0.0")(override val uid: String)
  extends Transformer with HasInputCol with HasOutputCol with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("labelBinarizer"))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)

    val iter = udf { (s: String) =>
      val arr = s.split(",")
      val len = arr.length
      val clsLen = arr.distinct.length
      val vec: Array[Double] = new Array(len * clsLen)
      var i: Int = 0
      var j: Int = 0
      arr.distinct.sortWith(_ < _).foreach { (v: String) =>
        while (i < arr.length) {
          val idx: Int = arr.indexOf(v, i)
          if (idx != -1) {
            vec.update(idx + j, 1)
          }
          i += 1
        }
        i = 0
        j += len
      }
      Matrices.dense(len, clsLen, vec)
    }
    dataset.withColumn($(outputCol), iter(col($(inputCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    val outputColName = $(outputCol)
    val inputType = schema($(inputCol)).dataType

    if (!inputType.typeName.equals("string")) {
      throw new IllegalArgumentException(s"Data type $inputType is not supported.")
    }

    if (schema.fieldNames.contains(outputColName)) {
      throw new IllegalArgumentException(s"Output column $outputColName already exists.")
    }
    val outputFields = schema.fields :+ StructField($(outputCol), new MatrixUDT, false)
    StructType(outputFields)
  }

  override def copy(extra: ParamMap): LabelBinarizer = defaultCopy(extra)
}

@Since("1.6.0")
object LabelBinarizer extends DefaultParamsReadable[LabelBinarizer] {

  @Since("1.6.0")
  override def load(path: String): LabelBinarizer = super.load(path)
}
