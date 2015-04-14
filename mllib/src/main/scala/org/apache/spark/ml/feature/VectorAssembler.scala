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

import scala.collection.mutable.ArrayBuilder

import org.apache.spark.SparkException
import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared._
import org.apache.spark.mllib.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, CreateStruct}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * :: AlphaComponent ::
 * A feature transformer than merge multiple columns into a vector column.
 */
@AlphaComponent
class VectorAssembler extends Transformer with HasInputCols with HasOutputCol {

  /** @group setParam */
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: DataFrame, paramMap: ParamMap): DataFrame = {
    val map = extractParamMap(paramMap)
    val assembleFunc = udf { r: Row =>
      VectorAssembler.assemble(r.toSeq: _*)
    }
    val schema = dataset.schema
    val inputColNames = map(inputCols)
    val args = inputColNames.map { c =>
      schema(c).dataType match {
        case DoubleType => UnresolvedAttribute(c)
        case t if t.isInstanceOf[VectorUDT] => UnresolvedAttribute(c)
        case _: NativeType => Alias(Cast(UnresolvedAttribute(c), DoubleType), s"${c}_double_$uid")()
      }
    }
    dataset.select(col("*"), assembleFunc(new Column(CreateStruct(args))).as(map(outputCol)))
  }

  override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    val map = extractParamMap(paramMap)
    val inputColNames = map(inputCols)
    val outputColName = map(outputCol)
    val inputDataTypes = inputColNames.map(name => schema(name).dataType)
    inputDataTypes.foreach {
      case _: NativeType =>
      case t if t.isInstanceOf[VectorUDT] =>
      case other =>
        throw new IllegalArgumentException(s"Data type $other is not supported.")
    }
    if (schema.fieldNames.contains(outputColName)) {
      throw new IllegalArgumentException(s"Output column $outputColName already exists.")
    }
    StructType(schema.fields :+ new StructField(outputColName, new VectorUDT, false))
  }
}

@AlphaComponent
object VectorAssembler {

  private[feature] def assemble(vv: Any*): Vector = {
    val indices = ArrayBuilder.make[Int]
    val values = ArrayBuilder.make[Double]
    var cur = 0
    vv.foreach {
      case v: Double =>
        if (v != 0.0) {
          indices += cur
          values += v
        }
        cur += 1
      case vec: Vector =>
        vec.foreachActive { case (i, v) =>
          if (v != 0.0) {
            indices += cur + i
            values += v
          }
        }
        cur += vec.size
      case null =>
        // TODO: output Double.NaN?
        throw new SparkException("Values to assemble cannot be null.")
      case o =>
        throw new SparkException(s"$o of type ${o.getClass.getName} is not supported.")
    }
    Vectors.sparse(cur, indices.result(), values.result())
  }
}
