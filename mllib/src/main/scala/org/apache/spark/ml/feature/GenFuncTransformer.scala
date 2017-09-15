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

import scala.util.Random

import org.apache.spark.annotation.Since
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasInputCols
import org.apache.spark.ml.param.shared.HasOutputCol
import org.apache.spark.ml.util.DefaultParamsReadable
import org.apache.spark.ml.util.DefaultParamsWritable
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.NumericType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import javax.script.ScriptEngineManager


/**
 * A feature transformer that executes a given javascript function on dataframe columns.
 */
@Since("2.3.0")
class GenFuncTransformer(override val uid: String)
  extends Transformer with HasInputCols with HasOutputCol with DefaultParamsWritable {
  
  def this() = this(Identifiable.randomUID("mathTransformer"))
  
  final val function: Param[String] = new Param[String](this, "function", "Evaulation function written in javascript which return numerical value")
  
  /** @group setParam */
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)
  
  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)
  
  /** @group setParam */
  def setFunction(value: String): this.type = set(function, value)
  
  @Since("2.3.0")
  def transform(dataset: Dataset[_]): DataFrame = {
    val schema = dataset.schema
    implicit val rowEncoder = RowEncoder(transformSchema(schema))
    dataset.toDF.mapPartitions {
      rows =>
        val engine = new ScriptEngineManager().getEngineByName("JavaScript")
        val func = {
          val alphabets = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
          (for(i <- 0 until 10) yield {
            val position = Random.nextInt(52)
            alphabets.charAt(position)
          }).mkString
        }
        engine.eval("var " + func + " = " + $(function).stripMargin)
        rows.map {
          row =>
            $(inputCols).foreach {
              col =>
                val datatype = schema(col).dataType
                datatype match {
                  case _: NumericType => engine.eval(col + "=" + row.get(row.fieldIndex(col)))
                  case _ => engine.eval(col + "=\"" + row.get(row.fieldIndex(col)).toString + "\"")
                }
            }
            val args = $(inputCols).mkString("(", ",", ")")
            val resObj = engine.eval(func + args)
            val res = if(resObj == null) Double.NaN else resObj.toString.toDouble
            Row.fromSeq(row.toSeq :+ res)
        }
    }
  }

  @Since("2.3.0")
  def transformSchema(schema: StructType): StructType = {
    schema.add(StructField($(outputCol), DoubleType, true))
  }

  @Since("2.3.0")
  def copy(extra: ParamMap): GenFuncTransformer = defaultCopy(extra)
}

@Since("2.3.0")
object GenFuncTransformer extends DefaultParamsReadable[GenFuncTransformer] {
  
  @Since("2.3.0")
  override def load(path: String): GenFuncTransformer = super.load(path)
}