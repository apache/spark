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

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCols, HasOutputCol}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class WeightOfEvidence(override val uid: String)
  extends Transformer with HasInputCols with HasOutputCol with DefaultParamsWritable {

    def this() = this(Identifiable.randomUID("woe"))

    /** @group setParam */
    def setInputCols(value: Array[String]): this.type = set(inputCols, value)

    /** @group setParam */
    def setOutputCol(value: String): this.type = set(outputCol, value)

    override def transform(dataset: DataFrame): DataFrame = {
      validateParams()
      val inputFeatures = $(inputCols).map(c => dataset.schema(c))

      val categoryCol = $(inputCols)(0)
      val labelCol = $(inputCols)(1)

      val data = dataset.select(categoryCol, labelCol)
      val tmpTableName = "woe_temp"
      data.registerTempTable(tmpTableName)

      val total0 = data.where(s"$labelCol= '0'").count()
      val total1 = data.where(s"$labelCol= '1'").count()

      val tt = data.sqlContext.sql(
        s"""
          |select
          |$categoryCol,
          |sum (IF($labelCol='1', 1, 1e-2)) as 1count,
          |sum (IF($labelCol='0', 1, 1e-2)) as 0count
          |from $tmpTableName
          |group by $categoryCol
        """.stripMargin
      ).selectExpr(categoryCol, "1count", "0count", s"1count/$total1 as p1", s"0count/$total0 as p0", "1count/0count as ratio")

      val woeMap = tt.map(r => {
        val category = r.getString(0)
        val oneZeroRatio = r.getAs[Double]("ratio")
        val base = total1.toDouble / total0
        val woe = math.log(oneZeroRatio / base)
        (category, woe)
      }).collectAsMap()

      val iv = tt.map(r => {
        val oneZeroRatio = r.getAs[Double]("ratio")
        val base = total1.toDouble / total0
        val woe = math.log(oneZeroRatio / base)
        val p1 = r.getAs[Double]("p1")
        val p0 = r.getAs[Double]("p0")
        woe * (p1 - p0)
      }).sum()

      println("information value: " + iv)

      val trans = udf { (factor: String) =>
        woeMap.get(factor)
      }

      dataset.withColumn($(outputCol), trans(col(categoryCol)))
    }

    override def transformSchema(schema: StructType): StructType = {
      validateParams()
      val inputColNames = $(inputCols)
      val outputColName = $(outputCol)
      val inputDataTypes = inputColNames.map(name => schema(name).dataType)

      if (schema.fieldNames.contains(outputColName)) {
        throw new IllegalArgumentException(s"Output column $outputColName already exists.")
      }
      StructType(schema.fields :+ new StructField(outputColName, DoubleType, true))
    }

    override def copy(extra: ParamMap): VectorAssembler = defaultCopy(extra)
  }
