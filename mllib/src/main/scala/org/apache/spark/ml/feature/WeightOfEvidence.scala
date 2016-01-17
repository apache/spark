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
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class WeightOfEvidence(override val uid: String) extends Transformer
with HasInputCol with HasLabelCol with HasOutputCol  {

  def this() = this(Identifiable.randomUID("woe"))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setLabelCol(value: String): this.type = set(labelCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: DataFrame): DataFrame = {
    validateParams()
    val woeTable = WeightOfEvidence.getWoeTable(dataset, $(inputCol), $(labelCol))

    val woeMap = woeTable.map(r => {
      val category = r.getAs[String]($(inputCol))
      val woe = r.getAs[Double]("woe")
      (category, woe)
    }).collectAsMap()

    val trans = udf { (factor: String) =>
      woeMap.get(factor)
    }
    dataset.withColumn($(outputCol), trans(col($(inputCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateParams()
    val inputColNames = $(inputCol)
    val outputColName = $(outputCol)

    if (schema.fieldNames.contains(outputColName)) {
      throw new IllegalArgumentException(s"Output column $outputColName already exists.")
    }
    StructType(schema.fields :+ new StructField(outputColName, DoubleType, true))
  }

  override def copy(extra: ParamMap): VectorAssembler = defaultCopy(extra)
}

object WeightOfEvidence{

  def getInformationValue(dataset: DataFrame, categoryCol: String, labelCol: String): Double = {
    val tt = getWoeTable(dataset, categoryCol, labelCol)
    val iv = tt.selectExpr("SUM(woe * (p1 - p0)) as iv").first().getAs[Double](0)
    iv
  }

  def getWoeTable(dataset: DataFrame, categoryCol: String, labelCol: String): DataFrame = {

    val data = dataset.select(categoryCol, labelCol)
    val tmpTableName = "woe_temp"
    data.registerTempTable(tmpTableName)
    val err = 0.01
    val query = s"""
         |SELECT
         |$categoryCol,
         |SUM (IF(CAST ($labelCol AS DOUBLE)=1, 1, 0)) AS 1count,
         |SUM (IF(CAST ($labelCol AS DOUBLE)=0, 1, 0)) AS 0count
         |FROM $tmpTableName
         |GROUP BY $categoryCol
        """.stripMargin
    val groupResult = data.sqlContext.sql(query).cache()

    val total0 = groupResult.selectExpr("SUM(0count)").first().getAs[Long](0).toDouble
    val total1 = groupResult.selectExpr("SUM(1count)").first().getAs[Long](0).toDouble
    groupResult.selectExpr(
      categoryCol,
      s"1count/$total1 AS p1",
      s"0count/$total0 AS p0",
      s"LOG(($err + 1count) / $total1 * $total0 / (0count + $err)) AS woe")

  }
}