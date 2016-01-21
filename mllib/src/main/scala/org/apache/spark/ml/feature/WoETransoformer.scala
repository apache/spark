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

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.feature.WoEModel.WoEModelWriter
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.param.shared.{HasInputCol, HasLabelCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

private[feature] trait WoEParams
  extends Params with HasInputCol with HasLabelCol with HasOutputCol {

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    validateParams()
    require(isDefined(inputCol),
      s"WoETransformer requires input column parameter: $inputCol")
    require(isDefined(outputCol),
      s"WoETransformer requires output column parameter: $outputCol")
    require(isDefined(labelCol),
      s"WoETransformer requires output column parameter: $labelCol")

    val outputColName = $(outputCol)
    if (schema.fieldNames.contains(outputColName)) {
      throw new IllegalArgumentException(s"Output column $outputColName already exists.")
    }
    StructType(schema.fields :+ new StructField(outputColName, DoubleType, true))
  }
}

/**
  * :: Experimental ::
  * The Weight of Evidence or WoE value provides a measure of how well a grouping of feature is
  * able to distinguish between a binary response (e.g. "good" versus "bad"), which is widely
  * used in grouping continuous feature or mapping categorical features to continuous values.
  *      WoEi = ln(Distribution Of Goodi / Distribution of Badi).
  *
  * The WoE recoding of features is particularly well suited for subsequent modeling using
  * Logistic Regression or MLP.
  *
  * In addition, the information value or IV can be computed based on WoE, which is a popular
  * technique to select variables in a predictive model.
  *
  * TODO: estimate the proper grouping for continuous feature.
  */
@Experimental
@Since("2.0.0")
class WoETransformer(override val uid: String)
  extends Estimator[WoEModel] with WoEParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("woe"))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /**
    * Set the column name which is used as binary classes label column. The data type can be
    * boolean or numeric, which has at most two distinct values.
    * @group setParam */
  def setLabelCol(value: String): this.type = set(labelCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def fit(dataset: DataFrame): WoEModel = {
    transformSchema(dataset.schema, logging = true)
    val woeTable = WoETransformer.getWoeTable(dataset, $(inputCol), $(labelCol))
    copyValues(new WoEModel(uid, woeTable).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): WoETransformer = defaultCopy(extra)
}

@Experimental
@Since("2.0.0")
object WoETransformer{

  def getInformationValue(dataset: DataFrame, categoryCol: String, labelCol: String): Double = {
    val tt = getWoeTable(dataset, categoryCol, labelCol)
    val iv = tt.selectExpr("SUM(woe * (p1 - p0)) as iv").first().getAs[Double](0)
    iv
  }

  private def getWoeTable(dataset: DataFrame, categoryCol: String, labelCol: String): DataFrame = {
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
    val groupResult = data.sqlContext.sql(query)

    val total0 = groupResult.selectExpr("SUM(0count)").first().getAs[Long](0).toDouble
    val total1 = groupResult.selectExpr("SUM(1count)").first().getAs[Long](0).toDouble
    groupResult.selectExpr(
      categoryCol,
      s"1count/$total1 AS p1",
      s"0count/$total0 AS p0",
      s"LOG(($err + 1count) / $total1 * $total0 / (0count + $err)) AS woe")
  }
}

@Experimental
@Since("2.0.0")
class WoEModel private[ml] (override val uid: String,
    val woeTable: DataFrame)
  extends Model[WoEModel] with WoEParams with MLWritable {

  override def transform(dataset: DataFrame): DataFrame = {
    validateParams()
    val iv = woeTable.selectExpr("SUM(woe * (p1 - p0)) as iv").first().getAs[Double](0)
    logInfo(s"iv value for ${$(inputCol)} is: $iv")

    val woeMap = woeTable.map(r => {
      val category = r.get(0)
      val woe = r.getAs[Double]("woe")
      (category, woe)
    }).collectAsMap().toMap

    val trans = udf { (factor: Any) => woeMap.get(factor) }
    dataset.withColumn($(outputCol), trans(col($(inputCol))))
  }

  @Since("2.0.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("2.0.0")
  override def copy(extra: ParamMap): WoEModel = {
    val copied = new WoEModel(uid, woeTable)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("2.0.0")
  override def write: MLWriter = new WoEModelWriter(this)
}


@Since("2.0.0")
object WoEModel extends MLReadable[WoEModel] {

  private[WoEModel]
  class WoEModelWriter(instance: WoEModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val dataPath = new Path(path, "data").toString
      instance.woeTable.repartition(1).write.parquet(dataPath)
    }
  }

  private class WoEModelReader extends MLReader[WoEModel] {

    private val className = classOf[WoEModel].getName

    override def load(path: String): WoEModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sqlContext.read.parquet(dataPath)
      val model = new WoEModel(metadata.uid, data)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  @Since("2.0.0")
  override def read: MLReader[WoEModel] = new WoEModelReader

  @Since("2.0.0")
  override def load(path: String): WoEModel = super.load(path)
}
