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

import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.feature.InformationValueModel.InformationValueModelWriter
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsReader, DefaultParamsWritable, DefaultParamsWriter, Identifiable, MLReadable, MLReader, MLWritable, MLWriter}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{StringType, StructType}

class InformationValueEstimator(override val uid: String)
  extends Estimator[InformationValueModel] with WeightOfEvidenceParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("informationValueEstimator"))

  override def fit(dataset: Dataset[_]): InformationValueModel = {

    transformSchema(dataset.schema, logging = true)
    val woe = new WeightOfEvidenceEstimator(uid)
      .setHandleInvalid($(handleInvalid))
      .setInputCols($(inputCols))
      .setOutputCols($(outputCols))
      .setLabelCol($(labelCol))

    val model = new InformationValueModel(uid, woe.fit(dataset).rule).setParent(this)
    copyValues(model)
  }

  override def copy(extra: ParamMap): Estimator[InformationValueModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)
}

object InformationValueEstimator extends DefaultParamsReadable[InformationValueEstimator] {

  private[feature] val SKIP_INVALID: String = "skip"
  private[feature] val ERROR_INVALID: String = "error"

  override def load(path: String): InformationValueEstimator = super.load(path)

}

class InformationValueModel private[ml](override val uid: String,
                                        val rule: Map[Int, Map[String, (Double, Double, Double)]])
  extends Model[InformationValueModel] with WeightOfEvidenceParams with MLWritable {

  private var iv: Map[String, Double] = _

  override def copy(extra: ParamMap): InformationValueModel = {
    defaultCopy[InformationValueModel](extra).setParent(parent)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {

    val transformedSchema = transformSchema(dataset.schema)
    val seq: Seq[UserDefinedFunction] = getInputCols.zipWithIndex.map { case (_, idx) =>
      udf { (feature: String) =>
        val (woe, yRecall, nRecall) = rule(idx)(feature)
        _iv(yRecall, nRecall, woe) // information value of each groups
      }
    }

    val newCols = getInputCols.zipWithIndex.map { case (inputCol, idx) =>
      seq(idx)(col(inputCol).cast(StringType))
    }

    val metadata = getOutputCols.map { col =>
      transformedSchema(col).metadata
    }

    val filteredData = getHandleInvalid match {
      case WeightOfEvidenceEstimator.SKIP_INVALID =>
        dataset.na.drop($(inputCols))
      case _ =>
        dataset
    }

    filteredData.withColumns(getOutputCols, newCols, metadata)
  }

  /**
   * get the IV of input features
   *
   * @return columnName -> iv
   */
  def getOrCalculateIV: Map[String, Double] = {
    if (iv == null) {
      iv = rule.map {
        case (idx, stats) =>
          val colName = getInputCols(idx)
          (colName, stats.foldLeft(0D) {
            case (cur, (_, (woe, yRecall, nRecall))) =>
              val iv = cur + _iv(yRecall, nRecall, woe)
              iv
          })
      }
    }
    iv
  }

  private def _iv(yRecall: Double, nRecall: Double, woe: Double): Double = {
    (yRecall - nRecall) * woe
  }

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)

  /**
   * Returns an `MLWriter` instance for this ML instance.
   */
  override def write: MLWriter = new InformationValueModelWriter(this)
}

object InformationValueModel extends MLReadable[InformationValueModel] {

  private[InformationValueModel]
  class InformationValueModelWriter(instance: InformationValueModel) extends MLWriter {

    private case class Data(rule: Map[Int, Map[String, (Double, Double, Double)]])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.rule)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class InformationValueModelReader extends MLReader[InformationValueModel] {

    private val className = classOf[InformationValueModel].getName

    override def load(path: String): InformationValueModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
        .select("rule")
        .head()
      val rule = data.getAs[Map[Int, Map[String, (Double, Double, Double)]]](0)
      val model = new InformationValueModel(metadata.uid, rule)
      metadata.getAndSetParams(model)
      model
    }
  }

  /**
   * Returns an `MLReader` instance for this class.
   */
  override def read: MLReader[InformationValueModel] = new InformationValueModelReader

  override def load(path: String): InformationValueModel = super.load(path)
}
