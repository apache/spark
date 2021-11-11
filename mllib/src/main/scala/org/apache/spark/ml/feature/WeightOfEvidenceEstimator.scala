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
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.feature.WeightOfEvidenceModel.WeightOfEvidenceModelWriter
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasHandleInvalid, HasInputCols, HasLabelCol, HasOutputCols}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsReader, DefaultParamsWritable, DefaultParamsWriter, Identifiable, MLReadable, MLReader, MLWritable, MLWriter}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{NumericType, StringType, StructType}

private[ml] trait WeightOfEvidenceParams extends HasInputCols with HasOutputCols
  with HasLabelCol with HasHandleInvalid {

  /** @group setParam */
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  /** @group setParam */
  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  /** @group setParam */
  def setLabelCol(value: String): this.type = set(labelCol, value)

  /** @group setParam */
  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)

  setDefault(handleInvalid, WeightOfEvidenceEstimator.ERROR_INVALID)

  /**
   * validates and transforms data schema
   */
  protected def validateAndTransformSchema(schema: StructType): StructType = {

    $(inputCols).foreach(inputColName => {
      val inputDataType = schema(inputColName).dataType
      require(inputDataType == StringType || inputDataType.isInstanceOf[NumericType],
        s"The input column $inputColName must be either string type or numeric type, " +
          s"but got $inputDataType.")
    })

    val inputFields = schema.fields

    val zipFields = $(outputCols).map(outputColName => {
      require(inputFields.forall(_.name != outputColName),
        s"Output column $outputColName already exists.")
      NominalAttribute.defaultAttr.withName(outputColName).toStructField()
    })

    StructType(inputFields ++ zipFields)
  }
}

class WeightOfEvidenceEstimator(override val uid: String)
  extends Estimator[WeightOfEvidenceModel]
    with WeightOfEvidenceParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("weightOfEvidenceEstimator"))

  override def fit(dataset: Dataset[_]): WeightOfEvidenceModel = {

    transformSchema(dataset.schema, logging = true)
    val rule = dataset.na.drop($(inputCols))
      .select($(inputCols).map(col(_).cast(StringType)) ++ Array(col($(labelCol))): _*)
      .rdd
      .flatMap(row => for (i <- 0 until row.size - 1) yield {
        // (colIndex,(value,label))
        (i, (row.getAs[String](i), row.getAs[Double](row.size - 1)))
      })
      .groupByKey()
      .map {
        case (colIndex, itr) =>
          var theCnt0 = 0D
          var theCnt1 = 0D

          val stats = itr.groupBy(_._1)
            .map {
              case (v, itr) =>
                val (marginObservation0, marginObservation1) = itr.foldLeft((0L, 0L)) {
                  case ((cnt0, cnt1), (_, label)) =>
                    if (label == 0D) {
                      theCnt0 += 1D
                      (cnt0 + 1, cnt1)
                    } else if (label == 1D) {
                      theCnt1 += 1D
                      (cnt0, cnt1 + 1)
                    } else throw new UnsupportedOperationException(
                      s"Classification labels should be 0 or 1 but found $label.")
                }

                (v, marginObservation0, marginObservation1)
            }

          require(theCnt0 != 0 && theCnt1 != 0,
            s"meaningless data set,found number of label0 is" +
              s" $theCnt0 and the number of table1 is $theCnt1")

          val woes = stats.map {
            case (v, marginObservation0, marginObservation1) =>

              var marginCnt0 = marginObservation0.toDouble
              var marginCnt1 = marginObservation1.toDouble

              // the purpose is mostly to prevent division by zero
              // and the default value of 1 is mostly to reduce the variance
              if (marginCnt0 == 0) marginCnt0 = 1
              if (marginCnt1 == 0) marginCnt1 = 1

              val yRecall = marginCnt1 / theCnt1
              val nRecall = marginCnt0 / theCnt0

              val woe = _woe(yRecall, nRecall)
              (v, (woe, yRecall, nRecall))
          }.toMap

          (colIndex, woes)
      }.collect().toMap

    copyValues(new WeightOfEvidenceModel(uid, rule)).setParent(this)
  }


  /**
   * get woe of a group
   *
   * @param yProb the probability of grouped y in total y
   * @param nProb the probability of grouped n in total n
   * @return woe value
   */
  private def _woe(yProb: Double, nProb: Double): Double = {
    math.log(yProb / nProb)
  }

  override def copy(extra: ParamMap): Estimator[WeightOfEvidenceModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)

}

object WeightOfEvidenceEstimator extends DefaultParamsReadable[WeightOfEvidenceEstimator] {

  private[feature] val SKIP_INVALID: String = "skip"
  private[feature] val ERROR_INVALID: String = "error"

  override def load(path: String): WeightOfEvidenceEstimator = super.load(path)

}

class WeightOfEvidenceModel private[ml](override val uid: String,
                                        val rule: Map[Int, Map[String, (Double, Double, Double)]])
  extends Model[WeightOfEvidenceModel] with WeightOfEvidenceParams with MLWritable {

  override def copy(extra: ParamMap): WeightOfEvidenceModel = {
    defaultCopy[WeightOfEvidenceModel](extra).setParent(parent)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {

    val transformedSchema = transformSchema(dataset.schema)

    val seq: Seq[UserDefinedFunction] = getInputCols.zipWithIndex.map { case (_, idx) =>
      udf { (feature: String) => rule(idx)(feature)._1 }
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

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)

  /**
   * Returns an `MLWriter` instance for this ML instance.
   */
  override def write: MLWriter = new WeightOfEvidenceModelWriter(this)
}

object WeightOfEvidenceModel extends MLReadable[WeightOfEvidenceModel] {

  private[WeightOfEvidenceModel]
  class WeightOfEvidenceModelWriter(instance: WeightOfEvidenceModel) extends MLWriter {

    private case class Data(rule: Map[Int, Map[String, (Double, Double, Double)]])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.rule)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class WeightOfEvidenceModelReader extends MLReader[WeightOfEvidenceModel] {

    private val className = classOf[WeightOfEvidenceModel].getName

    override def load(path: String): WeightOfEvidenceModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
        .select("rule")
        .head()
      val rule = data.getAs[Map[Int, Map[String, (Double, Double, Double)]]](0)
      val model = new WeightOfEvidenceModel(metadata.uid, rule)
      metadata.getAndSetParams(model)
      model
    }
  }

  /**
   * Returns an `MLReader` instance for this class.
   */
  override def read: MLReader[WeightOfEvidenceModel] = new WeightOfEvidenceModelReader

  override def load(path: String): WeightOfEvidenceModel = super.load(path)
}
