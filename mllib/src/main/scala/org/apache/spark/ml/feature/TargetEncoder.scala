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
import org.apache.spark.ml.feature.TargetEncoderModel.TargetEncoderModelWriter
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasHandleInvalid, HasInputCols, HasLabelCol, HasOutputCols}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsReader, DefaultParamsWritable, DefaultParamsWriter, Identifiable, MLReadable, MLReader, MLWritable, MLWriter}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{NumericType, StringType, StructType}

private[ml] trait TargetEncoderParams extends HasInputCols with HasOutputCols
  with HasLabelCol with HasHandleInvalid {

  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  def setOutputCols(value: Array[String]): this.type = set(outputCols, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)
  setDefault(handleInvalid, TargetEncoder.ERROR_INVALID)

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

class TargetEncoder(override val uid: String) extends Estimator[TargetEncoderModel]
  with TargetEncoderParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("targetEncoder"))

  override def fit(dataset: Dataset[_]): TargetEncoderModel = {

    transformSchema(dataset.schema, logging = true)
    val probabilityOf1 = dataset.na.drop($(inputCols))
      .select($(inputCols).map(col(_).cast(StringType)) ++ Array(col($(labelCol))): _*)
      .rdd
      .flatMap(row => for (i <- 0 until row.size - 1) yield {
        // ((colIndex,value), label)
        ((i, row.getAs[String](i)), row.getAs[Double](row.size - 1))
      })
      .aggregateByKey[(Long, Long)]((0L, 0L))(
        seqOp = {
          case ((cnt0, cnt1), label) =>
            if (label == 0D) {
              (cnt0 + 1, cnt1)
            } else if (label == 1D) {
              (cnt0, cnt1 + 1)
            } else throw new UnsupportedOperationException(
              s"Classification labels should be 0 or 1 but found $label.")
        },
        combOp = {
          case ((thisCnt0, thisCnt1), (otherCnt0, otherCnt1)) =>
            (thisCnt0 + otherCnt0, thisCnt1 + otherCnt1)
        }
      )
      .map {
        case ((index, v), (cnt0, cnt1)) =>
          val prob = cnt1 / (cnt0 + cnt1).toDouble

          ((index, v), prob)
      }.collect().toMap


    copyValues(new TargetEncoderModel(uid, probabilityOf1)).setParent(this)

  }

  override def copy(extra: ParamMap): Estimator[TargetEncoderModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)
}

object TargetEncoder extends DefaultParamsReadable[TargetEncoder] {

  private[feature] val SKIP_INVALID: String = "skip"
  private[feature] val ERROR_INVALID: String = "error"

  override def load(path: String): TargetEncoder = super.load(path)

}

class TargetEncoderModel private[ml](override val uid: String,
                                     val probOf1: Map[(Int, String), Double])
  extends Model[TargetEncoderModel] with TargetEncoderParams with MLWritable {

  override def copy(extra: ParamMap): TargetEncoderModel = {
    defaultCopy[TargetEncoderModel](extra).setParent(parent)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {

    val transformedSchema = transformSchema(dataset.schema)

    val seq: Seq[UserDefinedFunction] = getInputCols.zipWithIndex.map { case (_, idx) =>
      udf { (feature: String) => probOf1((idx, feature)) }
    }

    val newCols = getInputCols.zipWithIndex.map { case (inputCol, idx) =>
      seq(idx)(col(inputCol).cast(StringType))
    }

    val metadata = getOutputCols.map { col =>
      transformedSchema(col).metadata
    }

    val filteredData = getHandleInvalid match {
      case TargetEncoder.SKIP_INVALID =>
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
  override def write: MLWriter = new TargetEncoderModelWriter(this)
}

object TargetEncoderModel extends MLReadable[TargetEncoderModel] {

  private[TargetEncoderModel]
  class TargetEncoderModelWriter(instance: TargetEncoderModel) extends MLWriter {

    private case class Data(probMaps: Map[(Int, String), Double])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.probOf1)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class TargetEncoderModelReader extends MLReader[TargetEncoderModel] {

    private val className = classOf[TargetEncoderModel].getName

    override def load(path: String): TargetEncoderModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
        .select("probOf1")
        .head()
      val probOf1 = data.getAs[Map[(Int, String), Double]](0)
      val model = new TargetEncoderModel(metadata.uid, probOf1)
      metadata.getAndSetParams(model)
      model
    }
  }

  /**
   * Returns an `MLReader` instance for this class.
   */
  override def read: MLReader[TargetEncoderModel] = new TargetEncoderModelReader

  override def load(path: String): TargetEncoderModel = super.load(path)

}