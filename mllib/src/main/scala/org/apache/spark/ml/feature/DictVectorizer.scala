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

import org.apache.spark.{SparkException}
import org.apache.spark.annotation.Since
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.attribute.{AttributeGroup, NominalAttribute}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCols, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types._



private[feature] trait DictVectorizerBase extends Params with HasInputCols with HasOutputCol{
  val handleInvalid: Param[String] = new Param[String](this, "handleInvalid", "how to handle " +
    "invalid data (unseen labels or NULL values). " +
    "Options are 'skip' (filter out rows with invalid data), error (throw an error), " +
    "or 'keep' (put invalid data in a special additional bucket, at index numLabels).",
    ParamValidators.inArray(DictVectorizer.supportedHandleInvalids))

  setDefault(handleInvalid, DictVectorizer.ERROR_INVALID)

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    val fields = schema($(inputCols).toSet)
    require(fields.map(_.dataType).forall{
      case df => df.isInstanceOf[NumericType] || df.isInstanceOf[StringType]
    })
    val attrGroup = new AttributeGroup($(outputCol))
    SchemaUtils.appendColumn(schema, attrGroup.toStructField())
  }
}


class DictVectorizer(override val uid: String, val sep: String = "=")
  extends Estimator[DictVectorizerModel]
    with HasInputCols with HasOutputCol with DefaultParamsWritable with DictVectorizerBase{
  def this() = this(Identifiable.randomUID("dictVec"))

  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)



  override def fit(dataset: Dataset[_]): DictVectorizerModel = {


    dataset.na.drop($(inputCols)).show()

    val diest_df = dataset.na.drop($(inputCols))
    var labels = ArrayBuilder.make[String]

    dataset.schema($(inputCols).toSet).foreach(p => p.dataType match {
      case IntegerType => labels += p.name
      case StringType => labels ++= dataset.select(p.name).rdd.
          map(_.getString(0)).countByValue().toSeq.sortBy(-_._2).map(key => p.name + sep + key._1)
        case ArrayType(StringType, _) => labels ++= dataset.select(p.name).
          rdd.map(_.getAs[Seq[String]](0)).flatMap(y => y).
          countByValue().toSeq.sortBy(-_._2).map(key => p.name + sep + key._1)
          case ArrayType(t, true) => t match {
        case IntegerType => false
        case DoubleType => false
        case LongType => false
      }
      case _ =>
        throw new SparkException(s"un supported column : ${p.name}.  To handle unseen labels, " +
          s"set Param handleInvalid to ${DictVectorizer.KEEP_INVALID}.")

    })

    // println(labels.result().mkString(","))

    new DictVectorizerModel("x", labels.result())
  }

  override def copy(extra: ParamMap): DictVectorizer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}

class DictVectorizerModel( val uid: String, val vocabulary: Array[String],
                          val sep: String = "=") extends Model[DictVectorizerModel]
    with DictVectorizerBase{


  def this(labels: Array[String]) = this(Identifiable.randomUID("dictVec"), labels)

  override def copy(extra: ParamMap): DictVectorizerModel = {
    defaultCopy(extra)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val os = validateAndTransformSchema((dataset.schema($(inputCols).toSet)))
    val inputFields = $(inputCols).map(c => dataset.schema(c))
    dataset.select(getOutputCol)
  }


  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}


object DictVectorizer extends DefaultParamsReadable[DictVectorizer] {
  private[feature] val SKIP_INVALID: String = "skip"
  private[feature] val ERROR_INVALID: String = "error"
  private[feature] val KEEP_INVALID: String = "keep"
  private[feature] val supportedHandleInvalids: Array[String] =
    Array(SKIP_INVALID, ERROR_INVALID, KEEP_INVALID)

  @Since("1.6.0")
  override def load(path: String): DictVectorizer = super.load(path)
}
