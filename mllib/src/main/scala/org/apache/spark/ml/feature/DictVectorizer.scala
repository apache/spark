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

import scala.collection.{immutable, mutable}
import scala.collection.mutable.{ArrayBuilder, HashMap}

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Since
import org.apache.spark.SparkException
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.attribute.{AttributeGroup, NominalAttribute}
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCols, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.OpenHashMap






private[feature] trait DictVectorizerBase extends Params with HasInputCols with HasOutputCol{
  val handleInvalid: Param[String] = new Param[String](this, "handleInvalid", "how to handle " +
    "invalid data (unseen labels or NULL values). " +
    "Options are 'skip' (filter out rows with invalid data), error (throw an error), " +
    "or 'keep' (put invalid data in a special additional bucket, at index numLabels).",
    ParamValidators.inArray(DictVectorizer.supportedHandleInvalids))

  setDefault(handleInvalid, DictVectorizer.SKIP_INVALID)

  val sepToken: Param[String] = new Param[String](this, "sepToken",
    "split token between column names and column value")

  setDefault(sepToken, DictVectorizer.DEFAULT_SEP_TOKEN)

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    val fields = schema($(inputCols).toSet)
    require(fields.map(_.dataType).forall{
      case df: NumericType => true
      case df: BooleanType => true
      case df: StringType => true
      case df: ArrayType => df match {
        case ArrayType(_: NumericType, _) => true
        case ArrayType(StringType, _) => true
      }
      case df => false
    }, "some filed type not supported")
    val attrGroup = new AttributeGroup($(outputCol))
    SchemaUtils.appendColumn(schema, attrGroup.toStructField())
  }

  def strToLabel(s: String, col: String): String = {
    col + $(sepToken) + s
  }

  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)

  def setSepToken(value: String): this.type = set(sepToken, value)
}


class DictVectorizer(override val uid: String)
  extends Estimator[DictVectorizerModel]
    with HasInputCols with HasOutputCol with DefaultParamsWritable with DictVectorizerBase{
  def this() = this(Identifiable.randomUID("dictVec"))

  override def fit(dataset: Dataset[_]): DictVectorizerModel = {

    val pruned_df = dataset.na.drop($(inputCols))
    var labels = ArrayBuilder.make[String]
    var labelDimensions = new HashMap[String, Int]()

    dataset.schema($(inputCols).toSet).foreach(p => p.dataType match {
      case _: NumericType|BooleanType =>
        labels += p.name
      case StringType =>
        pruned_df.select(p.name).distinct().collect().foreach{
          case Row(v: String) =>
          labels += strToLabel(v, p.name)
      }
      case ArrayType(v, _) =>
        v match {
          case StringType =>
            pruned_df.select(explode(col(p.name))).distinct().collect().foreach{
              case Row(x: String) =>
                labels += strToLabel(x, p.name)
            }
          case BooleanType =>
            labels += p.name
            val maxDimension = pruned_df.agg(max(size(col(p.name)))).head().getAs[Int](0)
            labelDimensions.update(p.name, maxDimension)
          case _: NumericType =>
            labels += p.name
            val maxDimension = pruned_df.agg(max(size(col(p.name)))).head().getAs[Int](0)
            labelDimensions.update(p.name, maxDimension)
        }
      case _ =>
        throw new SparkException(s"Unsupported column : ${p.name}. with type: ${p.dataType}" )
    })

    val sortedLabels = labels.result().sorted

    require(sortedLabels.length > 0, "Empty columns encountered")

    copyValues(new DictVectorizerModel(uid, sortedLabels, labelDimensions).setParent(this))
  }

  override def copy(extra: ParamMap): DictVectorizer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}

class DictVectorizerModel( val uid: String, val vocabulary: Array[String],
                           val vocabDimension: HashMap[String, Int] =
                           new HashMap[String, Int]())
  extends Model[DictVectorizerModel]
    with DictVectorizerBase with MLWritable{

  import org.apache.spark.ml.feature.DictVectorizerModel._


  private val labelToIndex: OpenHashMap[String, Int] = {
    val map = new OpenHashMap[String, Int]
    var i = 0
    for(v <- vocabulary) {
      map.update(v, i)
      i += vocabDimension.getOrElse(v, 1)
    }
    map
  }

  def getLabelIndex(label: String): Int = {
      if (labelToIndex.contains(label)) {
        labelToIndex(label)
    } else {
      -1
    }
  }

  def getSepToken: String = $(sepToken)

  def this(vocabulary: Array[String]) = {
    this(Identifiable.randomUID("dictVec"), vocabulary)
  }

  def this(vocabulary: Array[String], vocabDimension: HashMap[String, Int]) = {
    this(Identifiable.randomUID("dictVec"), vocabulary, vocabDimension)
  }

  override def copy(extra: ParamMap): DictVectorizerModel = {
    val copied = new DictVectorizerModel(uid, vocabulary, vocabDimension)
    copyValues(copied, extra).setParent(parent)
  }



  override def transform(dataset: Dataset[_]): DataFrame = {
    def vec(r: Row): Vector = {
      val indices = ArrayBuilder.make[Int]
      val values = ArrayBuilder.make[Double]

      dataset.schema($(inputCols).toSet).foreach(p => {
        p.dataType match {
          case IntegerType =>
            val v = r.getAs[Int](p.name)
            if ( v != 0 )
            {
              val newIndice = getLabelIndex(p.name)
              if (newIndice >= 0) {
                indices += newIndice
                values += v.asInstanceOf[Double]
              }

            }
          case DoubleType =>
            val v = r.getAs[Double](p.name)
            if ( v != 0 ) {
              val newIndice = getLabelIndex(p.name)
              if (newIndice >= 0) {
                indices += newIndice
                values += v
              }
            }
          case BooleanType =>
            val v = r.getAs[Boolean](p.name)
            if(v) {
              val newIndice = getLabelIndex(p.name)
              if (newIndice >= 0) {
                indices += newIndice
                values += 1.0
              }
            }

          case StringType =>
            val v = r.getAs[String](p.name)
            val newIndice = getLabelIndex(strToLabel(v, p.name))
            if (newIndice >= 0) {
              indices += newIndice
              values += 1.0
            }
          case ArrayType(ele, _) =>
            ele match {
              case IntegerType =>
                val v = r.getAs[Seq[Int]](p.name)
                val baseIndice = labelToIndex(p.name)
                val limit = vocabDimension(p.name)
                for(i <- 0 until v.length)
                {
                  if (v(i) != 0 && i < limit)
                  {
                    indices += (baseIndice + i)
                    values += v(i)
                  }
                }
              case DoubleType =>
                val v = r.getAs[Seq[Double]](p.name)
                val baseIndice = labelToIndex(p.name)
                val limit = vocabDimension(p.name)
                for(i <- 0 until v.length)
                {
                  if (v(i) != 0 && i < limit)
                  {
                    indices += (baseIndice + i)
                    values += v(i)
                  }
                }
              case StringType =>
                val v = r.getAs[Seq[String]](p.name)
                v.foreach {
                  s =>
                    val newIndice = getLabelIndex(strToLabel(s, p.name))
                    if (newIndice >= 0) {
                      indices += newIndice
                      values += 1.0
                    }
                }
              case BooleanType =>
                val v = r.getAs[Seq[Boolean]](p.name)
                val baseIndice = labelToIndex(p.name)
                val limit = vocabDimension(p.name)
                for(i <- 0 until v.length)
                {
                  if (v(i) && i < limit)
                  {
                    indices += (baseIndice + i)
                    values += 1.0
                  }
                }
            }
        }
      })

      val indices_and_values = (indices.result() zip(values.result()) sortBy(_._1)).unzip
      val colSize = vocabulary.map{p => vocabDimension.getOrElse(p, 1)}.sum
      Vectors.sparse(colSize, indices_and_values._1, indices_and_values._2).compressed
    }

    val vectorizer = udf {
      r: Row => vec(r)
    }


    val os = validateAndTransformSchema((dataset.schema($(inputCols).toSet)))

    val metadata = NominalAttribute.defaultAttr.toMetadata()
    val args = $(inputCols).map { c => dataset(c)}
    dataset.select(col("*"), vectorizer(struct(args: _*)).as($(outputCol), metadata))
  }


  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }


  override def write: MLWriter = new DictVectorizerModelWriter(this)
}

@Since("1.6.0")
object DictVectorizerModel extends MLReadable[DictVectorizerModel] {

  private[DictVectorizerModel]
  class DictVectorizerModelWriter(instance: DictVectorizerModel) extends MLWriter {

    private case class Data(vocabulary: Array[String], vocabDimension: immutable.Map[String, Int])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.vocabulary, instance.vocabDimension.toMap)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }


  private class DictVectorizerModelReader extends MLReader[DictVectorizerModel] {

    private val className = classOf[DictVectorizerModel].getName

    override def load(path: String): DictVectorizerModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
        .select("vocabulary", "vocabDimension")
        .head()
      val vocabulary = data.getAs[Seq[String]]("vocabulary").toArray
      val vocabDimension = mutable.HashMap(data.getAs[Map[String, Int]]("vocabDimension").toSeq: _*)

      val model = new DictVectorizerModel(metadata.uid, vocabulary, vocabDimension)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  @Since("1.6.0")
  override def read: MLReader[DictVectorizerModel] = new DictVectorizerModelReader

  @Since("1.6.0")
  override def load(path: String): DictVectorizerModel = super.load(path)
}



object DictVectorizer extends DefaultParamsReadable[DictVectorizer] {
  private[feature] val SKIP_INVALID: String = "skip"
  private[feature] val ERROR_INVALID: String = "error"
  private[feature] val KEEP_INVALID: String = "keep"
  private[feature] val DEFAULT_SEP_TOKEN: String = "="
  private[feature] val supportedHandleInvalids: Array[String] =
    Array(SKIP_INVALID, ERROR_INVALID, KEEP_INVALID)

  @Since("1.6.0")
  override def load(path: String): DictVectorizer = super.load(path)
}
