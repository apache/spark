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

import org.apache.spark.annotation.Since
import org.apache.spark.ml._
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.VersionUtils.majorVersion

/**
 * Params for [[IDF]] and [[IDFModel]].
 */
private[feature] trait IDFBase extends Params with HasInputCol with HasOutputCol {

  /**
   * The minimum number of documents in which a term should appear.
   * Default: 0
   * @group param
   */
  final val minDocFreq = new IntParam(
    this, "minDocFreq", "minimum number of documents in which a term should appear for filtering" +
      " (>= 0)", ParamValidators.gtEq(0))

  setDefault(minDocFreq -> 0)

  /** @group getParam */
  def getMinDocFreq: Int = $(minDocFreq)

  /**
   * Validate and transform the input schema.
   */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), new VectorUDT)
    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
  }
}

/**
 * Compute the Inverse Document Frequency (IDF) given a collection of documents.
 */
@Since("1.4.0")
final class IDF @Since("1.4.0") (@Since("1.4.0") override val uid: String)
  extends Estimator[IDFModel] with IDFBase with DefaultParamsWritable {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("idf"))

  /** @group setParam */
  @Since("1.4.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.4.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("1.4.0")
  def setMinDocFreq(value: Int): this.type = set(minDocFreq, value)

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): IDFModel = {
    transformSchema(dataset.schema, logging = true)
    val input: RDD[OldVector] = dataset.select($(inputCol)).rdd.map {
      case Row(v: Vector) => OldVectors.fromML(v)
    }
    val idf = new feature.IDF($(minDocFreq)).fit(input)
    copyValues(new IDFModel(uid, idf).setParent(this))
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): IDF = defaultCopy(extra)
}

@Since("1.6.0")
object IDF extends DefaultParamsReadable[IDF] {

  @Since("1.6.0")
  override def load(path: String): IDF = super.load(path)
}

/**
 * Model fitted by [[IDF]].
 */
@Since("1.4.0")
class IDFModel private[ml] (
    @Since("1.4.0") override val uid: String,
    idfModel: feature.IDFModel)
  extends Model[IDFModel] with IDFBase with MLWritable {

  import IDFModel._

  /** @group setParam */
  @Since("1.4.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.4.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    // TODO: Make the idfModel.transform natively in ml framework to avoid extra conversion.
    val idf = udf { vec: Vector => idfModel.transform(OldVectors.fromML(vec)).asML }
    dataset.withColumn($(outputCol), idf(col($(inputCol))))
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): IDFModel = {
    val copied = new IDFModel(uid, idfModel)
    copyValues(copied, extra).setParent(parent)
  }

  /** Returns the IDF vector. */
  @Since("2.0.0")
  def idf: Vector = idfModel.idf.asML

  /** Returns the document frequency */
  @Since("3.0.0")
  def docFreq: Array[Long] = idfModel.docFreq

  /** Returns number of documents evaluated to compute idf */
  @Since("3.0.0")
  def numDocs: Long = idfModel.numDocs


  @Since("1.6.0")
  override def write: MLWriter = new IDFModelWriter(this)
}

@Since("1.6.0")
object IDFModel extends MLReadable[IDFModel] {

  private[IDFModel] class IDFModelWriter(instance: IDFModel) extends MLWriter {

    private case class Data(idf: Vector, docFreq: Array[Long], numDocs: Long)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.idf, instance.docFreq, instance.numDocs)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class IDFModelReader extends MLReader[IDFModel] {

    private val className = classOf[IDFModel].getName

    override def load(path: String): IDFModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)

      val model = if (majorVersion(metadata.sparkVersion) >= 3) {
        val Row(idf: Vector, df: Seq[_], numDocs: Long) = data.select("idf", "docFreq", "numDocs")
          .head()
        new IDFModel(metadata.uid, new feature.IDFModel(OldVectors.fromML(idf),
          df.asInstanceOf[Seq[Long]].toArray, numDocs))
      } else {
        val Row(idf: Vector) = MLUtils.convertVectorColumnsToML(data, "idf")
          .select("idf")
          .head()
        new IDFModel(metadata.uid,
          new feature.IDFModel(OldVectors.fromML(idf), new Array[Long](idf.size), 0L))
      }
      metadata.getAndSetParams(model)
      model
    }
  }

  @Since("1.6.0")
  override def read: MLReader[IDFModel] = new IDFModelReader

  @Since("1.6.0")
  override def load(path: String): IDFModel = super.load(path)
}
