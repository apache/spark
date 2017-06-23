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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.FuncTransformer.FuncTransformerWriter
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * :: Experimental ::
 * FuncTransformer helps create a custom feature transformer easily for DataFrame, such like
 * conditional conversion(if...else...), type conversion, array indexing and many string ops.
 * Note that FuncTransformer supports serialization via Scala ObjectOutputStream and may not
 * guarantee save/load compatibility between different Scala version.
 * @param func a custom UserDefinedFunction to map from inputCol to outputCol e.g.
 *             udf { (i: Double) => i + 1 }. Only udf with one input is supported for now.
 */
@Experimental
@Since("2.3.0")
class FuncTransformer @Since("2.3.0") (
    @Since("2.3.0") override val uid: String,
    @Since("2.3.0") val func: UserDefinedFunction
  ) extends Transformer with HasInputCol with HasOutputCol with DefaultParamsWritable {

  @Since("2.3.0")
  def this(func: UserDefinedFunction) = this(Identifiable.randomUID("FuncTransformer"), func)

  setDefault(inputCol -> "input", outputCol -> "output")

  /** @group setParam */
  @Since("2.3.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("2.3.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  @Since("2.3.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    dataset.withColumn($(outputCol), func(col($(inputCol))))
  }

  @Since("2.3.0")
  override def transformSchema(schema: StructType): StructType = {
    func.inputTypes match {
      case Some(funcInputType) =>
        require(funcInputType.length == 1, "FuncTransformer only supports udf with one input")
        val dataType = schema($(inputCol)).dataType
        require(dataType == funcInputType.head, s"data type mismatch: udf input type" +
          s" ${funcInputType.head}; inputCol ${$(inputCol)} data type $dataType ")
      case None =>
        val dataType = schema($(inputCol)).dataType
        require(dataType.isInstanceOf[StructType], s"When func input types is None," +
          s" FuncTransformer only supports StructType. ${$(inputCol)} is $dataType")
    }
    val outputFields = schema.fields :+ StructField($(outputCol), func.dataType, false)
    StructType(outputFields)
  }

  @Since("2.3.0")
  override def copy(extra: ParamMap): FuncTransformer = {
    val copied = new FuncTransformer(uid, func)
    copyValues(copied, extra)
  }

  @Since("2.3.0")
  override def write: MLWriter = new FuncTransformerWriter(this)
}

/**
 * :: Experimental ::
 * Companion object for FuncTransformer with save and load function.
 */
@Experimental
@Since("2.3.0")
object FuncTransformer extends DefaultParamsReadable[FuncTransformer] {

  private[FuncTransformer]
  class FuncTransformerWriter(instance: FuncTransformer) extends MLWriter {

    private case class Data(func: Array[Byte])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val bo = new ByteArrayOutputStream()
      new ObjectOutputStream(bo).writeObject(instance.func)
      val data = Data(bo.toByteArray)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class FuncTransformerReader extends MLReader[FuncTransformer] {

    private val className = classOf[FuncTransformer].getName

    override def load(path: String): FuncTransformer = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
      val Row(funcBytes: Array[Byte]) = data.select("func").head()
      val func = new ObjectInputStream(new ByteArrayInputStream(funcBytes)).readObject()
      val model = new FuncTransformer(metadata.uid, func.asInstanceOf[UserDefinedFunction])
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  @Since("2.3.0")
  override def read: MLReader[FuncTransformer] = new FuncTransformerReader

  @Since("2.3.0")
  override def load(path: String): FuncTransformer = super.load(path)
}
