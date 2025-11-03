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

import org.apache.spark.annotation.Since
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasNumFeatures, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.mllib.feature.{HashingTF => OldHashingTF}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.util.Utils
import org.apache.spark.util.VersionUtils.majorMinorVersion
import org.apache.spark.util.collection.OpenHashMap

/**
 * Maps a sequence of terms to their term frequencies using the hashing trick.
 * Currently we use Austin Appleby's MurmurHash 3 algorithm (MurmurHash3_x86_32)
 * to calculate the hash code value for the term object.
 * Since a simple modulo is used to transform the hash function to a column index,
 * it is advisable to use a power of two as the numFeatures parameter;
 * otherwise the features will not be mapped evenly to the columns.
 */
@Since("1.2.0")
class HashingTF @Since("3.0.0") private[ml] (
    @Since("1.4.0") override val uid: String,
    @Since("3.1.0") val hashFuncVersion: Int)
  extends Transformer with HasInputCol with HasOutputCol with HasNumFeatures
    with DefaultParamsWritable {

  @Since("1.2.0")
  def this() = this(Identifiable.randomUID("hashingTF"), HashingTF.SPARK_3_MURMUR3_HASH)

  @Since("1.4.0")
  def this(uid: String) = this(uid, hashFuncVersion = HashingTF.SPARK_3_MURMUR3_HASH)

  /** @group setParam */
  @Since("1.4.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.4.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /**
   * Binary toggle to control term frequency counts.
   * If true, all non-zero counts are set to 1.  This is useful for discrete probabilistic
   * models that model binary events rather than integer counts.
   * (default = false)
   * @group param
   */
  @Since("2.0.0")
  val binary = new BooleanParam(this, "binary", "If true, all non zero counts are set to 1. " +
    "This is useful for discrete probabilistic models that model binary events rather " +
    "than integer counts")

  setDefault(binary -> false)

  /** @group setParam */
  @Since("1.2.0")
  def setNumFeatures(value: Int): this.type = set(numFeatures, value)

  /** @group getParam */
  @Since("2.0.0")
  def getBinary: Boolean = $(binary)

  /** @group setParam */
  @Since("2.0.0")
  def setBinary(value: Boolean): this.type = set(binary, value)

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema)
    val n = $(numFeatures)
    val updateFunc = if ($(binary)) (v: Double) => 1.0 else (v: Double) => v + 1.0

    val hashUDF = udf { terms: Seq[_] =>
      val map = new OpenHashMap[Int, Double]()
      terms.foreach { term => map.changeValue(indexOf(term), 1.0, updateFunc) }
      Vectors.sparse(n, map.toSeq)
    }

    dataset.withColumn($(outputCol), hashUDF(col($(inputCol))),
      outputSchema($(outputCol)).metadata)
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    val inputType = SchemaUtils.getSchemaFieldType(schema, $(inputCol))
    require(inputType.isInstanceOf[ArrayType],
      s"The input column must be ${ArrayType.simpleString}, but got ${inputType.catalogString}.")
    val attrGroup = new AttributeGroup($(outputCol), $(numFeatures))
    SchemaUtils.appendColumn(schema, attrGroup.toStructField())
  }

  /**
   * Returns the index of the input term.
   */
  @Since("3.0.0")
  def indexOf(term: Any): Int = {
    val hashValue = hashFuncVersion match {
      case HashingTF.SPARK_2_MURMUR3_HASH => OldHashingTF.murmur3Hash(term)
      case HashingTF.SPARK_3_MURMUR3_HASH => FeatureHasher.murmur3Hash(term)
      case _ => throw new IllegalArgumentException("Illegal hash function version setting.")
    }
    Utils.nonNegativeMod(hashValue, $(numFeatures))
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): HashingTF = defaultCopy(extra)

  @Since("3.0.0")
  override def toString: String = {
    s"HashingTF: uid=$uid, binary=${$(binary)}, numFeatures=${$(numFeatures)}"
  }

  @Since("3.0.0")
  override def save(path: String): Unit = {
    require(hashFuncVersion == HashingTF.SPARK_3_MURMUR3_HASH,
      "Cannot save model which is loaded from lower version spark saved model. We can address " +
      "it by (1) use old spark version to save the model, or (2) use new version spark to " +
      "re-train the pipeline.")
    super.save(path)
  }
}

@Since("1.6.0")
object HashingTF extends DefaultParamsReadable[HashingTF] {

  private[ml] val SPARK_2_MURMUR3_HASH = 1
  private[ml] val SPARK_3_MURMUR3_HASH = 2

  private class HashingTFReader extends MLReader[HashingTF] {

    private val className = classOf[HashingTF].getName

    override def load(path: String): HashingTF = {
      val metadata = DefaultParamsReader.loadMetadata(path, sparkSession, className)

      // We support loading old `HashingTF` saved by previous Spark versions.
      // Previous `HashingTF` uses `mllib.feature.HashingTF.murmur3Hash`, but new `HashingTF` uses
      // `ml.Feature.FeatureHasher.murmur3Hash`.
      val (majorVersion, _) = majorMinorVersion(metadata.sparkVersion)
      val hashFuncVersion = if (majorVersion < 3) {
        SPARK_2_MURMUR3_HASH
      } else {
        SPARK_3_MURMUR3_HASH
      }
      val hashingTF = new HashingTF(metadata.uid, hashFuncVersion = hashFuncVersion)
      metadata.getAndSetParams(hashingTF)
      hashingTF
    }
  }

  @Since("3.0.0")
  override def read: MLReader[HashingTF] = new HashingTFReader

  @Since("1.6.0")
  override def load(path: String): HashingTF = super.load(path)
}
