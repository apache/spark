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

import scala.collection.mutable

import org.apache.spark.annotation.Since
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.mllib.feature.{HashingTF => OldHashingTF}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.util.Utils
import org.apache.spark.util.VersionUtils.majorMinorVersion

/**
 * Maps a sequence of terms to their term frequencies using the hashing trick.
 * Currently we use Austin Appleby's MurmurHash 3 algorithm (MurmurHash3_x86_32)
 * to calculate the hash code value for the term object.
 * Since a simple modulo is used to transform the hash function to a column index,
 * it is advisable to use a power of two as the numFeatures parameter;
 * otherwise the features will not be mapped evenly to the columns.
 */
@Since("1.2.0")
class HashingTF @Since("1.4.0") (@Since("1.4.0") override val uid: String)
  extends Transformer with HasInputCol with HasOutputCol with DefaultParamsWritable {

  private var hashFunc: Any => Int = FeatureHasher.murmur3Hash

  @Since("1.2.0")
  def this() = this(Identifiable.randomUID("hashingTF"))

  /** @group setParam */
  @Since("1.4.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.4.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /**
   * Number of features. Should be greater than 0.
   * (default = 2^18^)
   * @group param
   */
  @Since("1.2.0")
  val numFeatures = new IntParam(this, "numFeatures", "number of features (> 0)",
    ParamValidators.gt(0))

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

  setDefault(numFeatures -> (1 << 18), binary -> false)

  /** @group getParam */
  @Since("1.2.0")
  def getNumFeatures: Int = $(numFeatures)

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
    val localNumFeatures = $(numFeatures)
    val localBinary = $(binary)

    val hashUDF = udf { terms: Seq[_] =>
      val termFrequencies = mutable.HashMap.empty[Int, Double].withDefaultValue(0.0)
      terms.foreach { term =>
        val i = indexOf(term)
        if (localBinary) {
          termFrequencies(i) = 1.0
        } else {
          termFrequencies(i) += 1.0
        }
      }
      Vectors.sparse(localNumFeatures, termFrequencies.toSeq)
    }

    dataset.withColumn($(outputCol), hashUDF(col($(inputCol))),
      outputSchema($(outputCol)).metadata)
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
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
    Utils.nonNegativeMod(hashFunc(term), $(numFeatures))
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): HashingTF = defaultCopy(extra)
}

@Since("1.6.0")
object HashingTF extends DefaultParamsReadable[HashingTF] {

  private class HashingTFReader extends MLReader[HashingTF] {

    private val className = classOf[HashingTF].getName

    override def load(path: String): HashingTF = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val hashingTF = new HashingTF(metadata.uid)
      metadata.getAndSetParams(hashingTF)

      // We support loading old `HashingTF` saved by previous Spark versions.
      // Previous `HashingTF` uses `mllib.feature.HashingTF.murmur3Hash`, but new `HashingTF` uses
      // `ml.Feature.FeatureHasher.murmur3Hash`.
      val (majorVersion, _) = majorMinorVersion(metadata.sparkVersion)
      if (majorVersion < 3) {
        hashingTF.hashFunc = OldHashingTF.murmur3Hash
      }
      hashingTF
    }
  }

  @Since("3.0.0")
  override def read: MLReader[HashingTF] = new HashingTFReader

  @Since("1.6.0")
  override def load(path: String): HashingTF = super.load(path)
}
