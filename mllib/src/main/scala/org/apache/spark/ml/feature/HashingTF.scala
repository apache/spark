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
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.mllib.feature
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{ArrayType, StructType}

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
    val hashingTF = new feature.HashingTF($(numFeatures)).setBinary($(binary))
    // TODO: Make the hashingTF.transform natively in ml framework to avoid extra conversion.
    val t = udf { terms: Seq[_] => hashingTF.transform(terms).asML }
    val metadata = outputSchema($(outputCol)).metadata
    dataset.select(col("*"), t(col($(inputCol))).as($(outputCol), metadata))
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.isInstanceOf[ArrayType],
      s"The input column must be ArrayType, but got $inputType.")
    val attrGroup = new AttributeGroup($(outputCol), $(numFeatures))
    SchemaUtils.appendColumn(schema, attrGroup.toStructField())
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): HashingTF = defaultCopy(extra)
}

@Since("1.6.0")
object HashingTF extends DefaultParamsReadable[HashingTF] {

  @Since("1.6.0")
  override def load(path: String): HashingTF = super.load(path)
}
