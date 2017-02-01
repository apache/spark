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

import org.apache.spark.SparkException
import org.apache.spark.annotation.Since
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.{IntParam, ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared.{HasInputCol, HasInputCols, HasOutputCol}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable, SchemaUtils}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.hash.Murmur3_x86_32._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.OpenHashMap


@Since("2.2.0")
class FeatureHasher(@Since("2.2.0") override val uid: String) extends Transformer
  with HasInputCols with HasOutputCol with DefaultParamsWritable {

  @Since("2.2.0")
  def this() = this(Identifiable.randomUID("featureHasher"))

  /**
   * Number of features. Should be > 0.
   * (default = 2^18^)
   * @group param
   */
  @Since("2.2.0")
  val numFeatures = new IntParam(this, "numFeatures", "number of features (> 0)",
    ParamValidators.gt(0))

  setDefault(numFeatures -> (1 << 18))

  /** @group getParam */
  @Since("2.2.0")
  def getNumFeatures: Int = $(numFeatures)

  /** @group setParam */
  @Since("2.2.0")
  def setNumFeatures(value: Int): this.type = set(numFeatures, value)

  /** @group setParam */
  @Since("2.2.0")
  def setInputCols(values: String*): this.type = setInputCols(values.toArray)

  /** @group setParam */
  @Since("2.2.0")
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  /** @group setParam */
  @Since("2.2.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val os = transformSchema(dataset.schema)

    val inputFields = $(inputCols).map(c => dataset.schema(c))
    val featureCols = inputFields.map { f =>
      f.dataType match {
        case DoubleType | StringType => dataset(f.name)
        case _: NumericType | BooleanType => dataset(f.name).cast(DoubleType).alias(f.name)
      }
    }

    val realFields = os.fields.filter(f => f.dataType.isInstanceOf[NumericType]).map(_.name).toSet
    val hashFunc: Any => Int = FeatureHasher.murmur3Hash

    def hashFeatures = udf { row: Row =>
      val map = new OpenHashMap[Int, Double]()
      $(inputCols).foreach { case field =>
        val (rawIdx, value) = if (realFields(field)) {
          val value = row.getDouble(row.fieldIndex(field))
          val hash = hashFunc(field)
          (hash, value)
        } else {
          val value = row.getString(row.fieldIndex(field))
          val fieldName = s"$field=$value"
          val hash = hashFunc(fieldName)
          (hash, 1.0)
        }
        val idx = Utils.nonNegativeMod(rawIdx, $(numFeatures))
        map.changeValue(idx, value, v => v + value)
        (idx, value)
      }
      Vectors.sparse($(numFeatures), map.toSeq)
    }

    dataset.select(
      col("*"),
      hashFeatures(struct(featureCols: _*)).as($(outputCol))) // , featureAttrs.toMetadata()))

    /*
    dataset.select($(inputCols).map(col(_)): _*).map { case row =>
      val map = new OpenHashMap[Int, Double]()
      val s = $(inputCols).zipWithIndex.map { case (field, i) =>
        if (realFields(field)) {
          val value = row.getDouble(i)
          val hash = hashFunc(field)
          val idx = Utils.nonNegativeMod(hash, $(numFeatures))
          (idx, value)
        } else {
          val value = row.getString(i)
          val idx = Utils.nonNegativeMod(hashFunc(value), $(numFeatures))
          (idx, 1.0)
        }
      }
      Row(Vectors.sparse($(numFeatures), s.toSeq))
    }.toDF($(outputCol))
    */
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    val fields = schema($(inputCols).toSet)
    require(fields.map(_.dataType).forall { case dt =>
      dt.isInstanceOf[NumericType] || dt.isInstanceOf[StringType]
    })
    val attrGroup = new AttributeGroup($(outputCol), $(numFeatures))
    SchemaUtils.appendColumn(schema, attrGroup.toStructField())
  }
}

object FeatureHasher {

  private val seed = 42

  /**
   * Calculate a hash code value for the term object using
   * Austin Appleby's MurmurHash 3 algorithm (MurmurHash3_x86_32).
   * This is the default hash algorithm used from Spark 2.0 onwards.
   */
  private[feature] def murmur3Hash(term: Any): Int = {
    term match {
      case null => seed
      case b: Boolean => hashInt(if (b) 1 else 0, seed)
      case b: Byte => hashInt(b, seed)
      case s: Short => hashInt(s, seed)
      case i: Int => hashInt(i, seed)
      case l: Long => hashLong(l, seed)
      case f: Float => hashInt(java.lang.Float.floatToIntBits(f), seed)
      case d: Double => hashLong(java.lang.Double.doubleToLongBits(d), seed)
      case s: String =>
        val utf8 = UTF8String.fromString(s)
        hashUnsafeBytes(utf8.getBaseObject, utf8.getBaseOffset, utf8.numBytes(), seed)
      case _ => throw new SparkException("HashingTF with murmur3 algorithm does not " +
        s"support type ${term.getClass.getCanonicalName} of input data.")
    }
  }
}
