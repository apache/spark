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
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.{IntParam, ParamMap, ParamValidators, StringArrayParam}
import org.apache.spark.ml.param.shared.{HasInputCols, HasOutputCol}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable, SchemaUtils}
import org.apache.spark.mllib.feature.{HashingTF => OldHashingTF}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.hash.Murmur3_x86_32.{hashInt, hashLong, hashUnsafeBytes2}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.OpenHashMap

/**
 * Feature hashing projects a set of categorical or numerical features into a feature vector of
 * specified dimension (typically substantially smaller than that of the original feature
 * space). This is done using the hashing trick (https://en.wikipedia.org/wiki/Feature_hashing)
 * to map features to indices in the feature vector.
 *
 * The [[FeatureHasher]] transformer operates on multiple columns. Each column may contain either
 * numeric or categorical features. Behavior and handling of column data types is as follows:
 *  -Numeric columns: For numeric features, the hash value of the column name is used to map the
 *                    feature value to its index in the feature vector. By default, numeric features
 *                    are not treated as categorical (even when they are integers). To treat them
 *                    as categorical, specify the relevant columns in `categoricalCols`.
 *  -String columns: For categorical features, the hash value of the string "column_name=value"
 *                   is used to map to the vector index, with an indicator value of `1.0`.
 *                   Thus, categorical features are "one-hot" encoded
 *                   (similarly to using [[OneHotEncoder]] with `dropLast=false`).
 *  -Boolean columns: Boolean values are treated in the same way as string columns. That is,
 *                    boolean features are represented as "column_name=true" or "column_name=false",
 *                    with an indicator value of `1.0`.
 *
 * Null (missing) values are ignored (implicitly zero in the resulting feature vector).
 *
 * The hash function used here is also the MurmurHash 3 used in [[HashingTF]]. Since a simple modulo
 * on the hashed value is used to determine the vector index, it is advisable to use a power of two
 * as the numFeatures parameter; otherwise the features will not be mapped evenly to the vector
 * indices.
 *
 * {{{
 *   val df = Seq(
 *    (2.0, true, "1", "foo"),
 *    (3.0, false, "2", "bar")
 *   ).toDF("real", "bool", "stringNum", "string")
 *
 *   val hasher = new FeatureHasher()
 *    .setInputCols("real", "bool", "stringNum", "string")
 *    .setOutputCol("features")
 *
 *   hasher.transform(df).show(false)
 *
 *   +----+-----+---------+------+------------------------------------------------------+
 *   |real|bool |stringNum|string|features                                              |
 *   +----+-----+---------+------+------------------------------------------------------+
 *   |2.0 |true |1        |foo   |(262144,[51871,63643,174475,253195],[1.0,1.0,2.0,1.0])|
 *   |3.0 |false|2        |bar   |(262144,[6031,80619,140467,174475],[1.0,1.0,1.0,3.0]) |
 *   +----+-----+---------+------+------------------------------------------------------+
 * }}}
 */
@Experimental
@Since("2.3.0")
class FeatureHasher(@Since("2.3.0") override val uid: String) extends Transformer
  with HasInputCols with HasOutputCol with DefaultParamsWritable {

  @Since("2.3.0")
  def this() = this(Identifiable.randomUID("featureHasher"))

  /**
   * Numeric columns to treat as categorical features. By default only string and boolean
   * columns are treated as categorical, so this param can be used to explicitly specify the
   * numerical columns to treat as categorical. Note, the relevant columns must also be set in
   * `inputCols`.
   * @group param
   */
  @Since("2.3.0")
  val categoricalCols = new StringArrayParam(this, "categoricalCols",
    "numeric columns to treat as categorical")

  /**
   * Number of features. Should be greater than 0.
   * (default = 2^18^)
   * @group param
   */
  @Since("2.3.0")
  val numFeatures = new IntParam(this, "numFeatures", "number of features (> 0)",
    ParamValidators.gt(0))

  setDefault(numFeatures -> (1 << 18))

  /** @group getParam */
  @Since("2.3.0")
  def getNumFeatures: Int = $(numFeatures)

  /** @group setParam */
  @Since("2.3.0")
  def setNumFeatures(value: Int): this.type = set(numFeatures, value)

  /** @group setParam */
  @Since("2.3.0")
  def setInputCols(values: String*): this.type = setInputCols(values.toArray)

  /** @group setParam */
  @Since("2.3.0")
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  /** @group setParam */
  @Since("2.3.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group getParam */
  @Since("2.3.0")
  def getCategoricalCols: Array[String] = $(categoricalCols)

  /** @group setParam */
  @Since("2.3.0")
  def setCategoricalCols(value: Array[String]): this.type = set(categoricalCols, value)

  @Since("2.3.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val hashFunc: Any => Int = FeatureHasher.murmur3Hash
    val n = $(numFeatures)
    val localInputCols = $(inputCols)
    val catCols = if (isSet(categoricalCols)) {
      $(categoricalCols).toSet
    } else {
      Set[String]()
    }

    val outputSchema = transformSchema(dataset.schema)
    val realFields = outputSchema.fields.filter { f =>
      f.dataType.isInstanceOf[NumericType] && !catCols.contains(f.name)
    }.map(_.name).toSet

    def getDouble(x: Any): Double = {
      x match {
        case n: java.lang.Number =>
          n.doubleValue()
        case other =>
          // will throw ClassCastException if it cannot be cast, as would row.getDouble
          other.asInstanceOf[Double]
      }
    }

    val hashFeatures = udf { row: Row =>
      val map = new OpenHashMap[Int, Double]()
      localInputCols.foreach { colName =>
        val fieldIndex = row.fieldIndex(colName)
        if (!row.isNullAt(fieldIndex)) {
          val (rawIdx, value) = if (realFields(colName)) {
            // numeric values are kept as is, with vector index based on hash of "column_name"
            val value = getDouble(row.get(fieldIndex))
            val hash = hashFunc(colName)
            (hash, value)
          } else {
            // string, boolean and numeric values that are in catCols are treated as categorical,
            // with an indicator value of 1.0 and vector index based on hash of "column_name=value"
            val value = row.get(fieldIndex).toString
            val fieldName = s"$colName=$value"
            val hash = hashFunc(fieldName)
            (hash, 1.0)
          }
          val idx = Utils.nonNegativeMod(rawIdx, n)
          map.changeValue(idx, value, v => v + value)
        }
      }
      Vectors.sparse(n, map.toSeq)
    }

    val metadata = outputSchema($(outputCol)).metadata
    dataset.select(
      col("*"),
      hashFeatures(struct($(inputCols).map(col): _*)).as($(outputCol), metadata))
  }

  @Since("2.3.0")
  override def copy(extra: ParamMap): FeatureHasher = defaultCopy(extra)

  @Since("2.3.0")
  override def transformSchema(schema: StructType): StructType = {
    val fields = schema($(inputCols).toSet)
    fields.foreach { fieldSchema =>
      val dataType = fieldSchema.dataType
      val fieldName = fieldSchema.name
      require(dataType.isInstanceOf[NumericType] ||
        dataType.isInstanceOf[StringType] ||
        dataType.isInstanceOf[BooleanType],
        s"FeatureHasher requires columns to be of ${NumericType.simpleString}, " +
          s"${BooleanType.catalogString} or ${StringType.catalogString}. " +
          s"Column $fieldName was ${dataType.catalogString}")
    }
    val attrGroup = new AttributeGroup($(outputCol), $(numFeatures))
    SchemaUtils.appendColumn(schema, attrGroup.toStructField())
  }
}

@Since("2.3.0")
object FeatureHasher extends DefaultParamsReadable[FeatureHasher] {

  @Since("2.3.0")
  override def load(path: String): FeatureHasher = super.load(path)

  private val seed = OldHashingTF.seed

  /**
   * Calculate a hash code value for the term object using
   * Austin Appleby's MurmurHash 3 algorithm (MurmurHash3_x86_32).
   * This is the default hash algorithm used from Spark 2.0 onwards.
   * Use hashUnsafeBytes2 to match the original algorithm with the value.
   * See SPARK-23381.
   */
  @Since("2.3.0")
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
        hashUnsafeBytes2(utf8.getBaseObject, utf8.getBaseOffset, utf8.numBytes(), seed)
      case _ => throw new SparkException("FeatureHasher with murmur3 algorithm does not " +
        s"support type ${term.getClass.getCanonicalName} of input data.")
    }
  }
}
