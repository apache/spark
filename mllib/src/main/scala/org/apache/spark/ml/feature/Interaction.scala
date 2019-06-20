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

import org.apache.spark.SparkException
import org.apache.spark.annotation.Since
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.linalg.{Vector, Vectors, VectorUDT}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Implements the feature interaction transform. This transformer takes in Double and Vector type
 * columns and outputs a flattened vector of their feature interactions. To handle interaction,
 * we first one-hot encode any nominal features. Then, a vector of the feature cross-products is
 * produced.
 *
 * For example, given the input feature values `Double(2)` and `Vector(3, 4)`, the output would be
 * `Vector(6, 8)` if all input features were numeric. If the first feature was instead nominal
 * with four categories, the output would then be `Vector(0, 0, 0, 0, 3, 4, 0, 0)`.
 */
@Since("1.6.0")
class Interaction @Since("1.6.0") (@Since("1.6.0") override val uid: String) extends Transformer
  with HasInputCols with HasOutputCol with DefaultParamsWritable {

  @Since("1.6.0")
  def this() = this(Identifiable.randomUID("interaction"))

  /** @group setParam */
  @Since("1.6.0")
  def setInputCols(values: Array[String]): this.type = set(inputCols, values)

  /** @group setParam */
  @Since("1.6.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  // optimistic schema; does not contain any ML attributes
  @Since("1.6.0")
  override def transformSchema(schema: StructType): StructType = {
    require(get(inputCols).isDefined, "Input cols must be defined first.")
    require(get(outputCol).isDefined, "Output col must be defined first.")
    require($(inputCols).length > 0, "Input cols must have non-zero length.")
    require($(inputCols).distinct.length == $(inputCols).length, "Input cols must be distinct.")
    StructType(schema.fields :+ StructField($(outputCol), new VectorUDT, false))
  }

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val inputFeatures = $(inputCols).map(c => dataset.schema(c))
    val featureEncoders = getFeatureEncoders(inputFeatures)
    val featureAttrs = getFeatureAttrs(inputFeatures)

    def interactFunc = udf { row: Row =>
      var indices = ArrayBuilder.make[Int]
      var values = ArrayBuilder.make[Double]
      var size = 1
      indices += 0
      values += 1.0
      var featureIndex = row.length - 1
      while (featureIndex >= 0) {
        val prevIndices = indices.result()
        val prevValues = values.result()
        val prevSize = size
        val currentEncoder = featureEncoders(featureIndex)
        indices = ArrayBuilder.make[Int]
        values = ArrayBuilder.make[Double]
        size *= currentEncoder.outputSize
        currentEncoder.foreachNonzeroOutput(row(featureIndex), (i, a) => {
          var j = 0
          while (j < prevIndices.length) {
            indices += prevIndices(j) + i * prevSize
            values += prevValues(j) * a
            j += 1
          }
        })
        featureIndex -= 1
      }
      Vectors.sparse(size, indices.result(), values.result()).compressed
    }

    val featureCols = inputFeatures.map { f =>
      f.dataType match {
        case DoubleType => dataset(f.name)
        case _: VectorUDT => dataset(f.name)
        case _: NumericType | BooleanType => dataset(f.name).cast(DoubleType)
      }
    }
    dataset.select(
      col("*"),
      interactFunc(struct(featureCols: _*)).as($(outputCol), featureAttrs.toMetadata()))
  }

  /**
   * Creates a feature encoder for each input column, which supports efficient iteration over
   * one-hot encoded feature values. See also the class-level comment of [[FeatureEncoder]].
   *
   * @param features The input feature columns to create encoders for.
   */
  private def getFeatureEncoders(features: Seq[StructField]): Array[FeatureEncoder] = {
    def getNumFeatures(attr: Attribute): Int = {
      attr match {
        case nominal: NominalAttribute =>
          math.max(1, nominal.getNumValues.getOrElse(
            throw new SparkException("Nominal features must have attr numValues defined.")))
        case _ =>
          1  // numeric feature
      }
    }
    features.map { f =>
      val numFeatures = f.dataType match {
        case _: NumericType | BooleanType =>
          Array(getNumFeatures(Attribute.fromStructField(f)))
        case _: VectorUDT =>
          val attrs = AttributeGroup.fromStructField(f).attributes.getOrElse(
            throw new SparkException("Vector attributes must be defined for interaction."))
          attrs.map(getNumFeatures)
      }
      new FeatureEncoder(numFeatures)
    }.toArray
  }

  /**
   * Generates ML attributes for the output vector of all feature interactions. We make a best
   * effort to generate reasonable names for output features, based on the concatenation of the
   * interacting feature names and values delimited with `_`. When no feature name is specified,
   * we fall back to using the feature index (e.g. `foo:bar_2_0` may indicate an interaction
   * between the numeric `foo` feature and a nominal third feature from column `bar`.
   *
   * @param features The input feature columns to the Interaction transformer.
   */
  private def getFeatureAttrs(features: Seq[StructField]): AttributeGroup = {
    var featureAttrs: Seq[Attribute] = Nil
    features.reverse.foreach { f =>
      val encodedAttrs = f.dataType match {
        case _: NumericType | BooleanType =>
          val attr = Attribute.decodeStructField(f, preserveName = true)
          if (attr == UnresolvedAttribute) {
            encodedFeatureAttrs(Seq(NumericAttribute.defaultAttr.withName(f.name)), None)
          } else if (!attr.name.isDefined) {
            encodedFeatureAttrs(Seq(attr.withName(f.name)), None)
          } else {
            encodedFeatureAttrs(Seq(attr), None)
          }
        case _: VectorUDT =>
          val group = AttributeGroup.fromStructField(f)
          encodedFeatureAttrs(group.attributes.get, Some(group.name))
      }
      if (featureAttrs.isEmpty) {
        featureAttrs = encodedAttrs
      } else {
        featureAttrs = encodedAttrs.flatMap { head =>
          featureAttrs.map { tail =>
            NumericAttribute.defaultAttr.withName(head.name.get + ":" + tail.name.get)
          }
        }
      }
    }
    new AttributeGroup($(outputCol), featureAttrs.toArray)
  }

  /**
   * Generates the output ML attributes for a single input feature. Each output feature name has
   * up to three parts: the group name, feature name, and category name (for nominal features),
   * each separated by an underscore.
   *
   * @param inputAttrs The attributes of the input feature.
   * @param groupName Optional name of the input feature group (for Vector type features).
   */
  private def encodedFeatureAttrs(
      inputAttrs: Seq[Attribute],
      groupName: Option[String]): Seq[Attribute] = {

    def format(
        index: Int,
        attrName: Option[String],
        categoryName: Option[String]): String = {
      val parts = Seq(groupName, Some(attrName.getOrElse(index.toString)), categoryName)
      parts.flatten.mkString("_")
    }

    inputAttrs.zipWithIndex.flatMap {
      case (nominal: NominalAttribute, i) =>
        if (nominal.values.isDefined) {
          nominal.values.get.map(
            v => BinaryAttribute.defaultAttr.withName(format(i, nominal.name, Some(v))))
        } else {
          Array.tabulate(nominal.getNumValues.get)(
            j => BinaryAttribute.defaultAttr.withName(format(i, nominal.name, Some(j.toString))))
        }
      case (a: Attribute, i) =>
        Seq(NumericAttribute.defaultAttr.withName(format(i, a.name, None)))
    }
  }

  @Since("1.6.0")
  override def copy(extra: ParamMap): Interaction = defaultCopy(extra)

}

@Since("1.6.0")
object Interaction extends DefaultParamsReadable[Interaction] {

  @Since("1.6.0")
  override def load(path: String): Interaction = super.load(path)
}

/**
 * This class performs on-the-fly one-hot encoding of features as you iterate over them. To
 * indicate which input features should be one-hot encoded, an array of the feature counts
 * must be passed in ahead of time.
 *
 * @param numFeatures Array of feature counts for each input feature. For nominal features this
 *                    count is equal to the number of categories. For numeric features the count
 *                    should be set to 1.
 */
private[ml] class FeatureEncoder(numFeatures: Array[Int]) extends Serializable {
  assert(numFeatures.forall(_ > 0), "Features counts must all be positive.")

  /** The size of the output vector. */
  val outputSize = numFeatures.sum

  /** Precomputed offsets for the location of each output feature. */
  private val outputOffsets = {
    val arr = new Array[Int](numFeatures.length)
    var i = 1
    while (i < arr.length) {
      arr(i) = arr(i - 1) + numFeatures(i - 1)
      i += 1
    }
    arr
  }

  /**
   * Given an input row of features, invokes the specific function for every non-zero output.
   *
   * @param value The row value to encode, either a Double or Vector.
   * @param f The callback to invoke on each non-zero (index, value) output pair.
   */
  def foreachNonzeroOutput(value: Any, f: (Int, Double) => Unit): Unit = value match {
    case d: Double =>
      assert(numFeatures.length == 1,
        s"${DoubleType.catalogString} columns should only contain one feature.")
      val numOutputCols = numFeatures.head
      if (numOutputCols > 1) {
        assert(
          d >= 0.0 && d == d.toInt && d < numOutputCols,
          s"Values from column must be indices, but got $d.")
        f(d.toInt, 1.0)
      } else {
        f(0, d)
      }
    case vec: Vector =>
      assert(numFeatures.length == vec.size,
        s"Vector column size was ${vec.size}, expected ${numFeatures.length}")
      vec.foreachActive { (i, v) =>
        val numOutputCols = numFeatures(i)
        if (numOutputCols > 1) {
          assert(
            v >= 0.0 && v == v.toInt && v < numOutputCols,
            s"Values from column must be indices, but got $v.")
          f(outputOffsets(i) + v.toInt, 1.0)
        } else {
          f(outputOffsets(i), v)
        }
      }
    case null =>
      throw new SparkException("Values to interact cannot be null.")
    case o =>
      throw new SparkException(s"$o of type ${o.getClass.getName} is not supported.")
  }
}
