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

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup}
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.{IntArrayParam, ParamMap, StringArrayParam}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

/**
 * :: Experimental ::
 * This class takes a feature vector and outputs a new feature vector with a subarray of the
 * original features.
 *
 * The subset of features can be specified with either indices ([[setIndices()]])
 * or names ([[setNames()]]).  At least one feature must be selected. Duplicate features
 * are not allowed, so there can be no overlap between selected indices and names.
 *
 * The output vector will order features with the selected indices first (in the order given),
 * followed by the selected names (in the order given).
 */
@Experimental
final class VectorSlicer(override val uid: String)
  extends Transformer with HasInputCol with HasOutputCol with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("vectorSlicer"))

  /**
   * An array of indices to select features from a vector column.
   * There can be no overlap with [[names]].
   * Default: Empty array
   * @group param
   */
  val indices = new IntArrayParam(this, "indices",
    "An array of indices to select features from a vector column." +
      " There can be no overlap with names.", VectorSlicer.validIndices)

  setDefault(indices -> Array.empty[Int])

  /** @group getParam */
  def getIndices: Array[Int] = $(indices)

  /** @group setParam */
  def setIndices(value: Array[Int]): this.type = set(indices, value)

  /**
   * An array of feature names to select features from a vector column.
   * These names must be specified by ML [[org.apache.spark.ml.attribute.Attribute]]s.
   * There can be no overlap with [[indices]].
   * Default: Empty Array
   * @group param
   */
  val names = new StringArrayParam(this, "names",
    "An array of feature names to select features from a vector column." +
      " There can be no overlap with indices.", VectorSlicer.validNames)

  setDefault(names -> Array.empty[String])

  /** @group getParam */
  def getNames: Array[String] = $(names)

  /** @group setParam */
  def setNames(value: Array[String]): this.type = set(names, value)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    // Validity checks
    transformSchema(dataset.schema)
    val inputAttr = AttributeGroup.fromStructField(dataset.schema($(inputCol)))
    inputAttr.numAttributes.foreach { numFeatures =>
      val maxIndex = $(indices).max
      require(maxIndex < numFeatures,
        s"Selected feature index $maxIndex invalid for only $numFeatures input features.")
    }

    // Prepare output attributes
    val inds = getSelectedFeatureIndices(dataset.schema)
    val selectedAttrs: Option[Array[Attribute]] = inputAttr.attributes.map { attrs =>
      inds.map(index => attrs(index))
    }
    val outputAttr = selectedAttrs match {
      case Some(attrs) => new AttributeGroup($(outputCol), attrs)
      case None => new AttributeGroup($(outputCol), inds.length)
    }

    // Select features
    val slicer = udf { vec: Vector =>
      vec match {
        case features: DenseVector => Vectors.dense(inds.map(features.apply))
        case features: SparseVector => features.slice(inds)
      }
    }
    dataset.withColumn($(outputCol), slicer(dataset($(inputCol))), outputAttr.toMetadata())
  }

  /** Get the feature indices in order: indices, names */
  private def getSelectedFeatureIndices(schema: StructType): Array[Int] = {
    val nameFeatures = MetadataUtils.getFeatureIndicesFromNames(schema($(inputCol)), $(names))
    val indFeatures = $(indices)
    val numDistinctFeatures = (nameFeatures ++ indFeatures).distinct.length
    lazy val errMsg = "VectorSlicer requires indices and names to be disjoint" +
      s" sets of features, but they overlap." +
      s" indices: ${indFeatures.mkString("[", ",", "]")}." +
      s" names: " +
      nameFeatures.zip($(names)).map { case (i, n) => s"$i:$n" }.mkString("[", ",", "]")
    require(nameFeatures.length + indFeatures.length == numDistinctFeatures, errMsg)
    indFeatures ++ nameFeatures
  }

  override def transformSchema(schema: StructType): StructType = {
    require($(indices).length > 0 || $(names).length > 0,
      s"VectorSlicer requires that at least one feature be selected.")
    SchemaUtils.checkColumnType(schema, $(inputCol), new VectorUDT)

    if (schema.fieldNames.contains($(outputCol))) {
      throw new IllegalArgumentException(s"Output column ${$(outputCol)} already exists.")
    }
    val numFeaturesSelected = $(indices).length + $(names).length
    val outputAttr = new AttributeGroup($(outputCol), numFeaturesSelected)
    val outputFields = schema.fields :+ outputAttr.toStructField()
    StructType(outputFields)
  }

  override def copy(extra: ParamMap): VectorSlicer = defaultCopy(extra)
}

@Since("1.6.0")
object VectorSlicer extends DefaultParamsReadable[VectorSlicer] {

  /** Return true if given feature indices are valid */
  private[feature] def validIndices(indices: Array[Int]): Boolean = {
    if (indices.isEmpty) {
      true
    } else {
      indices.length == indices.distinct.length && indices.forall(_ >= 0)
    }
  }

  /** Return true if given feature names are valid */
  private[feature] def validNames(names: Array[String]): Boolean = {
    names.forall(_.nonEmpty) && names.length == names.distinct.length
  }

  @Since("1.6.0")
  override def load(path: String): VectorSlicer = super.load(path)
}
