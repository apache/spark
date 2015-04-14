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

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.attribute.{BinaryAttribute, NumericAttribute, NominalAttribute,
  Attribute, AttributeGroup}
import org.apache.spark.ml.param.{IntParam, ParamMap, Params}
import org.apache.spark.ml.param.shared._
import org.apache.spark.mllib.linalg.{SparseVector, DenseVector, Vector, VectorUDT}
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.functions.callUDF
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.collection.OpenHashSet


/** Private trait for params for VectorIndexer and VectorIndexerModel */
private[ml] trait VectorIndexerParams extends Params with HasInputCol with HasOutputCol {

  /**
   * Threshold for the number of values a categorical feature can take.
   * If a feature is found to have > maxCategories values, then it is declared continuous.
   *
   * (default = 20)
   */
  val maxCategories = new IntParam(this, "maxCategories",
    "Threshold for the number of values a categorical feature can take." +
      " If a feature is found to have > maxCategories values, then it is declared continuous.")

  /** @group getParam */
  def getMaxCategories: Int = getOrDefault(maxCategories)

  setDefault(maxCategories -> 20)
}

/**
 * :: AlphaComponent ::
 *
 * Class for indexing categorical feature columns in a dataset of [[Vector]].
 *
 * This has 2 usage modes:
 *  - Automatically identify categorical features (default behavior)
 *     - This helps process a dataset of unknown vectors into a dataset with some continuous
 *       features and some categorical features. The choice between continuous and categorical
 *       is based upon a maxCategories parameter.
 *     - Set maxCategories to the maximum number of categorical any categorical feature should have.
 *     - E.g.: Feature 0 has unique values {-1.0, 0.0}, and feature 1 values {1.0, 3.0, 5.0}.
 *       If maxCategories = 2, then feature 0 will be declared categorical and use indices {0, 1},
 *       and feature 1 will be declared continuous.
 *  - Index all features, if all features are categorical
 *     - If maxCategories is set to be very large, then this will build an index of unique
 *       values for all features.
 *     - Warning: This can cause problems if features are continuous since this will collect ALL
 *       unique values to the driver.
 *     - E.g.: Feature 0 has unique values {-1.0, 0.0}, and feature 1 values {1.0, 3.0, 5.0}.
 *       If maxCategories >= 3, then both features will be declared categorical.
 *
 * This returns a model which can transform categorical features to use 0-based indices.
 *
 * Index stability:
 *  - This is not guaranteed to choose the same category index across multiple runs.
 *  - If a categorical feature includes value 0, then this is guaranteed to map value 0 to index 0.
 *    This maintains vector sparsity.
 *  - More stability may be added in the future.
 *
 * TODO: Future extensions: The following functionality is planned for the future:
 *  - Preserve metadata in transform; if a feature's metadata is already present, do not recompute.
 *  - Specify certain features to not index, either via a parameter or via existing metadata.
 *  - Add warning if a categorical feature has only 1 category.
 *  - Add option for allowing unknown categories.
 */
@AlphaComponent
class VectorIndexer extends Estimator[VectorIndexerModel] with VectorIndexerParams {

  /** @group setParam */
  def setMaxCategories(value: Int): this.type = {
    require(value > 1,
      s"DatasetIndexer given maxCategories = value, but requires maxCategories > 1.")
    set(maxCategories, value)
  }

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def fit(dataset: DataFrame, paramMap: ParamMap): VectorIndexerModel = {
    transformSchema(dataset.schema, paramMap, logging = true)
    val map = extractParamMap(paramMap)
    val firstRow = dataset.select(map(inputCol)).take(1)
    require(firstRow.length == 1, s"VectorIndexer cannot be fit on an empty dataset.")
    val numFeatures = firstRow(0).getAs[Vector](0).size
    val vectorDataset = dataset.select(map(inputCol)).map { case Row(v: Vector) => v }
    val maxCats = map(maxCategories)
    val categoryStats: VectorIndexer.CategoryStats = vectorDataset.mapPartitions { iter =>
      val localCatStats = new VectorIndexer.CategoryStats(numFeatures, maxCats)
      iter.foreach(localCatStats.addVector)
      Iterator(localCatStats)
    }.reduce((stats1, stats2) => stats1.merge(stats2))
    val model = new VectorIndexerModel(this, map, numFeatures, categoryStats.getCategoryMaps)
    Params.inheritValues(map, this, model)
    model
  }

  override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    // We do not transfer feature metadata since we do not know what types of features we will
    // produce in transform().
    val map = extractParamMap(paramMap)
    val dataType = new VectorUDT
    require(map.contains(inputCol), s"VectorIndexer requires input column parameter: $inputCol")
    require(map.contains(outputCol), s"VectorIndexer requires output column parameter: $outputCol")
    SchemaUtils.checkColumnType(schema, map(inputCol), dataType)
    SchemaUtils.appendColumn(schema, map(outputCol), dataType)
  }
}

private object VectorIndexer {

  /**
   * Helper class for tracking unique values for each feature.
   *
   * TODO: Track which features are known to be continuous already; do not update counts for them.
   *
   * @param numFeatures  This class fails if it encounters a Vector whose length is not numFeatures.
   * @param maxCategories  This class caps the number of unique values collected at maxCategories.
   */
  class CategoryStats(private val numFeatures: Int, private val maxCategories: Int)
    extends Serializable {

    /** featureValueSets[feature index] = set of unique values */
    private val featureValueSets =
      Array.fill[OpenHashSet[Double]](numFeatures)(new OpenHashSet[Double]())

    /** Merge with another instance, modifying this instance. */
    def merge(other: CategoryStats): CategoryStats = {
      featureValueSets.zip(other.featureValueSets).foreach { case (thisValSet, otherValSet) =>
        otherValSet.iterator.foreach { x =>
          // Once we have found > maxCategories values, we know the feature is continuous
          // and do not need to collect more values for it.
          if (thisValSet.size <= maxCategories) thisValSet.add(x)
        }
      }
      this
    }

    /** Add a new vector to this index, updating sets of unique feature values */
    def addVector(v: Vector): Unit = {
      require(v.size == numFeatures, s"VectorIndexer expected $numFeatures features but" +
        s" found vector of size ${v.size}.")
      v match {
        case dv: DenseVector => addDenseVector(dv)
        case sv: SparseVector => addSparseVector(sv)
      }
    }

    /**
     * Based on stats collected, decide which features are categorical,
     * and choose indices for categories.
     *
     * Sparsity: This tries to maintain sparsity by treating value 0.0 specially.
     *           If a categorical feature takes value 0.0, then value 0.0 is given index 0.
     *
     * @return  Feature value index.  Keys are categorical feature indices (column indices).
     *          Values are mappings from original features values to 0-based category indices.
     */
    def getCategoryMaps: Map[Int, Map[Double, Int]] = {
      // Filter out features which are declared continuous.
      featureValueSets.zipWithIndex.filter(_._1.size <= maxCategories).map {
        case (featureValues: OpenHashSet[Double], featureIndex: Int) =>
          var sortedFeatureValues = featureValues.iterator.filter(_ != 0.0).toArray.sorted
          val zeroExists = sortedFeatureValues.length + 1 == featureValues.size
          if (zeroExists) {
            sortedFeatureValues = 0.0 +: sortedFeatureValues
          }
          val categoryMap: Map[Double, Int] = sortedFeatureValues.zipWithIndex.toMap
          (featureIndex, categoryMap)
      }.toMap
    }

    private def addDenseVector(dv: DenseVector): Unit = {
      var i = 0
      while (i < dv.size) {
        if (featureValueSets(i).size <= maxCategories) {
          featureValueSets(i).add(dv(i))
        }
        i += 1
      }
    }

    private def addSparseVector(sv: SparseVector): Unit = {
      // TODO: This might be able to handle 0's more efficiently.
      var vecIndex = 0 // index into vector
      var k = 0 // index into non-zero elements
      while (vecIndex < sv.size) {
        val featureValue = if (k < sv.indices.length && vecIndex == sv.indices(k)) {
          k += 1
          sv.values(k - 1)
        } else {
          0.0
        }
        if (featureValueSets(vecIndex).size <= maxCategories) {
          featureValueSets(vecIndex).add(featureValue)
        }
        vecIndex += 1
      }
    }
  }
}

/**
 * :: AlphaComponent ::
 *
 * Transform categorical features to use 0-based indices instead of their original values.
 *  - Categorical features are mapped to indices.
 *  - Continuous features (columns) are left unchanged.
 * This also appends metadata to the output column, marking features as Numeric (continuous),
 * Nominal (categorical), or Binary (either continuous or categorical).
 *
 * This maintains vector sparsity.
 *
 * @param numFeatures  Number of features, i.e., length of Vectors which this transforms
 * @param categoryMaps  Feature value index.  Keys are categorical feature indices (column indices).
 *                      Values are maps from original features values to 0-based category indices.
 *                      If a feature is not in this map, it is treated as continuous.
 */
@AlphaComponent
class VectorIndexerModel private[ml] (
    override val parent: VectorIndexer,
    override val fittingParamMap: ParamMap,
    val numFeatures: Int,
    val categoryMaps: Map[Int, Map[Double, Int]])
  extends Model[VectorIndexerModel] with VectorIndexerParams {

  /**
   * Pre-computed feature attributes, with some missing info.
   * In transform(), set attribute name and other info, if available.
   */
  private val partialFeatureAttributes: Array[Attribute] = {
    val attrs = new Array[Attribute](numFeatures)
    var categoricalFeatureCount = 0 // validity check for numFeatures, categoryMaps
    var featureIndex = 0
    while (featureIndex < numFeatures) {
      if (categoryMaps.contains(featureIndex)) {
        // categorical feature
        val featureValues: Array[String] =
          categoryMaps(featureIndex).toArray.sortBy(_._1).map(_._1).map(_.toString)
        if (featureValues.length == 2) {
          attrs(featureIndex) = new BinaryAttribute(index = Some(featureIndex),
            values = Some(featureValues))
        } else {
          attrs(featureIndex) = new NominalAttribute(index = Some(featureIndex),
            isOrdinal = Some(false), values = Some(featureValues))
        }
        categoricalFeatureCount += 1
      } else {
        // continuous feature
        attrs(featureIndex) = new NumericAttribute(index = Some(featureIndex))
      }
      featureIndex += 1
    }
    require(categoricalFeatureCount == categoryMaps.size, "VectorIndexerModel given categoryMaps" +
      s" with keys outside expected range [0,...,numFeatures), where numFeatures=$numFeatures")
    attrs
  }

  // TODO: Check more carefully about whether this whole class will be included in a closure.

  private val transformFunc: Vector => Vector = {
    val sortedCategoricalFeatureIndices = categoryMaps.keys.toArray.sorted
    val localVectorMap = categoryMaps
    val f: Vector => Vector = {
      case dv: DenseVector =>
        val tmpv = dv.copy
        localVectorMap.foreach { case (featureIndex: Int, categoryMap: Map[Double, Int]) =>
          tmpv.values(featureIndex) = categoryMap(tmpv(featureIndex))
        }
        tmpv
      case sv: SparseVector =>
        // We use the fact that categorical value 0 is always mapped to index 0.
        val tmpv = sv.copy
        var catFeatureIdx = 0 // index into sortedCategoricalFeatureIndices
        var k = 0 // index into non-zero elements of sparse vector
        while (catFeatureIdx < sortedCategoricalFeatureIndices.length && k < tmpv.indices.length) {
          val featureIndex = sortedCategoricalFeatureIndices(catFeatureIdx)
          if (featureIndex < tmpv.indices(k)) {
            catFeatureIdx += 1
          } else if (featureIndex > tmpv.indices(k)) {
            k += 1
          } else {
            tmpv.values(k) = localVectorMap(featureIndex)(tmpv.values(k))
            catFeatureIdx += 1
            k += 1
          }
        }
        tmpv
    }
    f
  }

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: DataFrame, paramMap: ParamMap): DataFrame = {
    transformSchema(dataset.schema, paramMap, logging = true)
    val map = extractParamMap(paramMap)
    val newField = prepOutputField(dataset.schema, map)
    val newCol = callUDF(transformFunc, new VectorUDT, dataset(map(inputCol)))
    // For now, just check the first row of inputCol for vector length.
    val firstRow = dataset.select(map(inputCol)).take(1)
    if (firstRow.length != 0) {
      val actualNumFeatures = firstRow(0).getAs[Vector](0).size
      require(numFeatures == actualNumFeatures, "VectorIndexerModel expected vector of length" +
        s" $numFeatures but found length $actualNumFeatures")
    }
    dataset.withColumn(map(outputCol), newCol.as(map(outputCol), newField.metadata))
  }

  override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    val map = extractParamMap(paramMap)
    val dataType = new VectorUDT
    require(map.contains(inputCol),
      s"VectorIndexerModel requires input column parameter: $inputCol")
    require(map.contains(outputCol),
      s"VectorIndexerModel requires output column parameter: $outputCol")
    SchemaUtils.checkColumnType(schema, map(inputCol), dataType)

    val origAttrGroup = AttributeGroup.fromStructField(schema(map(inputCol)))
    val origNumFeatures: Option[Int] = if (origAttrGroup.attributes.nonEmpty) {
      Some(origAttrGroup.attributes.get.length)
    } else {
      origAttrGroup.numAttributes
    }
    require(origNumFeatures.forall(_ == numFeatures), "VectorIndexerModel expected" +
      s" $numFeatures features, but input column ${map(inputCol)} had metadata specifying" +
      s" ${origAttrGroup.numAttributes.get} features.")

    val newField = prepOutputField(schema, map)
    val outputFields = schema.fields :+ newField
    StructType(outputFields)
  }

  /**
   * Prepare the output column field, including per-feature metadata.
   * @param schema  Input schema
   * @param map  Parameter map (with this class' embedded parameter map folded in)
   * @return  Output column field
   */
  private def prepOutputField(schema: StructType, map: ParamMap): StructField = {
    val origAttrGroup = AttributeGroup.fromStructField(schema(map(inputCol)))
    val featureAttributes: Array[Attribute] = if (origAttrGroup.attributes.nonEmpty) {
      // Convert original attributes to modified attributes
      val origAttrs: Array[Attribute] = origAttrGroup.attributes.get
      origAttrs.zip(partialFeatureAttributes).map {
        case (origAttr: Attribute, featAttr: BinaryAttribute) =>
          if (origAttr.name.nonEmpty) {
            featAttr.withName(origAttr.name.get)
          } else {
            featAttr
          }
        case (origAttr: Attribute, featAttr: NominalAttribute) =>
          if (origAttr.name.nonEmpty) {
            featAttr.withName(origAttr.name.get)
          } else {
            featAttr
          }
        case (origAttr: Attribute, featAttr: NumericAttribute) =>
          origAttr.withIndex(featAttr.index.get)
      }
    } else {
      partialFeatureAttributes
    }
    val newAttributeGroup = new AttributeGroup(map(outputCol), featureAttributes)
    newAttributeGroup.toStructField(schema(map(inputCol)).metadata)
  }
}
