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

import java.lang.{Double => JDouble, Integer => JInt}
import java.util.{Map => JMap}

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, VectorUDT}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.collection.OpenHashSet

/** Private trait for params for VectorIndexer and VectorIndexerModel */
private[ml] trait VectorIndexerParams extends Params with HasInputCol with HasOutputCol {

  /**
   * Threshold for the number of values a categorical feature can take.
   * If a feature is found to have > maxCategories values, then it is declared continuous.
   * Must be >= 2.
   *
   * (default = 20)
   * @group param
   */
  val maxCategories = new IntParam(this, "maxCategories",
    "Threshold for the number of values a categorical feature can take (>= 2)." +
      " If a feature is found to have > maxCategories values, then it is declared continuous.",
    ParamValidators.gtEq(2))

  setDefault(maxCategories -> 20)

  /** @group getParam */
  def getMaxCategories: Int = $(maxCategories)
}

/**
 * :: Experimental ::
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
@Experimental
class VectorIndexer(override val uid: String) extends Estimator[VectorIndexerModel]
  with VectorIndexerParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("vecIdx"))

  /** @group setParam */
  def setMaxCategories(value: Int): this.type = set(maxCategories, value)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def fit(dataset: DataFrame): VectorIndexerModel = {
    transformSchema(dataset.schema, logging = true)
    val firstRow = dataset.select($(inputCol)).take(1)
    require(firstRow.length == 1, s"VectorIndexer cannot be fit on an empty dataset.")
    val numFeatures = firstRow(0).getAs[Vector](0).size
    val vectorDataset = dataset.select($(inputCol)).map { case Row(v: Vector) => v }
    val maxCats = $(maxCategories)
    val categoryStats: VectorIndexer.CategoryStats = vectorDataset.mapPartitions { iter =>
      val localCatStats = new VectorIndexer.CategoryStats(numFeatures, maxCats)
      iter.foreach(localCatStats.addVector)
      Iterator(localCatStats)
    }.reduce((stats1, stats2) => stats1.merge(stats2))
    val model = new VectorIndexerModel(uid, numFeatures, categoryStats.getCategoryMaps)
      .setParent(this)
    copyValues(model)
  }

  override def transformSchema(schema: StructType): StructType = {
    // We do not transfer feature metadata since we do not know what types of features we will
    // produce in transform().
    val dataType = new VectorUDT
    require(isDefined(inputCol), s"VectorIndexer requires input column parameter: $inputCol")
    require(isDefined(outputCol), s"VectorIndexer requires output column parameter: $outputCol")
    SchemaUtils.checkColumnType(schema, $(inputCol), dataType)
    SchemaUtils.appendColumn(schema, $(outputCol), dataType)
  }

  override def copy(extra: ParamMap): VectorIndexer = defaultCopy(extra)
}

@Since("1.6.0")
object VectorIndexer extends DefaultParamsReadable[VectorIndexer] {

  @Since("1.6.0")
  override def load(path: String): VectorIndexer = super.load(path)

  /**
   * Helper class for tracking unique values for each feature.
   *
   * TODO: Track which features are known to be continuous already; do not update counts for them.
   *
   * @param numFeatures  This class fails if it encounters a Vector whose length is not numFeatures.
   * @param maxCategories  This class caps the number of unique values collected at maxCategories.
   */
  private class CategoryStats(private val numFeatures: Int, private val maxCategories: Int)
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
      val size = dv.size
      while (i < size) {
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
      val size = sv.size
      while (vecIndex < size) {
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
 * :: Experimental ::
 * Transform categorical features to use 0-based indices instead of their original values.
 *  - Categorical features are mapped to indices.
 *  - Continuous features (columns) are left unchanged.
 * This also appends metadata to the output column, marking features as Numeric (continuous),
 * Nominal (categorical), or Binary (either continuous or categorical).
 * Non-ML metadata is not carried over from the input to the output column.
 *
 * This maintains vector sparsity.
 *
 * @param numFeatures  Number of features, i.e., length of Vectors which this transforms
 * @param categoryMaps  Feature value index.  Keys are categorical feature indices (column indices).
 *                      Values are maps from original features values to 0-based category indices.
 *                      If a feature is not in this map, it is treated as continuous.
 */
@Experimental
class VectorIndexerModel private[ml] (
    override val uid: String,
    val numFeatures: Int,
    val categoryMaps: Map[Int, Map[Double, Int]])
  extends Model[VectorIndexerModel] with VectorIndexerParams with MLWritable {

  import VectorIndexerModel._

  /** Java-friendly version of [[categoryMaps]] */
  def javaCategoryMaps: JMap[JInt, JMap[JDouble, JInt]] = {
    categoryMaps.mapValues(_.asJava).asJava.asInstanceOf[JMap[JInt, JMap[JDouble, JInt]]]
  }

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

  /** Per-vector transform function */
  private val transformFunc: Vector => Vector = {
    val sortedCatFeatureIndices = categoryMaps.keys.toArray.sorted
    val localVectorMap = categoryMaps
    val localNumFeatures = numFeatures
    val f: Vector => Vector = { (v: Vector) =>
      assert(v.size == localNumFeatures, "VectorIndexerModel expected vector of length" +
        s" $numFeatures but found length ${v.size}")
      v match {
        case dv: DenseVector =>
          val tmpv = dv.copy
          localVectorMap.foreach { case (featureIndex: Int, categoryMap: Map[Double, Int]) =>
            tmpv.values(featureIndex) = categoryMap(tmpv(featureIndex))
          }
          tmpv
        case sv: SparseVector =>
          // We use the fact that categorical value 0 is always mapped to index 0.
          val tmpv = sv.copy
          var catFeatureIdx = 0 // index into sortedCatFeatureIndices
          var k = 0 // index into non-zero elements of sparse vector
          while (catFeatureIdx < sortedCatFeatureIndices.length && k < tmpv.indices.length) {
            val featureIndex = sortedCatFeatureIndices(catFeatureIdx)
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
    }
    f
  }

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val newField = prepOutputField(dataset.schema)
    val transformUDF = udf { (vector: Vector) => transformFunc(vector) }
    val newCol = transformUDF(dataset($(inputCol)))
    dataset.withColumn($(outputCol), newCol, newField.metadata)
  }

  override def transformSchema(schema: StructType): StructType = {
    val dataType = new VectorUDT
    require(isDefined(inputCol),
      s"VectorIndexerModel requires input column parameter: $inputCol")
    require(isDefined(outputCol),
      s"VectorIndexerModel requires output column parameter: $outputCol")
    SchemaUtils.checkColumnType(schema, $(inputCol), dataType)

    // If the input metadata specifies numFeatures, compare with expected numFeatures.
    val origAttrGroup = AttributeGroup.fromStructField(schema($(inputCol)))
    val origNumFeatures: Option[Int] = if (origAttrGroup.attributes.nonEmpty) {
      Some(origAttrGroup.attributes.get.length)
    } else {
      origAttrGroup.numAttributes
    }
    require(origNumFeatures.forall(_ == numFeatures), "VectorIndexerModel expected" +
      s" $numFeatures features, but input column ${$(inputCol)} had metadata specifying" +
      s" ${origAttrGroup.numAttributes.get} features.")

    val newField = prepOutputField(schema)
    val outputFields = schema.fields :+ newField
    StructType(outputFields)
  }

  /**
   * Prepare the output column field, including per-feature metadata.
   * @param schema  Input schema
   * @return  Output column field.  This field does not contain non-ML metadata.
   */
  private def prepOutputField(schema: StructType): StructField = {
    val origAttrGroup = AttributeGroup.fromStructField(schema($(inputCol)))
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
        case (origAttr: Attribute, _) =>
          origAttr
      }
    } else {
      partialFeatureAttributes
    }
    val newAttributeGroup = new AttributeGroup($(outputCol), featureAttributes)
    newAttributeGroup.toStructField()
  }

  override def copy(extra: ParamMap): VectorIndexerModel = {
    val copied = new VectorIndexerModel(uid, numFeatures, categoryMaps)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("1.6.0")
  override def write: MLWriter = new VectorIndexerModelWriter(this)
}

@Since("1.6.0")
object VectorIndexerModel extends MLReadable[VectorIndexerModel] {

  private[VectorIndexerModel]
  class VectorIndexerModelWriter(instance: VectorIndexerModel) extends MLWriter {

    private case class Data(numFeatures: Int, categoryMaps: Map[Int, Map[Double, Int]])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.numFeatures, instance.categoryMaps)
      val dataPath = new Path(path, "data").toString
      sqlContext.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class VectorIndexerModelReader extends MLReader[VectorIndexerModel] {

    private val className = classOf[VectorIndexerModel].getName

    override def load(path: String): VectorIndexerModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sqlContext.read.parquet(dataPath)
        .select("numFeatures", "categoryMaps")
        .head()
      val numFeatures = data.getAs[Int](0)
      val categoryMaps = data.getAs[Map[Int, Map[Double, Int]]](1)
      val model = new VectorIndexerModel(metadata.uid, numFeatures, categoryMaps)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  @Since("1.6.0")
  override def read: MLReader[VectorIndexerModel] = new VectorIndexerModelReader

  @Since("1.6.0")
  override def load(path: String): VectorIndexerModel = super.load(path)
}
