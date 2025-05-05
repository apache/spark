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

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Since
import org.apache.spark.ml.param._
import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ArrayImplicits._


/**
 * Chi-Squared feature selection, which selects categorical features to use for predicting a
 * categorical label.
 * The selector supports different selection methods: `numTopFeatures`, `percentile`, `fpr`,
 * `fdr`, `fwe`.
 *  - `numTopFeatures` chooses a fixed number of top features according to a chi-squared test.
 *  - `percentile` is similar but chooses a fraction of all features instead of a fixed number.
 *  - `fpr` chooses all features whose p-value are below a threshold, thus controlling the false
 *    positive rate of selection.
 *  - `fdr` uses the [Benjamini-Hochberg procedure]
 *    (https://en.wikipedia.org/wiki/False_discovery_rate#Benjamini.E2.80.93Hochberg_procedure)
 *    to choose all features whose false discovery rate is below a threshold.
 *  - `fwe` chooses all features whose p-values are below a threshold. The threshold is scaled by
 *    1/numFeatures, thus controlling the family-wise error rate of selection.
 * By default, the selection method is `numTopFeatures`, with the default number of top features
 * set to 50.
 */
@deprecated("use UnivariateFeatureSelector instead", "3.1.1")
@Since("1.6.0")
final class ChiSqSelector @Since("1.6.0") (@Since("1.6.0") override val uid: String)
  extends Selector[ChiSqSelectorModel] {

  @Since("1.6.0")
  def this() = this(Identifiable.randomUID("chiSqSelector"))

  /** @group setParam */
  @Since("1.6.0")
  override def setNumTopFeatures(value: Int): this.type = super.setNumTopFeatures(value)

  /** @group setParam */
  @Since("2.1.0")
  override def setPercentile(value: Double): this.type = super.setPercentile(value)

  /** @group setParam */
  @Since("2.1.0")
  override def setFpr(value: Double): this.type = super.setFpr(value)

  /** @group setParam */
  @Since("2.2.0")
  override def setFdr(value: Double): this.type = super.setFdr(value)

  /** @group setParam */
  @Since("2.2.0")
  override def setFwe(value: Double): this.type = super.setFwe(value)

  /** @group setParam */
  @Since("2.1.0")
  override def setSelectorType(value: String): this.type = super.setSelectorType(value)

  /** @group setParam */
  @Since("1.6.0")
  override def setFeaturesCol(value: String): this.type = super.setFeaturesCol(value)

  /** @group setParam */
  @Since("1.6.0")
  override def setOutputCol(value: String): this.type = super.setOutputCol(value)

  /** @group setParam */
  @Since("1.6.0")
  override def setLabelCol(value: String): this.type = super.setLabelCol(value)

  /**
   * get the SelectionTestResult for every feature against the label
   */
  protected[this] override def getSelectionTestResult(df: DataFrame): DataFrame = {
    ChiSquareTest.test(df, getFeaturesCol, getLabelCol, true)
  }

  /**
   * Create a new instance of concrete SelectorModel.
   * @param indices The indices of the selected features
   * @return A new SelectorModel instance
   */
  protected[this] def createSelectorModel(
      uid: String,
      indices: Array[Int]): ChiSqSelectorModel = {
    new ChiSqSelectorModel(uid, indices)
  }

  @Since("1.6.0")
  override def fit(dataset: Dataset[_]): ChiSqSelectorModel = {
     super.fit(dataset)
  }

  @Since("1.6.0")
  override def transformSchema(schema: StructType): StructType = super.transformSchema(schema)

  @Since("1.6.0")
  override def copy(extra: ParamMap): ChiSqSelector = defaultCopy(extra)
}

@Since("1.6.0")
object ChiSqSelector extends DefaultParamsReadable[ChiSqSelector] {

  @Since("1.6.0")
  override def load(path: String): ChiSqSelector = super.load(path)
}

/**
 * Model fitted by [[ChiSqSelector]].
 */
@Since("1.6.0")
final class ChiSqSelectorModel private[ml] (
    @Since("1.6.0") override val uid: String,
    @Since("3.1.0") override val selectedFeatures: Array[Int])
  extends SelectorModel[ChiSqSelectorModel](uid, selectedFeatures)  {

  import ChiSqSelectorModel._

  // For ml connect only
  private[ml] def this() = this("", Array.emptyIntArray)

  override protected def isNumericAttribute = false

  /** @group setParam */
  @Since("1.6.0")
  override def setFeaturesCol(value: String): this.type = super.setFeaturesCol(value)

  /** @group setParam */
  @Since("1.6.0")
  override def setOutputCol(value: String): this.type = super.setOutputCol(value)

  @Since("1.6.0")
  override def transformSchema(schema: StructType): StructType = super.transformSchema(schema)

  @Since("1.6.0")
  override def copy(extra: ParamMap): ChiSqSelectorModel = {
    val copied = new ChiSqSelectorModel(uid, selectedFeatures)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("1.6.0")
  override def write: MLWriter = new ChiSqSelectorModelWriter(this)

  @Since("3.0.0")
  override def toString: String = {
    s"ChiSqSelectorModel: uid=$uid, numSelectedFeatures=${selectedFeatures.length}"
  }
}

@Since("1.6.0")
object ChiSqSelectorModel extends MLReadable[ChiSqSelectorModel] {
  private[ml] case class Data(selectedFeatures: Seq[Int])

  class ChiSqSelectorModelWriter(instance: ChiSqSelectorModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sparkSession)
      val data = Data(instance.selectedFeatures.toImmutableArraySeq)
      val dataPath = new Path(path, "data").toString
      ReadWriteUtils.saveObject[Data](dataPath, data, sparkSession)
    }
  }

  private class ChiSqSelectorModelReader extends MLReader[ChiSqSelectorModel] {

    private val className = classOf[ChiSqSelectorModel].getName

    override def load(path: String): ChiSqSelectorModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sparkSession, className)
      val dataPath = new Path(path, "data").toString
      val data = ReadWriteUtils.loadObject[Data](dataPath, sparkSession)
      val model = new ChiSqSelectorModel(metadata.uid, data.selectedFeatures.toArray)
      metadata.getAndSetParams(model)
      model
    }
  }

  @Since("1.6.0")
  override def read: MLReader[ChiSqSelectorModel] = new ChiSqSelectorModelReader

  @Since("1.6.0")
  override def load(path: String): ChiSqSelectorModel = super.load(path)
}
