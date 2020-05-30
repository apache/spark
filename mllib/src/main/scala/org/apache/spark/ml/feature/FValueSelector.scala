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
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.stat.FValueTest
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}


/**
 * F Value Regression feature selector, which selects continuous features to use for predicting a
 * continuous label.
 * The selector supports different selection methods: `numTopFeatures`, `percentile`, `fpr`,
 * `fdr`, `fwe`.
 *  - `numTopFeatures` chooses a fixed number of top features according to a F value regression
 *  test.
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
@Since("3.1.0")
final class FValueSelector @Since("3.1.0") (@Since("3.1.0") override val uid: String) extends
  Selector[FValueSelectorModel] {

  @Since("3.1.0")
  def this() = this(Identifiable.randomUID("FValueSelector"))

  /** @group setParam */
  @Since("3.1.0")
  override def setNumTopFeatures(value: Int): this.type = super.setNumTopFeatures(value)

  /** @group setParam */
  @Since("3.1.0")
  override def setPercentile(value: Double): this.type = super.setPercentile(value)

  /** @group setParam */
  @Since("3.1.0")
  override def setFpr(value: Double): this.type = super.setFpr(value)

  /** @group setParam */
  @Since("3.1.0")
  override def setFdr(value: Double): this.type = super.setFdr(value)

  /** @group setParam */
  @Since("3.1.0")
  override def setFwe(value: Double): this.type = super.setFwe(value)

  /** @group setParam */
  @Since("3.1.0")
  override def setSelectorType(value: String): this.type = super.setSelectorType(value)

  /** @group setParam */
  @Since("3.1.0")
  override def setFeaturesCol(value: String): this.type = super.setFeaturesCol(value)

  /** @group setParam */
  @Since("3.1.0")
  override def setOutputCol(value: String): this.type = super.setOutputCol(value)

  /** @group setParam */
  @Since("3.1.0")
  override def setLabelCol(value: String): this.type = super.setLabelCol(value)

  /**
   * get the SelectionTestResult for every feature against the label
   */
  protected[this] override def getSelectionTestResult(df: DataFrame): DataFrame = {
    FValueTest.test(df, getFeaturesCol, getLabelCol, true)
  }

  /**
   * Create a new instance of concrete SelectorModel.
   * @param indices The indices of the selected features
   * @return A new SelectorModel instance
   */
  protected[this] def createSelectorModel(
      uid: String,
      indices: Array[Int]): FValueSelectorModel = {
    new FValueSelectorModel(uid, indices)
  }

  @Since("3.1.0")
  override def fit(dataset: Dataset[_]): FValueSelectorModel = {
    super.fit(dataset)
  }

  @Since("3.1.0")
  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
}

@Since("3.1.0")
object FValueSelector extends DefaultParamsReadable[FValueSelector] {

  @Since("3.1.0")
  override def load(path: String): FValueSelector = super.load(path)
}

/**
 * Model fitted by [[FValueSelector]]
 */
@Since("3.1.0")
class FValueSelectorModel private[ml](
    @Since("3.1.0") override val uid: String,
    @Since("3.1.0") override val selectedFeatures: Array[Int])
  extends SelectorModel[FValueSelectorModel] (uid, selectedFeatures) {

  /** @group setParam */
  @Since("3.1.0")
  override def setFeaturesCol(value: String): this.type = super.setFeaturesCol(value)

  /** @group setParam */
  @Since("3.1.0")
  override def setOutputCol(value: String): this.type = super.setOutputCol(value)

  @Since("3.1.0")
  override def copy(extra: ParamMap): FValueSelectorModel = {
    val copied = new FValueSelectorModel(uid, selectedFeatures)
      .setParent(parent)
    copyValues(copied, extra)
  }

  @Since("3.1.0")
  override def write: MLWriter = new FValueSelectorModel.FValueSelectorModelWriter(this)

  @Since("3.1.0")
  override def toString: String = {
    s"FValueSelectorModel: uid=$uid, numSelectedFeatures=${selectedFeatures.length}"
  }
}

@Since("3.1.0")
object FValueSelectorModel extends MLReadable[FValueSelectorModel] {

  @Since("3.1.0")
  override def read: MLReader[FValueSelectorModel] = new FValueSelectorModelReader

  @Since("3.1.0")
  override def load(path: String): FValueSelectorModel = super.load(path)

  private[FValueSelectorModel] class FValueSelectorModelWriter(
      instance: FValueSelectorModel) extends MLWriter {

    private case class Data(selectedFeatures: Seq[Int])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.selectedFeatures.toSeq)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class FValueSelectorModelReader extends MLReader[FValueSelectorModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[FValueSelectorModel].getName

    override def load(path: String): FValueSelectorModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
        .select("selectedFeatures").head()
      val selectedFeatures = data.getAs[Seq[Int]](0).toArray
      val model = new FValueSelectorModel(metadata.uid, selectedFeatures)
      metadata.getAndSetParams(model)
      model
    }
  }
}
