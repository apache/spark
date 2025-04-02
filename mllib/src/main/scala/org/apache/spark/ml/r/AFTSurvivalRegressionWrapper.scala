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

package org.apache.spark.ml.r

import org.apache.hadoop.fs.Path
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkException
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.regression.{AFTSurvivalRegression, AFTSurvivalRegressionModel}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.util.ArrayImplicits._

private[r] class AFTSurvivalRegressionWrapper private (
    val pipeline: PipelineModel,
    val features: Array[String]) extends MLWritable {

  private val aftModel: AFTSurvivalRegressionModel =
    pipeline.stages(1).asInstanceOf[AFTSurvivalRegressionModel]

  lazy val rCoefficients: Array[Double] = if (aftModel.getFitIntercept) {
    Array(aftModel.intercept) ++ aftModel.coefficients.toArray ++ Array(math.log(aftModel.scale))
  } else {
    aftModel.coefficients.toArray ++ Array(math.log(aftModel.scale))
  }

  lazy val rFeatures: Array[String] = if (aftModel.getFitIntercept) {
    Array("(Intercept)") ++ features ++ Array("Log(scale)")
  } else {
    features ++ Array("Log(scale)")
  }

  def transform(dataset: Dataset[_]): DataFrame = {
    pipeline.transform(dataset).drop(aftModel.getFeaturesCol)
  }

  override def write: MLWriter =
    new AFTSurvivalRegressionWrapper.AFTSurvivalRegressionWrapperWriter(this)
}

private[r] object AFTSurvivalRegressionWrapper extends MLReadable[AFTSurvivalRegressionWrapper] {

  private val FORMULA_REGEXP = """Surv\(([^,]+), ([^,]+)\) ~ (.+)""".r

  private def formulaRewrite(formula: String): (String, String) = {
    var rewrittenFormula: String = null
    var censorCol: String = null
    try {
      val FORMULA_REGEXP(label, censor, features) = formula
      // TODO: Support dot operator.
      if (features.contains(".")) {
        throw new UnsupportedOperationException(
          "Terms of survreg formula can not support dot operator.")
      }
      rewrittenFormula = label.trim + "~" + features.trim
      censorCol = censor.trim
    } catch {
      case e: MatchError =>
        throw new SparkException(s"Could not parse formula: $formula")
    }

    (rewrittenFormula, censorCol)
  }


  def fit(
      formula: String,
      data: DataFrame,
      aggregationDepth: Int,
      stringIndexerOrderType: String): AFTSurvivalRegressionWrapper = {

    val (rewrittenFormula, censorCol) = formulaRewrite(formula)

    val rFormula = new RFormula().setFormula(rewrittenFormula)
      .setStringIndexerOrderType(stringIndexerOrderType)
    RWrapperUtils.checkDataColumns(rFormula, data)
    val rFormulaModel = rFormula.fit(data)

    // get feature names from output schema
    val schema = rFormulaModel.transform(data).schema
    val featureAttrs = AttributeGroup.fromStructField(schema(rFormula.getFeaturesCol))
      .attributes.get
    val features = featureAttrs.map(_.name.get)

    val aft = new AFTSurvivalRegression()
      .setCensorCol(censorCol)
      .setFitIntercept(rFormula.hasIntercept)
      .setFeaturesCol(rFormula.getFeaturesCol)
      .setAggregationDepth(aggregationDepth)

    val pipeline = new Pipeline()
      .setStages(Array(rFormulaModel, aft))
      .fit(data)

    new AFTSurvivalRegressionWrapper(pipeline, features)
  }

  override def read: MLReader[AFTSurvivalRegressionWrapper] = new AFTSurvivalRegressionWrapperReader

  override def load(path: String): AFTSurvivalRegressionWrapper = super.load(path)

  class AFTSurvivalRegressionWrapperWriter(instance: AFTSurvivalRegressionWrapper)
    extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadata = ("class" -> instance.getClass.getName) ~
        ("features" -> instance.features.toImmutableArraySeq)
      val rMetadataJson: String = compact(render(rMetadata))
      // Note that we should write single file. If there are more than one row
      // it produces more partitions.
      sparkSession.createDataFrame(Seq(Tuple1(rMetadataJson))).write.text(rMetadataPath)

      instance.pipeline.save(pipelinePath)
    }
  }

  class AFTSurvivalRegressionWrapperReader extends MLReader[AFTSurvivalRegressionWrapper] {

    override def load(path: String): AFTSurvivalRegressionWrapper = {
      implicit val format: Formats = DefaultFormats
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadataStr = sparkSession.read.text(rMetadataPath)
        .first().getString(0)
      val rMetadata = parse(rMetadataStr)
      val features = (rMetadata \ "features").extract[Array[String]]

      val pipeline = PipelineModel.load(pipelinePath)
      new AFTSurvivalRegressionWrapper(pipeline, features)
    }
  }
}
