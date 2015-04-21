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

package org.apache.spark.ml

import scala.collection.mutable.ListBuffer

import org.apache.spark.Logging
import org.apache.spark.annotation.{AlphaComponent, DeveloperApi}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
 * :: AlphaComponent ::
 * A stage in a pipeline, either an [[Estimator]] or a [[Transformer]].
 */
@AlphaComponent
abstract class PipelineStage extends Serializable with Logging {

  /**
   * :: DeveloperApi ::
   *
   * Derives the output schema from the input schema and parameters.
   * The schema describes the columns and types of the data.
   *
   * @param schema  Input schema to this stage
   * @param paramMap  Parameters passed to this stage
   * @return  Output schema from this stage
   */
  @DeveloperApi
  def transformSchema(schema: StructType, paramMap: ParamMap): StructType

  /**
   * Derives the output schema from the input schema and parameters, optionally with logging.
   *
   * This should be optimistic.  If it is unclear whether the schema will be valid, then it should
   * be assumed valid until proven otherwise.
   */
  protected def transformSchema(
      schema: StructType,
      paramMap: ParamMap,
      logging: Boolean): StructType = {
    if (logging) {
      logDebug(s"Input schema: ${schema.json}")
    }
    val outputSchema = transformSchema(schema, paramMap)
    if (logging) {
      logDebug(s"Expected output schema: ${outputSchema.json}")
    }
    outputSchema
  }
}

/**
 * :: AlphaComponent ::
 * A simple pipeline, which acts as an estimator. A Pipeline consists of a sequence of stages, each
 * of which is either an [[Estimator]] or a [[Transformer]]. When [[Pipeline#fit]] is called, the
 * stages are executed in order. If a stage is an [[Estimator]], its [[Estimator#fit]] method will
 * be called on the input dataset to fit a model. Then the model, which is a transformer, will be
 * used to transform the dataset as the input to the next stage. If a stage is a [[Transformer]],
 * its [[Transformer#transform]] method will be called to produce the dataset for the next stage.
 * The fitted model from a [[Pipeline]] is an [[PipelineModel]], which consists of fitted models and
 * transformers, corresponding to the pipeline stages. If there are no stages, the pipeline acts as
 * an identity transformer.
 */
@AlphaComponent
class Pipeline extends Estimator[PipelineModel] {

  /** param for pipeline stages */
  val stages: Param[Array[PipelineStage]] = new Param(this, "stages", "stages of the pipeline")
  def setStages(value: Array[PipelineStage]): this.type = { set(stages, value); this }
  def getStages: Array[PipelineStage] = getOrDefault(stages)

  /**
   * Fits the pipeline to the input dataset with additional parameters. If a stage is an
   * [[Estimator]], its [[Estimator#fit]] method will be called on the input dataset to fit a model.
   * Then the model, which is a transformer, will be used to transform the dataset as the input to
   * the next stage. If a stage is a [[Transformer]], its [[Transformer#transform]] method will be
   * called to produce the dataset for the next stage. The fitted model from a [[Pipeline]] is an
   * [[PipelineModel]], which consists of fitted models and transformers, corresponding to the
   * pipeline stages. If there are no stages, the output model acts as an identity transformer.
   *
   * @param dataset input dataset
   * @param paramMap parameter map
   * @return fitted pipeline
   */
  override def fit(dataset: DataFrame, paramMap: ParamMap): PipelineModel = {
    transformSchema(dataset.schema, paramMap, logging = true)
    val map = extractParamMap(paramMap)
    val theStages = map(stages)
    // Search for the last estimator.
    var indexOfLastEstimator = -1
    theStages.view.zipWithIndex.foreach { case (stage, index) =>
      stage match {
        case _: Estimator[_] =>
          indexOfLastEstimator = index
        case _ =>
      }
    }
    var curDataset = dataset
    val transformers = ListBuffer.empty[Transformer]
    theStages.view.zipWithIndex.foreach { case (stage, index) =>
      if (index <= indexOfLastEstimator) {
        val transformer = stage match {
          case estimator: Estimator[_] =>
            estimator.fit(curDataset, paramMap)
          case t: Transformer =>
            t
          case _ =>
            throw new IllegalArgumentException(
              s"Do not support stage $stage of type ${stage.getClass}")
        }
        if (index < indexOfLastEstimator) {
          curDataset = transformer.transform(curDataset, paramMap)
        }
        transformers += transformer
      } else {
        transformers += stage.asInstanceOf[Transformer]
      }
    }

    new PipelineModel(this, map, transformers.toArray)
  }

  override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    val map = extractParamMap(paramMap)
    val theStages = map(stages)
    require(theStages.toSet.size == theStages.size,
      "Cannot have duplicate components in a pipeline.")
    theStages.foldLeft(schema)((cur, stage) => stage.transformSchema(cur, paramMap))
  }
}

/**
 * :: AlphaComponent ::
 * Represents a compiled pipeline.
 */
@AlphaComponent
class PipelineModel private[ml] (
    override val parent: Pipeline,
    override val fittingParamMap: ParamMap,
    private[ml] val stages: Array[Transformer])
  extends Model[PipelineModel] with Logging {

  /**
   * Gets the model produced by the input estimator. Throws an NoSuchElementException is the input
   * estimator does not exist in the pipeline.
   */
  def getModel[M <: Model[M]](stage: Estimator[M]): M = {
    val matched = stages.filter {
      case m: Model[_] => m.parent.eq(stage)
      case _ => false
    }
    if (matched.isEmpty) {
      throw new NoSuchElementException(s"Cannot find stage $stage from the pipeline.")
    } else if (matched.size > 1) {
      throw new IllegalStateException(s"Cannot have duplicate estimators in the sample pipeline.")
    } else {
      matched.head.asInstanceOf[M]
    }
  }

  override def transform(dataset: DataFrame, paramMap: ParamMap): DataFrame = {
    // Precedence of ParamMaps: paramMap > this.paramMap > fittingParamMap
    val map = fittingParamMap ++ extractParamMap(paramMap)
    transformSchema(dataset.schema, map, logging = true)
    stages.foldLeft(dataset)((cur, transformer) => transformer.transform(cur, map))
  }

  override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    // Precedence of ParamMaps: paramMap > this.paramMap > fittingParamMap
    val map = fittingParamMap ++ extractParamMap(paramMap)
    stages.foldLeft(schema)((cur, transformer) => transformer.transformSchema(cur, map))
  }
}
