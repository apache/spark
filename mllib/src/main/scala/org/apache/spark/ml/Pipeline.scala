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
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
 * :: AlphaComponent ::
 * A stage in a pipeline, either an [[Estimator]] or a [[Transformer]].
 */
@AlphaComponent
abstract class PipelineStage extends Params with Logging {

  /**
   * :: DeveloperApi ::
   *
   * Derives the output schema from the input schema.
   */
  @DeveloperApi
  def transformSchema(schema: StructType): StructType

  /**
   * :: DeveloperApi ::
   *
   * Derives the output schema from the input schema and parameters, optionally with logging.
   *
   * This should be optimistic.  If it is unclear whether the schema will be valid, then it should
   * be assumed valid until proven otherwise.
   */
  @DeveloperApi
  protected def transformSchema(
      schema: StructType,
      logging: Boolean): StructType = {
    if (logging) {
      logDebug(s"Input schema: ${schema.json}")
    }
    val outputSchema = transformSchema(schema)
    if (logging) {
      logDebug(s"Expected output schema: ${outputSchema.json}")
    }
    outputSchema
  }

  override def copy(extra: ParamMap): PipelineStage = {
    super.copy(extra).asInstanceOf[PipelineStage]
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

  /**
   * param for pipeline stages
   * @group param
   */
  val stages: Param[Array[PipelineStage]] = new Param(this, "stages", "stages of the pipeline")

  /** @group setParam */
  def setStages(value: Array[PipelineStage]): this.type = { set(stages, value); this }

  /** @group getParam */
  def getStages: Array[PipelineStage] = $(stages).clone()

  override def validateParams(paramMap: ParamMap): Unit = {
    val map = extractParamMap(paramMap)
    getStages.foreach {
      case pStage: Params => pStage.validateParams(map)
      case _ =>
    }
  }

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
   * @return fitted pipeline
   */
  override def fit(dataset: DataFrame): PipelineModel = {
    transformSchema(dataset.schema, logging = true)
    val theStages = $(stages)
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
            estimator.fit(curDataset)
          case t: Transformer =>
            t
          case _ =>
            throw new IllegalArgumentException(
              s"Do not support stage $stage of type ${stage.getClass}")
        }
        if (index < indexOfLastEstimator) {
          curDataset = transformer.transform(curDataset)
        }
        transformers += transformer
      } else {
        transformers += stage.asInstanceOf[Transformer]
      }
    }

    new PipelineModel(this, transformers.toArray)
  }

  override def copy(extra: ParamMap): Pipeline = {
    val map = extractParamMap(extra)
    val newStages = map(stages).map(_.copy(extra))
    new Pipeline().setStages(newStages)
  }

  override def transformSchema(schema: StructType): StructType = {
    val theStages = $(stages)
    require(theStages.toSet.size == theStages.length,
      "Cannot have duplicate components in a pipeline.")
    theStages.foldLeft(schema)((cur, stage) => stage.transformSchema(cur))
  }
}

/**
 * :: AlphaComponent ::
 * Represents a compiled pipeline.
 */
@AlphaComponent
class PipelineModel private[ml] (
    override val parent: Pipeline,
    val stages: Array[Transformer])
  extends Model[PipelineModel] with Logging {

  override def validateParams(): Unit = {
    super.validateParams()
    stages.foreach(_.validateParams())
  }

  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    stages.foldLeft(dataset)((cur, transformer) => transformer.transform(cur))
  }

  override def transformSchema(schema: StructType): StructType = {
    stages.foldLeft(schema)((cur, transformer) => transformer.transformSchema(cur))
  }

  override def copy(extra: ParamMap): PipelineModel = {
    new PipelineModel(parent, stages)
  }
}
