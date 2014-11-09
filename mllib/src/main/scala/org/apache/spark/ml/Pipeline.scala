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

import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.SchemaRDD

/**
 * A stage in a pipeline, either an Estimator or an Transformer.
 */
abstract class PipelineStage

/**
 * A simple pipeline, which acts as an estimator.
 */
class Pipeline extends Estimator[PipelineModel] {

  val stages: Param[Array[PipelineStage]] = new Param(this, "stages", "stages of the pipeline")
  def setStages(value: Array[PipelineStage]): this.type = { set(stages, value); this }
  def getStages: Array[PipelineStage] = get(stages)

  override def fit(dataset: SchemaRDD, paramMap: ParamMap): PipelineModel = {
    val map = this.paramMap ++ paramMap
    val theStages = map(stages)
    // Search for the last estimator.
    var lastIndexOfEstimator = -1
    theStages.view.zipWithIndex.foreach { case (stage, index) =>
      stage match {
        case _: Estimator[_] =>
          lastIndexOfEstimator = index
        case _ =>
      }
    }
    var curDataset = dataset
    val transformers = ListBuffer.empty[Transformer]
    theStages.view.zipWithIndex.foreach { case (stage, index) =>
      stage match {
        case estimator: Estimator[_] =>
          val transformer = estimator.fit(curDataset, paramMap)
          if (index < lastIndexOfEstimator) {
            curDataset = transformer.transform(curDataset, paramMap)
          }
          transformers += transformer
        case transformer: Transformer =>
          if (index < lastIndexOfEstimator) {
            curDataset = transformer.transform(curDataset, paramMap)
          }
          transformers += transformer
        case _ =>
          throw new IllegalArgumentException
      }
    }

    new PipelineModel(this, map, transformers.toArray)
  }
}

/**
 * Represents a compiled pipeline.
 */
class PipelineModel(
    override val parent: Pipeline,
    override val fittingParamMap: ParamMap,
    val transformers: Array[Transformer]) extends Model {

  /**
   * Gets the model produced by the input estimator. Throws an NoSuchElementException is the input
   * estimator does not exist in the pipeline.
   */
  def getModel[M](estimator: Estimator[M]): M = {
    val matched = transformers.filter {
      case m: Model => m.parent.eq(estimator)
      case _ => false
    }
    if (matched.isEmpty) {
      throw new NoSuchElementException(s"Cannot find estimator $estimator from the pipeline.")
    } else if (matched.size > 1) {
      throw new IllegalStateException(s"Cannot have duplicate estimators in the sample pipeline.")
    } else {
      matched.head.asInstanceOf[M]
    }
  }

  override def transform(dataset: SchemaRDD, paramMap: ParamMap): SchemaRDD = {
    transformers.foldLeft(dataset) { (dataset, transformer) =>
      transformer.transform(dataset, paramMap)
    }
  }
}
