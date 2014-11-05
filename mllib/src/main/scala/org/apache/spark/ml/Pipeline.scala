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

import org.apache.spark.sql.SchemaRDD

import scala.collection.mutable.ListBuffer

trait PipelineStage extends Identifiable

/**
 * A simple pipeline, which acts as an estimator.
 */
class Pipeline extends Estimator[PipelineModel] {

  val stages: Param[Array[PipelineStage]] =
    new Param[Array[PipelineStage]](this, "stages", "stages of the pipeline")

  override def fit(dataset: SchemaRDD, paramMap: ParamMap): PipelineModel = {
    val theStages = paramMap.apply(stages)
    // Search for last estimator.
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

    new PipelineModel(transformers.toArray)
  }

  override def params: Array[Param[_]] = Array.empty
}

class PipelineModel(val transformers: Array[Transformer]) extends Model {

  override def transform(dataset: SchemaRDD, paramMap: ParamMap): SchemaRDD = {
    transformers.foldLeft(dataset) { (dataset, transformer) =>
      transformer.transform(dataset, paramMap)
    }
  }
}
