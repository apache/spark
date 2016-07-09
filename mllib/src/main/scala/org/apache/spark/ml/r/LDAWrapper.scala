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

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}

private[r] class LDAWrapper private (val pipeline: PipelineModel) extends MLWritable {

  def transform(dataset: Dataset[_]): DataFrame = {
    pipeline.transform(dataset)
  }

  override def write: MLWriter = new LDAWrapper.LDAWrapperWriter(this)
}

private[r] object LDAWrapper extends MLReadable[LDAWrapper] {

  /*
  def fit(data: DataFrame, features: String, k: Int, maxIter: Int, optimizer: String, seed: Long,
          subsamplingRate: Double, topicConcentration: Double, docConcentration: Double,
          checkpointInterval: Int): LDAWrapper = {

    val lda = new LDA()
      .setCheckpointInterval(checkpointInterval)
      .setDocConcentration(docConcentration)
      .setTopicConcentration(topicConcentration)
      .setFeaturesCol(features)
      .setOptimizer(optimizer)
      .setK(k)
      .setMaxIter(maxIter)
      .setSeed(seed)
      .setSubsamplingRate(subsamplingRate)

    val pipeline = new Pipeline().setStages(Array(lda))

    new LDAWrapper(pipeline.fit(data))
  }
  */

  def fit(data: DataFrame, features: String): LDAWrapper = {
    val lda = new LDA().setFeaturesCol(features)
    val pipeline = new Pipeline().setStages(Array(lda))
    new LDAWrapper(pipeline.fit(data))
  }

  override def read: MLReader[LDAWrapper] = new LDAWrapperReader

  override def load(path: String): LDAWrapper = super.load(path)

  class LDAWrapperWriter(instance: LDAWrapper) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadata = "class" -> instance.getClass.getName
      val rMetadataJson: String = compact(render(rMetadata))
      sc.parallelize(Seq(rMetadataJson), 1).saveAsTextFile(rMetadataPath)

      instance.pipeline.save(pipelinePath)
    }
  }

  class LDAWrapperReader extends MLReader[LDAWrapper] {

    override def load(path: String): LDAWrapper = {
      implicit val format = DefaultFormats
      val pipelinePath = new Path(path, "pipeline").toString

      val pipeline = PipelineModel.load(pipelinePath)
      new LDAWrapper(pipeline)
    }
  }
}
