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
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.util.{MLReadable, MLReader, MLWritable, MLWriter}

private[r] class MultilayerPerceptronClassifierWrapper private (
    val pipeline: PipelineModel,
    val labels: Array[String],
    val features: Array[String]) extends MLWritable {

  /**
   * Returns an [[MLWriter]] instance for this ML instance.
   */
  override def write: MLWriter =
    new MultilayerPerceptronClassifierWrapper.MultilayerPerceptronClassifierWrapperWriter(this)
}

private[r] object MultilayerPerceptronClassifierWrapper
  extends MLReadable[MultilayerPerceptronClassifierWrapper] {
  /**
    * Returns an [[MLReader]] instance for this class.
    */
  override def read: MLReader[MultilayerPerceptronClassifierWrapper] =
    new MultilayerPerceptronClassifierWrapperReader

  class MultilayerPerceptronClassifierWrapperReader
    extends MLReader[MultilayerPerceptronClassifierWrapper]{
    override def load(path: String): NaiveBayesWrapper = {
      implicit val format = DefaultFormats
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadataStr = sc.textFile(rMetadataPath, 1).first()
      val rMetadata = parse(rMetadataStr)
      val labels = (rMetadata \ "labels").extract[Array[String]]
      val features = (rMetadata \ "features").extract[Array[String]]

      val pipeline = PipelineModel.load(pipelinePath)
      new NaiveBayesWrapper(pipeline, labels, features)
    }
  }

  class MultilayerPerceptronClassifierWrapperWriter(instance: MultilayerPerceptronClassifierWrapper)
    extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadata = ("class" -> instance.getClass.getName) ~
        ("labels" -> instance.labels.toSeq) ~
        ("features" -> instance.features.toSeq)
      val rMetadataJson: String = compact(render(rMetadata))
      sc.parallelize(Seq(rMetadataJson), 1).saveAsTextFile(rMetadataPath)

      instance.pipeline.save(pipelinePath)
    }
  }

}