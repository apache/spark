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
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.ml.fpm.{FPGrowth, FPGrowthModel}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}

private[r] class FPGrowthWrapper private (val fpGrowthModel: FPGrowthModel) extends MLWritable {
  def freqItemsets: DataFrame = fpGrowthModel.freqItemsets
  def associationRules: DataFrame = fpGrowthModel.associationRules

  def transform(dataset: Dataset[_]): DataFrame = {
    fpGrowthModel.transform(dataset)
  }

  override def write: MLWriter = new FPGrowthWrapper.FPGrowthWrapperWriter(this)
}

private[r] object FPGrowthWrapper extends MLReadable[FPGrowthWrapper] {

  def fit(
           data: DataFrame,
           minSupport: Double,
           minConfidence: Double,
           itemsCol: String,
           numPartitions: Integer): FPGrowthWrapper = {
    val fpGrowth = new FPGrowth()
      .setMinSupport(minSupport)
      .setMinConfidence(minConfidence)
      .setItemsCol(itemsCol)

    if (numPartitions != null && numPartitions > 0) {
      fpGrowth.setNumPartitions(numPartitions)
    }

    val fpGrowthModel = fpGrowth.fit(data)

    new FPGrowthWrapper(fpGrowthModel)
  }

  override def read: MLReader[FPGrowthWrapper] = new FPGrowthWrapperReader

  class FPGrowthWrapperReader extends MLReader[FPGrowthWrapper] {
    override def load(path: String): FPGrowthWrapper = {
      val modelPath = new Path(path, "model").toString
      val fPGrowthModel = FPGrowthModel.load(modelPath)

      new FPGrowthWrapper(fPGrowthModel)
    }
  }

  class FPGrowthWrapperWriter(instance: FPGrowthWrapper) extends MLWriter {
    override protected def saveImpl(path: String): Unit = {
      val modelPath = new Path(path, "model").toString
      val rMetadataPath = new Path(path, "rMetadata").toString

      val rMetadataJson: String = compact(render(
        "class" -> instance.getClass.getName
      ))

      sc.parallelize(Seq(rMetadataJson), 1).saveAsTextFile(rMetadataPath)

      instance.fpGrowthModel.save(modelPath)
    }
  }
}
