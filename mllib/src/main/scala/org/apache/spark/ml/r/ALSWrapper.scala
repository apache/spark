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

import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}

private[r] class ALSWrapper private (
    val alsModel: ALSModel,
    val ratingCol: String) extends MLWritable {

  lazy val userCol: String = alsModel.getUserCol
  lazy val itemCol: String = alsModel.getItemCol
  lazy val userFactors: DataFrame = alsModel.userFactors
  lazy val itemFactors: DataFrame = alsModel.itemFactors
  lazy val rank: Int = alsModel.rank

  def transform(dataset: Dataset[_]): DataFrame = {
    alsModel.transform(dataset)
  }

  override def write: MLWriter = new ALSWrapper.ALSWrapperWriter(this)
}

private[r] object ALSWrapper extends MLReadable[ALSWrapper] {

  def fit(  // scalastyle:ignore
      data: DataFrame,
      ratingCol: String,
      userCol: String,
      itemCol: String,
      rank: Int,
      regParam: Double,
      maxIter: Int,
      implicitPrefs: Boolean,
      alpha: Double,
      nonnegative: Boolean,
      numUserBlocks: Int,
      numItemBlocks: Int,
      checkpointInterval: Int,
      seed: Int): ALSWrapper = {

    val als = new ALS()
      .setRatingCol(ratingCol)
      .setUserCol(userCol)
      .setItemCol(itemCol)
      .setRank(rank)
      .setRegParam(regParam)
      .setMaxIter(maxIter)
      .setImplicitPrefs(implicitPrefs)
      .setAlpha(alpha)
      .setNonnegative(nonnegative)
      .setNumBlocks(numUserBlocks)
      .setNumItemBlocks(numItemBlocks)
      .setCheckpointInterval(checkpointInterval)
      .setSeed(seed.toLong)

    val alsModel: ALSModel = als.fit(data)

    new ALSWrapper(alsModel, ratingCol)
  }

  override def read: MLReader[ALSWrapper] = new ALSWrapperReader

  override def load(path: String): ALSWrapper = super.load(path)

  class ALSWrapperWriter(instance: ALSWrapper) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val rMetadataPath = new Path(path, "rMetadata").toString
      val modelPath = new Path(path, "model").toString

      val rMetadata = ("class" -> instance.getClass.getName) ~
        ("ratingCol" -> instance.ratingCol)
      val rMetadataJson: String = compact(render(rMetadata))
      sc.parallelize(Seq(rMetadataJson), 1).saveAsTextFile(rMetadataPath)

      instance.alsModel.save(modelPath)
    }
  }

  class ALSWrapperReader extends MLReader[ALSWrapper] {

    override def load(path: String): ALSWrapper = {
      implicit val format = DefaultFormats
      val rMetadataPath = new Path(path, "rMetadata").toString
      val modelPath = new Path(path, "model").toString

      val rMetadataStr = sc.textFile(rMetadataPath, 1).first()
      val rMetadata = parse(rMetadataStr)
      val ratingCol = (rMetadata \ "ratingCol").extract[String]
      val alsModel = ALSModel.load(modelPath)

      new ALSWrapper(alsModel, ratingCol)
    }
  }

}
