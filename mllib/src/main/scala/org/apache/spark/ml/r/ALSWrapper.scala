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
    val alsm: ALSModel,
    val rRatingCol: String,
    val rUserCol: String,
    val rItemCol: String) extends MLWritable {

  lazy val rUserFactors: DataFrame = alsm.userFactors

  lazy val rItemFactors: DataFrame = alsm.itemFactors

  def transform(dataset: Dataset[_]): DataFrame = {
    alsm.transform(dataset)
  }

  override def write: MLWriter = new ALSWrapper.ALSWrapperWriter(this)
}

private[r] object ALSWrapper extends MLReadable[ALSWrapper] {

  def fit(data: DataFrame, ratingCol: String, userCol: String, itemCol: String,
          rank: Int, regParam: Double, maxIter: Int): ALSWrapper = {
    val als = new ALS()
      .setRank(rank)
      .setRegParam(regParam)
      .setMaxIter(maxIter)
      .setRatingCol(ratingCol)
      .setUserCol(userCol)
      .setItemCol(itemCol)

    val alsm: ALSModel = als.fit(data)

    new ALSWrapper(alsm, ratingCol, userCol, itemCol)
  }

  override def read: MLReader[ALSWrapper] = new ALSWrapperReader

  override def load(path: String): ALSWrapper = super.load(path)

  class ALSWrapperWriter(instance: ALSWrapper) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val rMetadataPath = new Path(path, "rMetadata").toString
      val modelPath = new Path(path, "model").toString

      val rMetadata = ("class" -> instance.getClass.getName) ~
        ("ratingCol" -> instance.rRatingCol) ~
        ("userCol" -> instance.rUserCol) ~
        ("itemCol" -> instance.rItemCol)

      val rMetadataJson: String = compact(render(rMetadata))
      sc.parallelize(Seq(rMetadataJson), 1).saveAsTextFile(rMetadataPath)
      instance.alsm.save(modelPath)
    }
  }

  class ALSWrapperReader extends MLReader[ALSWrapper] {

    override def load(path: String): ALSWrapper = {
      implicit val format = DefaultFormats
      val rMetadataPath = new Path(path, "rMetadata").toString
      val modelPath = new Path(path, "model").toString

      val rMetadataStr = sc.textFile(rMetadataPath, 1).first()
      val rMetadata = parse(rMetadataStr)
      val rRatingCol = (rMetadata \ "rRatingCol").extract[String]
      val rUserCol = (rMetadata \ "rUserCol").extract[String]
      val rItemCol = (rMetadata \ "rItemCol").extract[String]

      val alsm = ALSModel.load(modelPath)

      new ALSWrapper(alsm, rRatingCol, rUserCol, rItemCol)
    }
  }

}
