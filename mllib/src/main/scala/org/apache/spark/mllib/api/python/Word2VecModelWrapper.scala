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

package org.apache.spark.mllib.api.python

import java.util.{List => JList, Map => JMap}

import scala.collection.JavaConverters._

import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
 * Wrapper around Word2VecModel to provide helper methods in Python
 */
private[python] class Word2VecModelWrapper(model: Word2VecModel) {
  def transform(word: String): Vector = {
    model.transform(word)
  }

  /**
   * Transforms an RDD of words to its vector representation
   * @param rdd an RDD of words
   * @return an RDD of vector representations of words
   */
  def transform(rdd: JavaRDD[String]): JavaRDD[Vector] = {
    rdd.rdd.map(model.transform)
  }

  def findSynonyms(word: String, num: Int): JList[Object] = {
    val vec = transform(word)
    findSynonyms(vec, num)
  }

  def findSynonyms(vector: Vector, num: Int): JList[Object] = {
    val result = model.findSynonyms(vector, num)
    val similarity = Vectors.dense(result.map(_._2))
    val words = result.map(_._1)
    List(words, similarity).map(_.asInstanceOf[Object]).asJava
  }

  def getVectors: JMap[String, JList[Float]] = {
    model.getVectors.map { case (k, v) =>
      (k, v.toList.asJava)
    }.asJava
  }

  def save(sc: SparkContext, path: String): Unit = model.save(sc, path)
}
