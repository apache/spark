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
import org.apache.spark.mllib.feature.SkipGramModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD


/**
 * Wrapper around SkipGram to provide helper methods in Python
 */
private[python] class SkipGramModelWrapper(model: SkipGramModel) {
  /**
   * Finds synonyms of a word; do not include the word itself in results.
   * @param word a word
   * @param num number of synonyms to find
   * @return a list consisting of a list of words and a vector of cosine similarities
   */
  def findSynonyms(word: String, num: Int): JList[Object] = {
    prepareResult(model.findSynonyms(word, num))
  }

  /**
   * Finds words similar to the vector representation of a word without
   * filtering results.
   * @param vector a vector
   * @param num number of synonyms to find
   * @return a list consisting of a list of words and a vector of cosine similarities
   */
  def findSynonyms(vector: Vector, num: Int): JList[Object] = {
    prepareResult(model.findSynonyms(vector.toArray.map(_.toFloat), num))
  }

  private def prepareResult(result: Array[(String, Double)]) = {
    val similarity = Vectors.dense(result.map(_._2))
    val words = result.map(_._1)
    List(words, similarity).map(_.asInstanceOf[Object]).asJava
  }

  def save(sc: SparkContext, path: String): Unit = model.save(sc, path)

  def getVectors: RDD[Array[Any]] = {
    model.getVectors.map(x => Array(x._1.asInstanceOf[Any],
      Array(x._2._1.asInstanceOf[Any], x._2._2.asInstanceOf[Any], x._2._3.asInstanceOf[Any])))
  }
}
