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

package org.apache.spark.mllib.feature

import org.scalatest.FunSuite

import org.apache.spark.SparkContext._
import org.apache.spark.mllib.util.LocalSparkContext

class Word2VecSuite extends FunSuite with LocalSparkContext {
  test("Word2Vec") {
    val sentence = "a b " * 100 + "a c " * 10
    val localDoc = Seq(sentence, sentence)
    val doc = sc.parallelize(localDoc)
      .map(line => line.split(" ").toSeq)
    val size = 10
    val startingAlpha = 0.025
    val window = 2 
    val minCount = 2
    val num = 2
    val word = "a"

    val model = Word2Vec.train(doc, size, startingAlpha, window, minCount)
    val synons = model.findSynonyms("a", 2)
    assert(synons.length == num)
    assert(synons(0)._1 == "b")
    assert(synons(1)._1 == "c")
  }


  test("Word2VecModel") {
    val num = 2
    val localModel = Seq(
      ("china" ,  Array(0.50, 0.50, 0.50, 0.50)),
      ("japan" ,  Array(0.40, 0.50, 0.50, 0.50)),
      ("taiwan",  Array(0.60, 0.50, 0.50, 0.50)),
      ("korea" ,  Array(0.45, 0.60, 0.60, 0.60))
    )
    val model = new Word2VecModel(sc.parallelize(localModel, 2))
    val synons = model.findSynonyms("china", num)
    assert(synons.length == num)
    assert(synons(0)._1 == "taiwan")
    assert(synons(1)._1 == "japan")
  }
}
