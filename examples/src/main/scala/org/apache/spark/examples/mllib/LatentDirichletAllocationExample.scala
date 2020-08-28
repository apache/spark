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

// scalastyle:off println
package org.apache.spark.examples.mllib

import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg.Vectors
// $example off$

object LatentDirichletAllocationExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LatentDirichletAllocationExample")
    val sc = new SparkContext(conf)

    // $example on$
    // Load and parse the data
    val data = sc.textFile("data/mllib/sample_lda_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble)))
    // Index documents with unique IDs
    val corpus = parsedData.zipWithIndex.map(_.swap).cache()

    // Cluster the documents into three topics using LDA
    val ldaModel = new LDA().setK(3).run(corpus)

    // Output topics. Each is a distribution over words (matching word count vectors)
    println(s"Learned topics (as distributions over vocab of ${ldaModel.vocabSize} words):")
    val topics = ldaModel.topicsMatrix
    for (topic <- Range(0, 3)) {
      print(s"Topic $topic :")
      for (word <- Range(0, ldaModel.vocabSize)) {
        print(s"${topics(word, topic)}")
      }
      println()
    }

    // Save and load model.
    ldaModel.save(sc, "target/org/apache/spark/LatentDirichletAllocationExample/LDAModel")
    val sameModel = DistributedLDAModel.load(sc,
      "target/org/apache/spark/LatentDirichletAllocationExample/LDAModel")
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
