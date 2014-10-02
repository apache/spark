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
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vectors}
import org.apache.spark.mllib.util.LocalSparkContext
import org.apache.spark.mllib.util.TestingUtils._

class IDFSuite extends FunSuite with LocalSparkContext {

  test("idf") {
    val n = 4
    val localTermFrequencies = Seq(
      Vectors.sparse(n, Array(1, 3), Array(1.0, 2.0)),
      Vectors.dense(0.0, 1.0, 2.0, 3.0),
      Vectors.sparse(n, Array(1), Array(1.0))
    )
    val m = localTermFrequencies.size
    val termFrequencies = sc.parallelize(localTermFrequencies, 2)
    val idf = new IDF
    val model = idf.fit(termFrequencies)
    val expected = Vectors.dense(Array(0, 3, 1, 2).map { x =>
      math.log((m + 1.0) / (x + 1.0))
    })
    assert(model.idf ~== expected absTol 1e-12)
    val tfidf = model.transform(termFrequencies).cache().zipWithIndex().map(_.swap).collectAsMap()
    assert(tfidf.size === 3)
    val tfidf0 = tfidf(0L).asInstanceOf[SparseVector]
    assert(tfidf0.indices === Array(1, 3))
    assert(Vectors.dense(tfidf0.values) ~==
      Vectors.dense(1.0 * expected(1), 2.0 * expected(3)) absTol 1e-12)
    val tfidf1 = tfidf(1L).asInstanceOf[DenseVector]
    assert(Vectors.dense(tfidf1.values) ~==
      Vectors.dense(0.0, 1.0 * expected(1), 2.0 * expected(2), 3.0 * expected(3)) absTol 1e-12)
    val tfidf2 = tfidf(2L).asInstanceOf[SparseVector]
    assert(tfidf2.indices === Array(1))
    assert(tfidf2.values(0) ~== (1.0 * expected(1)) absTol 1e-12)
  }

  test("idf minimum document frequency filtering") {
    val n = 4
    val localTermFrequencies = Seq(
      Vectors.sparse(n, Array(1, 3), Array(1.0, 2.0)),
      Vectors.dense(0.0, 1.0, 2.0, 3.0),
      Vectors.sparse(n, Array(1), Array(1.0))
    )
    val m = localTermFrequencies.size
    val termFrequencies = sc.parallelize(localTermFrequencies, 2)
    val idf = new IDF(minDocFreq = 1)
    val model = idf.fit(termFrequencies)
    val expected = Vectors.dense(Array(0, 3, 1, 2).map { x =>
      if (x > 0) {
        math.log((m + 1.0) / (x + 1.0))
      } else {
        0
      }
    })
    assert(model.idf ~== expected absTol 1e-12)
    val tfidf = model.transform(termFrequencies).cache().zipWithIndex().map(_.swap).collectAsMap()
    assert(tfidf.size === 3)
    val tfidf0 = tfidf(0L).asInstanceOf[SparseVector]
    assert(tfidf0.indices === Array(1, 3))
    assert(Vectors.dense(tfidf0.values) ~==
      Vectors.dense(1.0 * expected(1), 2.0 * expected(3)) absTol 1e-12)
    val tfidf1 = tfidf(1L).asInstanceOf[DenseVector]
    assert(Vectors.dense(tfidf1.values) ~==
      Vectors.dense(0.0, 1.0 * expected(1), 2.0 * expected(2), 3.0 * expected(3)) absTol 1e-12)
    val tfidf2 = tfidf(2L).asInstanceOf[SparseVector]
    assert(tfidf2.indices === Array(1))
    assert(tfidf2.values(0) ~== (1.0 * expected(1)) absTol 1e-12)
  }

}
