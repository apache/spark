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

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.LocalSparkContext

class HashingTFSuite extends FunSuite with LocalSparkContext {

  test("hashing tf on a single doc") {
    val hashingTF = new HashingTF(1000)
    val doc = "a a b b c d".split(" ")
    val n = hashingTF.numFeatures
    val termFreqs = Seq(
      (hashingTF.indexOf("a"), 2.0),
      (hashingTF.indexOf("b"), 2.0),
      (hashingTF.indexOf("c"), 1.0),
      (hashingTF.indexOf("d"), 1.0))
    assert(termFreqs.map(_._1).forall(i => i >= 0 && i < n),
      "index must be in range [0, #features)")
    assert(termFreqs.map(_._1).toSet.size === 4, "expecting perfect hashing")
    val expected = Vectors.sparse(n, termFreqs)
    assert(hashingTF.transform(doc) === expected)
  }

  test("hashing tf on an RDD") {
    val hashingTF = new HashingTF
    val localDocs: Seq[Seq[String]] = Seq(
      "a a b b b c d".split(" "),
      "a b c d a b c".split(" "),
      "c b a c b a a".split(" "))
    val docs = sc.parallelize(localDocs, 2)
    assert(hashingTF.transform(docs).collect().toSet === localDocs.map(hashingTF.transform).toSet)
  }
}
