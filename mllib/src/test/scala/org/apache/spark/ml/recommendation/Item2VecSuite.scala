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

package org.apache.spark.ml.recommendation

import org.apache.spark.internal.Logging
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest}

class Item2VecSuite extends MLTest with DefaultReadWriteTest with Logging {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("Item2Vec test") {
    val spark = this.spark
    import spark.implicits._

    val rnd = new java.util.Random(239)
    val data = sc.parallelize(
      (0 until 100000)
        .map{_ =>
          val x = rnd.nextInt(100).toLong
          (0 until 10).map(i => (x + i)  % 100).toArray
        }
    ).toDF("input")

    val model = new Item2Vec()
      .setRank(10)
      .setNegative(5)
      .setWindowSize(5)
      .setSamplingMode("ITEM2VEC")
      .setFitIntercept(true)
      .setRegParam(0.001)
      .setMaxIter(10)
      .setStepSize(0.025)
      .setMinCount(1)
      .setParallelism(1)
      .setNumPartitions(5)
      .setPow(0)
      .setSeed(239)
      .setIntermediateStorageLevel("DISK_ONLY")
      .setFinalStorageLevel("DISK_ONLY")
      .fit(data)

    val left = model.contextFactors
      .select("id", "features", "intercept")
      .as[(Long, Array[Float], Float)]
      .map{case (id, f, b) =>
        id -> (f, b)
      }.collect().toMap

    val right = model.itemFactors
      .select("id", "features", "intercept")
      .as[(Long, Array[Float], Float)]
      .map{case (id, f, b) =>
        id -> (f, b)
      }.collect().toMap

    def predict(x: (Array[Float], Float), y: (Array[Float], Float)): Double = {
      x._1.zip(y._1).map(e => e._1 * e._2).sum + x._2 + y._2
    }

    val recs = right.map(e => e._1 -> predict(left(10), e._2))
      .toArray.sortBy(-_._2).map(_._1).take(20).toSet

    assert((5L to 15L).forall(recs.contains))
  }
}

object Item2VecSuite extends Logging {

}
