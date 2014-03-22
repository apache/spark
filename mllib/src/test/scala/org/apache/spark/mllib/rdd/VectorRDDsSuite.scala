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

package org.apache.spark.mllib.rdd

import org.scalatest.FunSuite

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.LocalSparkContext

class VectorRDDsSuite extends FunSuite with LocalSparkContext {

  test("from array rdd") {
    val data = Seq(Array(1.0, 2.0), Array(3.0, 4.0))
    val arrayRdd = sc.parallelize(data, 2)
    val vectorRdd = VectorRDDs.fromArrayRDD(arrayRdd)
    assert(arrayRdd.collect().map(v => Vectors.dense(v)) === vectorRdd.collect())
  }
}
