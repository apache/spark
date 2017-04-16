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

package org.apache.spark.mllib.preprocessing

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.util.LocalSparkContext

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers

class OneHotEncoderSuite extends FunSuite with LocalSparkContext with ShouldMatchers {

  test("one hot encoder") {
    val vecs = Array(
      Array("marcy playground", 1.3, "apple", 2),
      Array("pearl jam", 3.5, "banana", 4),
      Array("nirvana", 6.7, "apple", 3)
    )
    val categoricalFields = Array(0, 2)
    val rdd = sc.parallelize(vecs, 2)

    val catMap = OneHotEncoder.categories(rdd, categoricalFields)
    val encoded = OneHotEncoder.encode(rdd, catMap)

    val result = encoded.collect()
    result.size should be (vecs.size)

    val vec1 = Array[Any](0, 0, 0, 1.3, 0, 0, 2)
    vec1(catMap(0)._2.getOrElse("marcy playground", -1)) = 1
    vec1(4 + catMap(1)._2.getOrElse("apple", -1)) = 1

    val vec2 = Array[Any](0, 0, 0, 3.5, 0, 0, 4)
    vec2(catMap(0)._2.getOrElse("pearl jam", -1)) = 1
    vec2(4 + catMap(1)._2.getOrElse("banana", -1)) = 1

    val vec3 = Array[Any](0, 0, 0, 6.7, 0, 0, 3)
    vec3(catMap(0)._2.getOrElse("nirvana", -1)) = 1
    vec3(4 + catMap(1)._2.getOrElse("apple", -1)) = 1

    result(0) should equal (vec1)
    result(1) should equal (vec2)
    result(2) should equal (vec3)
  }
}

