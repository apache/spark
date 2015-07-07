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
package org.apache.spark.mllib.fpm

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext

class PrefixspanSuite extends SparkFunSuite with MLlibTestSparkContext {

  test("Prefixspan sequences mining using Integer type") {
    val sequences = Array(
      Array(3, 1, 3, 4, 5),
      Array(2, 3, 1),
      Array(3, 4, 4, 3),
      Array(1, 3, 4, 5),
      Array(2, 4, 1),
      Array(6, 5, 3))

    val rdd = sc.parallelize(sequences, 2).cache()

    val prefixspan1 = new Prefixspan(rdd, 2, 50)
    val result1 = prefixspan1.run()
    assert(result1.count() == 19)

    val prefixspan2 = new Prefixspan(rdd, 3, 50)
    val result2 = prefixspan2.run()
    assert(result2.count() == 5)

    val prefixspan3 = new Prefixspan(rdd, 2, 2)
    val result3 = prefixspan3.run()
    assert(result3.count() == 14)
  }
}
