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

package org.apache.spark.mllib.tree

import org.apache.spark.mllib.tree.impurity._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.scalatest.FunSuite

/**
 * Test suite for [[org.apache.spark.mllib.tree.impurity.Gini]]
 */
class GiniImpuritySuite extends FunSuite with MLlibTestSparkContext{
  test("Gini impurity does not support negative labels") {
    val gini = new GiniAggregator(2)
    try {
      gini.update(Array(0.0, 1.0, 2.0), 0, -1, 0.0)
      fail()
    } catch {
      case _: IllegalArgumentException =>
    }
  }
}
