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

package org.apache.spark.mllib.tree.configuration

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.tree.configuration.Algo.Classification
import org.apache.spark.mllib.tree.configuration.QuantileStrategy.Sort
import org.apache.spark.mllib.tree.impurity.Gini

class StrategySuite extends SparkFunSuite {

  test("retain the constructor signature used by Spark 4.2") {
    assert(classOf[Strategy].getConstructors.exists(_.getParameterCount == 15))
    for (bootstrap <- Seq(false, true)) {
      val strategy = new Strategy(Classification, Gini, 3, 2, 32, Sort, Map.empty[Int, Int],
        1, 0.0, 256, 1.0, false, 10, 0.0, bootstrap)

      assert(strategy.bootstrap == bootstrap)
      assert(strategy.pruneTree)
    }
  }

  test("retain the bootstrap default getter used by compiled callers") {
    val bootstrapDefault = Strategy.getClass.getMethod("$lessinit$greater$default$15")
    assert(bootstrapDefault.invoke(Strategy) == java.lang.Boolean.FALSE)
  }

  test("configure pruning independently of bootstrapping") {
    val strategy = new Strategy(Classification, Gini, 3, bootstrap = true, pruneTree = false)

    assert(strategy.bootstrap)
    assert(!strategy.pruneTree)
    assert(!strategy.copy.pruneTree)
  }
}
