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
package org.apache.spark.shuffle

import org.apache.spark._

case class KeyClass()

case class ValueClass()

case class CombinerClass()

class ShuffleDependencySuite extends SparkFunSuite with LocalSparkContext {

  val conf = new SparkConf(loadDefaults = false)

  test("key, value, and combiner classes correct in shuffle dependency without aggregation") {
    sc = new SparkContext("local", "test", conf.clone())
    val rdd = sc.parallelize(1 to 5, 4)
      .map(key => (KeyClass(), ValueClass()))
      .groupByKey()
    val dep = rdd.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]]
    assert(!dep.mapSideCombine, "Test requires that no map-side aggregator is defined")
    assert(dep.keyClassName == classOf[KeyClass].getName)
    assert(dep.valueClassName == classOf[ValueClass].getName)
  }

  test("key, value, and combiner classes available in shuffle dependency with aggregation") {
    sc = new SparkContext("local", "test", conf.clone())
    val rdd = sc.parallelize(1 to 5, 4)
      .map(key => (KeyClass(), ValueClass()))
      .aggregateByKey(CombinerClass())({ case (a, b) => a }, { case (a, b) => a })
    val dep = rdd.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]]
    assert(dep.mapSideCombine && dep.aggregator.isDefined, "Test requires map-side aggregation")
    assert(dep.keyClassName == classOf[KeyClass].getName)
    assert(dep.valueClassName == classOf[ValueClass].getName)
    assert(dep.combinerClassName == Some(classOf[CombinerClass].getName))
  }

  test("combineByKey null combiner class tag handled correctly") {
    sc = new SparkContext("local", "test", conf.clone())
    val rdd = sc.parallelize(1 to 5, 4)
      .map(key => (KeyClass(), ValueClass()))
      .combineByKey((v: ValueClass) => v,
        (c: AnyRef, v: ValueClass) => c,
        (c1: AnyRef, c2: AnyRef) => c1)
    val dep = rdd.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]]
    assert(dep.keyClassName == classOf[KeyClass].getName)
    assert(dep.valueClassName == classOf[ValueClass].getName)
    assert(dep.combinerClassName == None)
  }

}
