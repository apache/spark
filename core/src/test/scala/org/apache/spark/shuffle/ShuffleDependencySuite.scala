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
import org.apache.spark.rdd.RDD

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

  test("PipelinedShuffleDependency is a ShuffleDependency and preserves its fields") {
    sc = new SparkContext("local", "test", conf.clone())
    val rdd: RDD[(KeyClass, ValueClass)] =
      sc.parallelize(1 to 5, 4).map(_ => (KeyClass(), ValueClass()))
    val partitioner = new HashPartitioner(2)
    val dep = new PipelinedShuffleDependency[KeyClass, ValueClass, ValueClass](rdd, partitioner)

    // It is a first-class dependency kind, but IS-A ShuffleDependency: code that matches
    // ShuffleDependency continues to see it as an ordinary shuffle, so it changes no existing
    // behavior. The concurrent-scheduling behavior is keyed on the subtype elsewhere.
    assert(dep.isInstanceOf[ShuffleDependency[_, _, _]])
    assert(dep.partitioner === partitioner)
    assert(dep.keyClassName == classOf[KeyClass].getName)
    assert(dep.valueClassName == classOf[ValueClass].getName)
    assert(dep.rdd === rdd)
    // Construction goes through the normal ShuffleDependency path: the shuffle is registered with
    // the ShuffleManager (a handle is produced) and a second instance gets a distinct shuffleId.
    assert(dep.shuffleHandle != null)
    val dep2 = new PipelinedShuffleDependency[KeyClass, ValueClass, ValueClass](rdd, partitioner)
    assert(dep2.shuffleId != dep.shuffleId)

    // The checksum retry / query-level rollback params are intentionally not exposed by the
    // subclass (see PipelinedShuffleDependency scaladoc): their stage-level recompute is moot for a
    // pipelined group, so they must stay at their false defaults.
    assert(!dep.checksumMismatchFullRetryEnabled)
    assert(!dep.checksumMismatchQueryLevelRollbackEnabled)
  }

  test("PipelinedShuffleDependency forwards non-default constructor args to ShuffleDependency") {
    sc = new SparkContext("local", "test", conf.clone())
    val rdd: RDD[(KeyClass, ValueClass)] =
      sc.parallelize(1 to 5, 4).map(_ => (KeyClass(), ValueClass()))
    val partitioner = new HashPartitioner(2)
    val aggregator = new Aggregator[KeyClass, ValueClass, ValueClass](
      v => v, (c, _) => c, (c1, _) => c1)
    val dep = new PipelinedShuffleDependency[KeyClass, ValueClass, ValueClass](
      rdd, partitioner, aggregator = Some(aggregator), mapSideCombine = true)

    // The subclass forwards its constructor args positionally to ShuffleDependency; assert the
    // non-default ones land where expected rather than on the wrong parameter.
    assert(dep.aggregator.contains(aggregator))
    assert(dep.mapSideCombine)
  }

  test("an ordinary ShuffleDependency is not a PipelinedShuffleDependency") {
    sc = new SparkContext("local", "test", conf.clone())
    val rdd = sc.parallelize(1 to 5, 4).map(_ => (KeyClass(), ValueClass())).groupByKey()
    val dep = rdd.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]]
    assert(!dep.isInstanceOf[PipelinedShuffleDependency[_, _, _]])
  }

}
