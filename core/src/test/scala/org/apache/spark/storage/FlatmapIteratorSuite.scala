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
package org.apache.spark.storage

import org.apache.spark._
import org.apache.spark.internal.config._

class FlatmapIteratorSuite extends SparkFunSuite with LocalSparkContext {
  /* Tests the ability of Spark to deal with user provided iterators from flatMap
   * calls, that may generate more data then available memory. In any
   * memory based persistence Spark will unroll the iterator into an ArrayBuffer
   * for caching, however in the case that the use defines DISK_ONLY persistence,
   * the iterator will be fed directly to the serializer and written to disk.
   *
   * This also tests the ObjectOutputStream reset rate. When serializing using the
   * Java serialization system, the serializer caches objects to prevent writing redundant
   * data, however that stops GC of those objects. By calling 'reset' you flush that
   * info from the serializer, and allow old objects to be GC'd
   */
  test("Flatmap Iterator to Disk") {
    val sconf = new SparkConf().setMaster("local").setAppName("iterator_to_disk_test")
    sc = new SparkContext(sconf)
    val expand_size = 100
    val data = sc.parallelize((1 to 5).toSeq).
      flatMap( x => LazyList.range(0, expand_size))
    val persisted = data.persist(StorageLevel.DISK_ONLY)
    assert(persisted.count()===500)
    assert(persisted.filter(_==1).count()===5)
  }

  test("Flatmap Iterator to Memory") {
    val sconf = new SparkConf().setMaster("local").setAppName("iterator_to_disk_test")
    sc = new SparkContext(sconf)
    val expand_size = 100
    val data = sc.parallelize((1 to 5).toSeq).
      flatMap(x => LazyList.range(0, expand_size))
    val persisted = data.persist(StorageLevel.MEMORY_ONLY)
    assert(persisted.count()===500)
    assert(persisted.filter(_==1).count()===5)
  }

  test("Serializer Reset") {
    val sconf = new SparkConf().setMaster("local").setAppName("serializer_reset_test")
      .set(SERIALIZER_OBJECT_STREAM_RESET, 10)
    sc = new SparkContext(sconf)
    val expand_size = 500
    val data = sc.parallelize(Seq(1, 2)).
      flatMap(x => LazyList.range(1, expand_size).
      map(y => "%d: string test %d".format(y, x)))
    val persisted = data.persist(StorageLevel.MEMORY_ONLY_SER)
    assert(persisted.filter(_.startsWith("1:")).count()===2)
  }

}
