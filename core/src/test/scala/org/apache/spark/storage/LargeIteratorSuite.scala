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

import org.scalatest.FunSuite
import org.apache.spark.{SparkConf, LocalSparkContext, SparkContext}

class LargeIteratorSuite extends FunSuite with LocalSparkContext {
/* Tests the ability of Spark to deal with user provided iterators that
 * generate more data then available memory. In any memory based persistance
 * Spark will unroll the iterator into an ArrayBuffer for caching, however in
 * the case that the use defines DISK_ONLY persistance, the iterator will be
 * fed directly to the serializer and written to disk.
 */
test("Flatmap iterator") {
  //create a local spark cluster, but limit it to 128mb of RAM
  val sconf = new SparkConf().setMaster("local-cluster[1,1,128]")
    .setAppName("mem_test")
    .set("spark.executor.memory", "128m")
  sc = new SparkContext(sconf)
  try {
    val expand_size = 1000000
    val data = sc.parallelize( (1 to 4).toSeq ).
      flatMap( x => Stream.range(1, expand_size).
      map( y => "%d: string test %d".format(y,x) ) )
    // If we did persist(StorageLevel.MEMORY_ONLY), it would cause OutOfMemoryError
    var persisted = data.persist(StorageLevel.DISK_ONLY)
    assert( persisted.filter( _.startsWith("1:") ).count() == 4 )
  } catch {
    case _ : OutOfMemoryError => assert(false)
  }
}
}
