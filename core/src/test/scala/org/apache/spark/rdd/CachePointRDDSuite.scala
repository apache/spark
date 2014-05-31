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

package org.apache.spark.rdd

import org.scalatest.FunSuite

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel

class CachePointRDDSuite extends FunSuite with LocalSparkContext {

  test("local SparkContext") {
    sc = new SparkContext("local", "test")
    val rdd = sc.parallelize(Seq(1, 2, 3, 4))
    val cachePointRDD = rdd.cachePoint()
    assert(cachePointRDD.collect() === rdd.collect())
    val errorCachePointRDD = new CachePointRDD[Int](sc, 4)
    intercept[SparkException] {
      errorCachePointRDD.count()
    }
  }

  test("local cluster SparkContext") {
    sc = new SparkContext("local-cluster[2 , 1 , 512]", "test")
    val rdd = sc.parallelize(Seq(1, 2, 3, 4))
    val cachePointRDD = rdd.cachePoint(StorageLevel.DISK_ONLY)
    assert(cachePointRDD.dependencies.size === 0)
    assert(cachePointRDD.collect() === rdd.collect())
  }

  test("periodically clean up block open") {
    val conf = new SparkConf()
    conf.set("spark.cleaner.ttl", "1800")
    sc = new SparkContext("local-cluster[2 , 1 , 512]", "test", conf)
    val rdd = sc.parallelize(Seq(1, 2, 3, 4)).filter(t => t < 3)
    val cachePointRDD = rdd.cachePoint()
    assert(cachePointRDD.dependencies.size === 1)
  }
}
