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

package org.apache.spark

import org.scalatest.FunSuite

import org.apache.spark.storage.ShuffleBlockId


class ShuffleFaultToleranceSuite extends FunSuite with LocalSparkContext {

  test("[SPARK-4085] hash shuffle manager recovers when local shuffle files get deleted") {
    val conf = new SparkConf(false)
    conf.set("spark.shuffle.manager", "hash")
    sc = new SparkContext("local", "test", conf)
    val rdd = sc.parallelize(1 to 10, 2).map((_, 1)).reduceByKey(_ + _)
    rdd.count()

    // Delete one of the local shuffle blocks.
    val shuffleFile = sc.env.blockManager.diskBlockManager.getFile(new ShuffleBlockId(0, 0, 0))
    assert(shuffleFile.exists())
    shuffleFile.delete()

    // This count should retry the execution of the previous stage and rerun shuffle.
    rdd.count()
  }
}
