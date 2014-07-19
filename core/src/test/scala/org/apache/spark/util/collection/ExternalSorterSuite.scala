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

package org.apache.spark.util.collection

import org.scalatest.FunSuite

import org.apache.spark._
import org.apache.spark.SparkContext._
import scala.Some

class ExternalSorterSuite extends FunSuite with LocalSparkContext {
  ignore("spilling in local cluster") {
    val conf = new SparkConf(true)  // Load defaults, otherwise SPARK_HOME is not found
    conf.set("spark.shuffle.memoryFraction", "0.001")
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.SortShuffleManager")
    sc = new SparkContext("local-cluster[1,1,512]", "test", conf)

    // reduceByKey - should spill ~8 times
    val rddA = sc.parallelize(0 until 100000).map(i => (i/2, i))
    val resultA = rddA.reduceByKey(math.max).collect()
    assert(resultA.length == 50000)
    resultA.foreach { case(k, v) =>
      k match {
        case 0 => assert(v == 1)
        case 25000 => assert(v == 50001)
        case 49999 => assert(v == 99999)
        case _ =>
      }
    }

    // groupByKey - should spill ~17 times
    val rddB = sc.parallelize(0 until 100000).map(i => (i/4, i))
    val resultB = rddB.groupByKey().collect()
    assert(resultB.length == 25000)
    resultB.foreach { case(i, seq) =>
      i match {
        case 0 => assert(seq.toSet == Set[Int](0, 1, 2, 3))
        case 12500 => assert(seq.toSet == Set[Int](50000, 50001, 50002, 50003))
        case 24999 => assert(seq.toSet == Set[Int](99996, 99997, 99998, 99999))
        case _ =>
      }
    }

    // cogroup - should spill ~7 times
    val rddC1 = sc.parallelize(0 until 10000).map(i => (i, i))
    val rddC2 = sc.parallelize(0 until 10000).map(i => (i%1000, i))
    val resultC = rddC1.cogroup(rddC2).collect()
    assert(resultC.length == 10000)
    resultC.foreach { case(i, (seq1, seq2)) =>
      i match {
        case 0 =>
          assert(seq1.toSet == Set[Int](0))
          assert(seq2.toSet == Set[Int](0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000))
        case 5000 =>
          assert(seq1.toSet == Set[Int](5000))
          assert(seq2.toSet == Set[Int]())
        case 9999 =>
          assert(seq1.toSet == Set[Int](9999))
          assert(seq2.toSet == Set[Int]())
        case _ =>
      }
    }
  }

  test("cleanup of intermediate files in sorter") {
    val conf = new SparkConf(true)  // Load defaults, otherwise SPARK_HOME is not found
    conf.set("spark.shuffle.memoryFraction", "0.001")
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.SortShuffleManager")
    sc = new SparkContext("local", "test", conf)
    val diskBlockManager = SparkEnv.get.blockManager.diskBlockManager

    val sorter = new ExternalSorter[Int, Int, Int](None, Some(new HashPartitioner(3)), None, None)
    sorter.write((0 until 100000).iterator.map(i => (i, i)))
    assert(diskBlockManager.getAllFiles().length > 0)
    sorter.stop()
    assert(diskBlockManager.getAllBlocks().length === 0)

    val sorter2 = new ExternalSorter[Int, Int, Int](None, Some(new HashPartitioner(3)), None, None)
    sorter2.write((0 until 100000).iterator.map(i => (i, i)))
    assert(diskBlockManager.getAllFiles().length > 0)
    assert(sorter2.iterator.toSet === (0 until 100000).map(i => (i, i)).toSet)
    sorter2.stop()
    assert(diskBlockManager.getAllBlocks().length === 0)
  }

  test("cleanup of intermediate files in sorter if there are errors") {
    val conf = new SparkConf(true)  // Load defaults, otherwise SPARK_HOME is not found
    conf.set("spark.shuffle.memoryFraction", "0.001")
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.SortShuffleManager")
    sc = new SparkContext("local", "test", conf)
    val diskBlockManager = SparkEnv.get.blockManager.diskBlockManager

    val sorter = new ExternalSorter[Int, Int, Int](None, Some(new HashPartitioner(3)), None, None)
    intercept[SparkException] {
      sorter.write((0 until 100000).iterator.map(i => {
        if (i == 99990) {
          throw new SparkException("Intentional failure")
        }
        (i, i)
      }))
    }
    assert(diskBlockManager.getAllFiles().length > 0)
    sorter.stop()
    assert(diskBlockManager.getAllBlocks().length === 0)
  }

  test("cleanup of intermediate files in shuffle") {
    val conf = new SparkConf(true)  // Load defaults, otherwise SPARK_HOME is not found
    conf.set("spark.shuffle.memoryFraction", "0.001")
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.SortShuffleManager")
    sc = new SparkContext("local", "test", conf)
    val diskBlockManager = SparkEnv.get.blockManager.diskBlockManager

    val data = sc.parallelize(0 until 100000, 2).map(i => (i, i))
    assert(data.reduceByKey(_ + _).count() === 100000)

    // After the shuffle, there should be only 4 files on disk: our two map output files and
    // their index files. All other intermediate files should've been deleted.
    assert(diskBlockManager.getAllFiles().length === 4)
  }

  test("cleanup of intermediate files in shuffle with errors") {
    val conf = new SparkConf(true)  // Load defaults, otherwise SPARK_HOME is not found
    conf.set("spark.shuffle.memoryFraction", "0.001")
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.SortShuffleManager")
    sc = new SparkContext("local", "test", conf)
    val diskBlockManager = SparkEnv.get.blockManager.diskBlockManager

    val data = sc.parallelize(0 until 100000, 2).map(i => {
      if (i == 99990) {
        throw new Exception("Intentional failure")
      }
      (i, i)
    })
    intercept[SparkException] {
      data.reduceByKey(_ + _).count()
    }

    // After the shuffle, there should be only 2 files on disk: the output of task 1 and its index.
    // All other files (map 2's output and intermediate merge files) should've been deleted.
    assert(diskBlockManager.getAllFiles().length === 2)
  }
}
