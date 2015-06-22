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

package org.apache.spark.shuffle.unsafe

import java.io.File

import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.TrueFileFilter
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{HashPartitioner, ShuffleDependency, SparkContext, ShuffleSuite}
import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.util.Utils

class UnsafeShuffleSuite extends ShuffleSuite with BeforeAndAfterAll {

  // This test suite should run all tests in ShuffleSuite with unsafe-based shuffle.

  override def beforeAll() {
    conf.set("spark.shuffle.manager", "tungsten-sort")
    // UnsafeShuffleManager requires at least 128 MB of memory per task in order to be able to sort
    // shuffle records.
    conf.set("spark.shuffle.memoryFraction", "0.5")
  }

  test("UnsafeShuffleManager properly cleans up files for shuffles that use the new shuffle path") {
    val tmpDir = Utils.createTempDir()
    try {
      val myConf = conf.clone()
        .set("spark.local.dir", tmpDir.getAbsolutePath)
      sc = new SparkContext("local", "test", myConf)
      // Create a shuffled RDD and verify that it will actually use the new UnsafeShuffle path
      val rdd = sc.parallelize(1 to 10, 1).map(x => (x, x))
      val shuffledRdd = new ShuffledRDD[Int, Int, Int](rdd, new HashPartitioner(4))
        .setSerializer(new KryoSerializer(myConf))
      val shuffleDep = shuffledRdd.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]]
      assert(UnsafeShuffleManager.canUseUnsafeShuffle(shuffleDep))
      def getAllFiles: Set[File] =
        FileUtils.listFiles(tmpDir, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE).asScala.toSet
      val filesBeforeShuffle = getAllFiles
      // Force the shuffle to be performed
      shuffledRdd.count()
      // Ensure that the shuffle actually created files that will need to be cleaned up
      val filesCreatedByShuffle = getAllFiles -- filesBeforeShuffle
      filesCreatedByShuffle.map(_.getName) should be
        Set("shuffle_0_0_0.data", "shuffle_0_0_0.index")
      // Check that the cleanup actually removes the files
      sc.env.blockManager.master.removeShuffle(0, blocking = true)
      for (file <- filesCreatedByShuffle) {
        assert (!file.exists(), s"Shuffle file $file was not cleaned up")
      }
    } finally {
      Utils.deleteRecursively(tmpDir)
    }
  }

  test("UnsafeShuffleManager properly cleans up files for shuffles that use the old shuffle path") {
    val tmpDir = Utils.createTempDir()
    try {
      val myConf = conf.clone()
        .set("spark.local.dir", tmpDir.getAbsolutePath)
      sc = new SparkContext("local", "test", myConf)
      // Create a shuffled RDD and verify that it will actually use the old SortShuffle path
      val rdd = sc.parallelize(1 to 10, 1).map(x => (x, x))
      val shuffledRdd = new ShuffledRDD[Int, Int, Int](rdd, new HashPartitioner(4))
        .setSerializer(new JavaSerializer(myConf))
      val shuffleDep = shuffledRdd.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]]
      assert(!UnsafeShuffleManager.canUseUnsafeShuffle(shuffleDep))
      def getAllFiles: Set[File] =
        FileUtils.listFiles(tmpDir, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE).asScala.toSet
      val filesBeforeShuffle = getAllFiles
      // Force the shuffle to be performed
      shuffledRdd.count()
      // Ensure that the shuffle actually created files that will need to be cleaned up
      val filesCreatedByShuffle = getAllFiles -- filesBeforeShuffle
      filesCreatedByShuffle.map(_.getName) should be
        Set("shuffle_0_0_0.data", "shuffle_0_0_0.index")
      // Check that the cleanup actually removes the files
      sc.env.blockManager.master.removeShuffle(0, blocking = true)
      for (file <- filesCreatedByShuffle) {
        assert (!file.exists(), s"Shuffle file $file was not cleaned up")
      }
    } finally {
      Utils.deleteRecursively(tmpDir)
    }
  }
}
