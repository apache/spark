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

import java.io.File

import scala.jdk.CollectionConverters._

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.TrueFileFilter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.internal.config.SHUFFLE_MANAGER
import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.shuffle.sort.SortShuffleManager

class SortShuffleSuite extends ShuffleSuite with BeforeAndAfterAll {

  // This test suite should run all tests in ShuffleSuite with sort-based shuffle.
  override def beforeAll(): Unit = {
    super.beforeAll()
    conf.set(SHUFFLE_MANAGER, "sort")
  }

  test("SortShuffleManager properly cleans up files for shuffles that use the serialized path") {
    sc = new SparkContext("local", "test", conf)
    // Create a shuffled RDD and verify that it actually uses the new serialized map output path
    val rdd = sc.parallelize(1 to 10, 1).map(x => (x, x))
    val shuffledRdd = new ShuffledRDD[Int, Int, Int](rdd, new HashPartitioner(4))
      .setSerializer(new KryoSerializer(conf))
    val shuffleDep = shuffledRdd.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]]
    assert(SortShuffleManager.canUseSerializedShuffle(shuffleDep))
    ensureFilesAreCleanedUp(shuffledRdd)
  }

  test("SortShuffleManager properly cleans up files for shuffles that use the deserialized path") {
    sc = new SparkContext("local", "test", conf)
    // Create a shuffled RDD and verify that it actually uses the old deserialized map output path
    val rdd = sc.parallelize(1 to 10, 1).map(x => (x, x))
    val shuffledRdd = new ShuffledRDD[Int, Int, Int](rdd, new HashPartitioner(4))
      .setSerializer(new JavaSerializer(conf))
    val shuffleDep = shuffledRdd.dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]]
    assert(!SortShuffleManager.canUseSerializedShuffle(shuffleDep))
    ensureFilesAreCleanedUp(shuffledRdd)
  }

  private def ensureFilesAreCleanedUp(shuffledRdd: ShuffledRDD[_, _, _]): Unit = {
    def getAllFiles: Set[File] =
      FileUtils.listFiles(tempDir, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE).asScala.toSet
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
  }
}
