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

import java.io.File

import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.TrueFileFilter
import org.scalatest.BeforeAndAfterEach

import org.apache.spark._
import org.apache.spark.util.Utils


class RDDCleanerSuite extends SparkFunSuite with BeforeAndAfterEach {
  override def beforeEach(): Unit = {
    super.beforeEach()
    // Once `Utils.getOrCreateLocalRootDirs` is called, it is cached in `Utils.localRootDirs`.
    // Unless this is manually cleared before and after a test, it returns the same directory
    // set before even if 'spark.local.dir' is configured afterwards.
    Utils.clearLocalRootDirs()
  }

  override def afterEach(): Unit = {
    Utils.clearLocalRootDirs()
    super.afterEach()
  }

  test("RDD shuffle cleanup standalone") {
    val conf = new SparkConf()
    val localDir = Utils.createTempDir()
    val checkpointDir = Utils.createTempDir()
    def getAllFiles: Set[File] =
      FileUtils.listFiles(localDir, TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE).asScala.toSet
    try {
      conf.set("spark.local.dir", localDir.getAbsolutePath)
      val sc = new SparkContext("local[2]", "test", conf)
      try {
        sc.setCheckpointDir(checkpointDir.getAbsolutePath)
        // Test checkpoint and clean parents
        val input = sc.parallelize(1 to 1000)
        val keyed = input.map(x => (x % 20, 1))
        val shuffled = keyed.reduceByKey(_ + _)
        val keysOnly = shuffled.keys
        keysOnly.count()
        assert(getAllFiles.size > 0)
        keysOnly.cleanShuffleDependencies(true)
        val resultingFiles = getAllFiles
        assert(resultingFiles === Set())
        // Ensure running count again works fine even if we kill the shuffle files.
        assert(keysOnly.count() === 20)
      } finally {
        sc.stop()
      }
    } finally {
      Utils.deleteRecursively(localDir)
      Utils.deleteRecursively(checkpointDir)
    }
  }
}
