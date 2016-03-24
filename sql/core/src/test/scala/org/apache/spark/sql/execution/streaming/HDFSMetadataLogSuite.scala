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

package org.apache.spark.sql.execution.streaming

import java.net.URI
import java.util.ConcurrentModificationException

import scala.util.Random

import org.apache.hadoop.fs._
import org.scalatest.concurrent.AsyncAssertions._
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.execution.streaming.FakeFileSystem._
import org.apache.spark.sql.test.SharedSQLContext

class HDFSMetadataLogSuite extends SparkFunSuite with SharedSQLContext {


  override protected val sparkConf =
    new SparkConf()
      .setMaster("local")
      .setAppName(this.getClass.getSimpleName)
      .set(s"spark.hadoop.fs.$scheme.impl.disable.cache", "true") // to avoid caching of FS objects

  private implicit def toOption[A](a: A): Option[A] = Option(a)

  test("basic") {
    withTempDir { temp =>
      val metadataLog = new HDFSMetadataLog[String](sqlContext, temp.getAbsolutePath)
      assert(metadataLog.add(0, "batch0"))
      assert(metadataLog.getLatest() === Some(0 -> "batch0"))
      assert(metadataLog.get(0) === Some("batch0"))
      assert(metadataLog.getLatest() === Some(0 -> "batch0"))
      assert(metadataLog.get(None, 0) === Array(0 -> "batch0"))

      assert(metadataLog.add(1, "batch1"))
      assert(metadataLog.get(0) === Some("batch0"))
      assert(metadataLog.get(1) === Some("batch1"))
      assert(metadataLog.getLatest() === Some(1 -> "batch1"))
      assert(metadataLog.get(None, 1) === Array(0 -> "batch0", 1 -> "batch1"))

      // Adding the same batch does nothing
      metadataLog.add(1, "batch1-duplicated")
      assert(metadataLog.get(0) === Some("batch0"))
      assert(metadataLog.get(1) === Some("batch1"))
      assert(metadataLog.getLatest() === Some(1 -> "batch1"))
      assert(metadataLog.get(None, 1) === Array(0 -> "batch0", 1 -> "batch1"))
    }
  }

  test("restart") {
    withTempDir { temp =>
      val metadataLog = new HDFSMetadataLog[String](sqlContext, temp.getAbsolutePath)
      assert(metadataLog.add(0, "batch0"))
      assert(metadataLog.add(1, "batch1"))
      assert(metadataLog.get(0) === Some("batch0"))
      assert(metadataLog.get(1) === Some("batch1"))
      assert(metadataLog.getLatest() === Some(1 -> "batch1"))
      assert(metadataLog.get(None, 1) === Array(0 -> "batch0", 1 -> "batch1"))

      val metadataLog2 = new HDFSMetadataLog[String](sqlContext, temp.getAbsolutePath)
      assert(metadataLog2.get(0) === Some("batch0"))
      assert(metadataLog2.get(1) === Some("batch1"))
      assert(metadataLog2.getLatest() === Some(1 -> "batch1"))
      assert(metadataLog2.get(None, 1) === Array(0 -> "batch0", 1 -> "batch1"))
    }
  }

  test("metadata directory collision") {
    withTempDir { temp =>
      val waiter = new Waiter
      val maxBatchId = 100
      for (id <- 0 until 10) {
        new Thread() {
          override def run(): Unit = waiter {
            val metadataLog = new HDFSMetadataLog[String](sqlContext, temp.getAbsolutePath)
            try {
              var nextBatchId = metadataLog.getLatest().map(_._1).getOrElse(-1L)
              nextBatchId += 1
              while (nextBatchId <= maxBatchId) {
                metadataLog.add(nextBatchId, nextBatchId.toString)
                nextBatchId += 1
              }
            } catch {
              case e: ConcurrentModificationException =>
              // This is expected since there are multiple writers
            } finally {
              waiter.dismiss()
            }
          }
        }.start()
      }

      waiter.await(timeout(10.seconds), dismissals(10))
      val metadataLog = new HDFSMetadataLog[String](sqlContext, temp.getAbsolutePath)
      assert(metadataLog.getLatest() === Some(maxBatchId -> maxBatchId.toString))
      assert(metadataLog.get(None, maxBatchId) === (0 to maxBatchId).map(i => (i, i.toString)))
    }
  }

  testQuietly("fallback from FileContext to FileSystem") {
    sqlContext.sparkContext.hadoopConfiguration.set(
      s"fs.$scheme.impl",
      classOf[FakeFileSystem].getName)
    withTempDir { temp =>
      val metadataLog = new HDFSMetadataLog[String](sqlContext, s"$scheme://$temp")

      assert(metadataLog.add(0, "batch0"))
      assert(metadataLog.getLatest() === Some(0 -> "batch0"))
      assert(metadataLog.get(0) === Some("batch0"))
      assert(metadataLog.getLatest() === Some(0 -> "batch0"))
      assert(metadataLog.get(None, 0) === Array(0 -> "batch0"))

      assert(metadataLog.add(1, "batch1"))
      assert(metadataLog.get(0) === Some("batch0"))
      assert(metadataLog.get(1) === Some("batch1"))
      assert(metadataLog.getLatest() === Some(1 -> "batch1"))
      assert(metadataLog.get(None, 1) === Array(0 -> "batch0", 1 -> "batch1"))

      // Adding the same batch does nothing
      metadataLog.add(1, "batch1-duplicated")
      assert(metadataLog.get(0) === Some("batch0"))
      assert(metadataLog.get(1) === Some("batch1"))
      assert(metadataLog.getLatest() === Some(1 -> "batch1"))
      assert(metadataLog.get(None, 1) === Array(0 -> "batch0", 1 -> "batch1"))

      // Restarting recovers the same data
      val metadataLog2 = new HDFSMetadataLog[String](sqlContext, temp.getAbsolutePath)
      assert(metadataLog2.get(0) === Some("batch0"))
      assert(metadataLog2.get(1) === Some("batch1"))
      assert(metadataLog2.getLatest() === Some(1 -> "batch1"))
      assert(metadataLog2.get(None, 1) === Array(0 -> "batch0", 1 -> "batch1"))
    }
  }
}

/** FakeFileSystem to test fallback of the HDFSMetadataLog from FileContext to FileSystem API */
class FakeFileSystem extends RawLocalFileSystem {
  override def getUri: URI = {
    URI.create(s"$scheme:///")
  }
}

object FakeFileSystem {
  val scheme = s"HDFSMetadataLogSuite${math.abs(Random.nextInt)}"
}
