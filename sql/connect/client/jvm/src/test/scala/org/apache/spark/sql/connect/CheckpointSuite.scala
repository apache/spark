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
package org.apache.spark.sql.connect

import java.io.{ByteArrayOutputStream, PrintStream}

import scala.concurrent.duration.DurationInt

import org.apache.commons.io.output.TeeOutputStream
import org.scalatest.concurrent.Eventually.{eventually, interval, timeout}
import org.scalatest.exceptions.TestFailedDueToTimeoutException

import org.apache.spark.SparkException
import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.test.{ConnectFunSuite, RemoteSparkSession, SQLHelper}
import org.apache.spark.storage.StorageLevel

class CheckpointSuite extends ConnectFunSuite with RemoteSparkSession with SQLHelper {

  private def captureStdOut(block: => Unit): String = {
    val currentOut = Console.out
    val capturedOut = new ByteArrayOutputStream()
    val newOut = new PrintStream(new TeeOutputStream(currentOut, capturedOut))
    Console.withOut(newOut) {
      block
    }
    capturedOut.toString
  }

  private def checkFragments(result: String, fragmentsToCheck: Seq[String]): Unit = {
    fragmentsToCheck.foreach { fragment =>
      assert(result.contains(fragment))
    }
  }

  private def testCapturedStdOut(block: => Unit, fragmentsToCheck: String*): Unit = {
    checkFragments(captureStdOut(block), fragmentsToCheck)
  }

  test("localCheckpoint") {
    val df = spark.range(100).localCheckpoint()
    testCapturedStdOut(df.explain(), "ExistingRDD")
  }

  test("localCheckpoint with StorageLevel") {
    // We don't have a way to reach into the server and assert the storage level server side, but
    // this test should cover for unexpected errors in the API.
    val df =
      spark.range(100).localCheckpoint(eager = true, storageLevel = StorageLevel.DISK_ONLY)
    df.collect()
  }

  test("localCheckpoint gc") {
    val df = spark.range(100).localCheckpoint(eager = true)
    val encoder = df.agnosticEncoder
    val dfId = df.plan.getRoot.getCachedRemoteRelation.getRelationId
    spark.cleaner.doCleanupCachedRemoteRelation(dfId)

    val ex = intercept[SparkException] {
      spark
        .newDataset(encoder) { builder =>
          builder.setCachedRemoteRelation(
            proto.CachedRemoteRelation
              .newBuilder()
              .setRelationId(dfId)
              .build())
        }
        .collect()
    }
    assert(ex.getMessage.contains(s"No DataFrame with id $dfId is found"))
  }

  // This test is flaky because cannot guarantee GC
  // You can locally run this to verify the behavior.
  ignore("localCheckpoint gc derived DataFrame") {
    var df1 = spark.range(100).localCheckpoint(eager = true)
    var derived = df1.repartition(10)
    val encoder = df1.agnosticEncoder
    val dfId = df1.plan.getRoot.getCachedRemoteRelation.getRelationId

    df1 = null
    System.gc()
    Thread.sleep(3000L)

    def condition(): Unit = {
      val ex = intercept[SparkException] {
        spark
          .newDataset(encoder) { builder =>
            builder.setCachedRemoteRelation(
              proto.CachedRemoteRelation
                .newBuilder()
                .setRelationId(dfId)
                .build())
          }
          .collect()
      }
      assert(ex.getMessage.contains(s"No DataFrame with id $dfId is found"))
    }

    intercept[TestFailedDueToTimeoutException] {
      eventually(timeout(5.seconds), interval(1.second))(condition())
    }

    // GC triggers remove the cached remote relation
    derived = null
    System.gc()
    Thread.sleep(3000L)

    // Check the state was removed up on garbage-collection.
    eventually(timeout(60.seconds), interval(1.second))(condition())
  }
}
