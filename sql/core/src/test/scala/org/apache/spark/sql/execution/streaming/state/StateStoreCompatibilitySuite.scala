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

package org.apache.spark.sql.execution.streaming.state

import java.io.File

import org.apache.commons.io.FileUtils

import org.apache.spark.SparkFunSuite
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.catalyst.plans.PlanTestBase
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes.Update
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.util.Utils

class StateStoreCompatibilitySuite extends StreamTest with StateStoreCodecsTest {
   testWithAllCodec(
      "SPARK-33263: Recovery from checkpoint before codec config introduced") {
     val resourceUri = this.getClass.getResource(
       "/structured-streaming/checkpoint-version-3.0.0-streaming-statestore-codec/").toURI
     val checkpointDir = Utils.createTempDir().getCanonicalFile
     FileUtils.copyDirectory(new File(resourceUri), checkpointDir)

     import testImplicits._

     val inputData = MemoryStream[Int]
     val aggregated = inputData.toDF().groupBy("value").agg(count("*"))
     inputData.addData(1, 2, 3)

     /**
      * Note: The checkpoint was generated using the following input in Spark version 3.0.0:
      * AddData(inputData, 1, 2, 3)
      */

     testStream(aggregated, Update)(
       StartStream(
         checkpointLocation = checkpointDir.getAbsolutePath,
         additionalConfs = Map(SQLConf.SHUFFLE_PARTITIONS.key -> "1")),
       AddData(inputData, 1, 2),
       CheckNewAnswer((1, 2), (2, 2))
     )
   }
}

trait StateStoreCodecsTest extends SparkFunSuite with PlanTestBase {
  private val codecsInShortName =
    CompressionCodec.ALL_COMPRESSION_CODECS.map { c => CompressionCodec.getShortName(c) }

  protected def testWithAllCodec(name: String)(func: => Any): Unit = {
    codecsInShortName.foreach { codecShortName =>
      test(s"$name - with codec $codecShortName") {
        withSQLConf(SQLConf.STATE_STORE_COMPRESSION_CODEC.key -> codecShortName) {
          func
        }
      }
    }

    CompressionCodec.ALL_COMPRESSION_CODECS.foreach { codecShortName =>
      test(s"$name - with codec $codecShortName") {
        withSQLConf(SQLConf.STATE_STORE_COMPRESSION_CODEC.key -> codecShortName) {
          func
        }
      }
    }
  }
}
