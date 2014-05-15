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

package org.apache.spark.scheduler

import org.apache.spark.{SparkConf, SparkException, SparkContext}
import org.apache.spark.util.{SerializableBuffer, AkkaUtils}
import org.apache.spark.SparkContext._

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, FunSuite}

class CoarseGrainedSchedulerBackendSuite extends FunSuite with
  BeforeAndAfter with BeforeAndAfterAll {

  override def beforeAll {
    System.setProperty("spark.akka.frameSize", "1")
    System.setProperty("spark.default.parallelism", "1")
  }

  override def afterAll {
    System.clearProperty("spark.akka.frameSize")
    System.clearProperty("spark.default.parallelism")
  }

  test("serialized task larger than Akka frame size") {
    val conf = new SparkConf
    val sc = new SparkContext("local-cluster[2 , 1 , 512]", "test", conf)
    val frameSize = AkkaUtils.maxFrameSizeBytes(sc.conf)
    val buffer = new SerializableBuffer(java.nio.ByteBuffer.allocate(2 * frameSize))
    val larger = sc.parallelize(Seq(buffer))
    val thrown = intercept[SparkException] {
      larger.collect()
    }
    assert(thrown.getMessage.contains("Consider using broadcast variables for large values"))
    val smaller = sc.parallelize(1 to 4).collect()
    assert(smaller.size === 4)
  }

}
