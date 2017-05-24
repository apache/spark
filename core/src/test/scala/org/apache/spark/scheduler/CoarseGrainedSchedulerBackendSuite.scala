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

import java.io.{IOException, NotSerializableException, ObjectInputStream, ObjectOutputStream}

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{RpcUtils, SerializableBuffer}

class NotSerializablePartitionRDD(
    sc: SparkContext,
    numPartitions: Int) extends RDD[(Int, Int)](sc, Nil) with Serializable {

  override def compute(split: Partition, context: TaskContext): Iterator[(Int, Int)] =
    throw new RuntimeException("should not be reached")

  override def getPartitions: Array[Partition] = (0 until numPartitions).map(i => new Partition {
    override def index: Int = i

    @throws(classOf[IOException])
    private def writeObject(out: ObjectOutputStream): Unit = {
      throw new NotSerializableException()
    }

    @throws(classOf[IOException])
    private def readObject(in: ObjectInputStream): Unit = {}
  }).toArray

  override def getPreferredLocations(partition: Partition): Seq[String] = Nil

  override def toString: String = "DAGSchedulerSuiteRDD " + id
}

class CoarseGrainedSchedulerBackendSuite extends SparkFunSuite with LocalSparkContext {
  test("serialized task larger than max RPC message size") {
    val conf = new SparkConf
    conf.set("spark.rpc.message.maxSize", "1")
    conf.set("spark.default.parallelism", "1")
    sc = new SparkContext("local-cluster[2, 1, 1024]", "test", conf)
    val frameSize = RpcUtils.maxMessageSizeBytes(sc.conf)
    val buffer = new SerializableBuffer(java.nio.ByteBuffer.allocate(2 * frameSize))
    val larger = sc.parallelize(Seq(buffer))
    val thrown = intercept[SparkException] {
      larger.collect()
    }
    assert(thrown.getMessage.contains("using broadcast variables for large values"))
    val smaller = sc.parallelize(1 to 4).collect()
    assert(smaller.size === 4)
  }

  test("Scheduler aborts stages that have unserializable partition") {
    val conf = new SparkConf()
      .setMaster("local-cluster[2, 1, 1024]")
      .setAppName("test")
      .set("spark.dynamicAllocation.testing", "true")
    sc = new SparkContext(conf)
    val myRDD = new NotSerializablePartitionRDD(sc, 2)
    val e = intercept[SparkException] {
      myRDD.count()
    }
    assert(e.getMessage.contains("Failed to serialize task"))
    assertResult(10) {
      sc.parallelize(1 to 10).count()
    }
  }
}
