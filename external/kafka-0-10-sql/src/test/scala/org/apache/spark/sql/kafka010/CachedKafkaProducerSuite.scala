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

package org.apache.spark.sql.kafka010

import java.{util => ju}

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.ByteArraySerializer

import org.apache.spark.SparkException
import org.apache.spark.sql.test.SharedSQLContext

class CachedKafkaProducerSuite extends SharedSQLContext with KafkaTest {

  type KP = KafkaProducer[Array[Byte], Array[Byte]]

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    CachedKafkaProducer.clear()
  }

  test("Should return the cached instance on calling acquire with same params.") {
    val kafkaParams: ju.HashMap[String, Object] = generateKafkaParams
    val producer = CachedKafkaProducer.acquire(kafkaParams)
    val producer2 = CachedKafkaProducer.acquire(kafkaParams)
    assert(producer.kafkaProducer == producer2.kafkaProducer)
    assert(producer.getInUseCount == 2)
    val map = CachedKafkaProducer.getAsMap
    assert(map.size == 1)
  }

  test("Should return the new instance on calling acquire with different params.") {
    val kafkaParams: ju.HashMap[String, Object] = generateKafkaParams
    val producer = CachedKafkaProducer.acquire(kafkaParams)
    kafkaParams.remove("ack") // mutate the kafka params.
    val producer2 = CachedKafkaProducer.acquire(kafkaParams)
    assert(producer.kafkaProducer != producer2.kafkaProducer)
    assert(producer.getInUseCount == 1)
    assert(producer2.getInUseCount == 1)
    val map = CachedKafkaProducer.getAsMap
    assert(map.size == 2)
  }

  test("Automatically remove a failing kafka producer from cache.") {
    import testImplicits._
    val df = Seq[(String, String)](null.asInstanceOf[String] -> "1").toDF("topic", "value")
    val ex = intercept[SparkException] {
      // This will fail because the service is not reachable.
      df.write
        .format("kafka")
        .option("topic", "topic")
        .option("kafka.retries", "1")
        .option("kafka.max.block.ms", "2")
        .option("kafka.bootstrap.servers", "12.0.0.1:39022")
        .save()
    }
    assert(ex.getMessage.contains("org.apache.kafka.common.errors.TimeoutException"),
      "Spark command should fail due to service not reachable.")
    // Since failing kafka producer is released on error and also invalidated, it should not be in
    // cache.
    val map = CachedKafkaProducer.getAsMap
    assert(map.size == 0)
  }

  test("Should not close a producer in-use.") {
    val kafkaParams: ju.HashMap[String, Object] = generateKafkaParams
    val producer: CachedKafkaProducer = CachedKafkaProducer.acquire(kafkaParams)
    producer.kafkaProducer // initializing the producer.
    assert(producer.getInUseCount.intValue() == 1)
    // Explicitly cause the producer from guava cache to be evicted.
    CachedKafkaProducer.evict(producer.getKafkaParams)
    assert(producer.getInUseCount.intValue() == 1)
    assert(!producer.isClosed, "An in-use producer should not be closed.")
  }

 private def generateKafkaParams: ju.HashMap[String, Object] = {
    val kafkaParams = new ju.HashMap[String, Object]()
    kafkaParams.put("bootstrap.servers", "127.0.0.1:9022")
    kafkaParams.put("ack", "1")
    kafkaParams.put("key.serializer", classOf[ByteArraySerializer].getName)
    kafkaParams.put("value.serializer", classOf[ByteArraySerializer].getName)
    kafkaParams
  }
}
