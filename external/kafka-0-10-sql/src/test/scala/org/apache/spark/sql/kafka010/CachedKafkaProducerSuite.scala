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
import org.scalatest.PrivateMethodTester

import org.apache.spark.sql.test.SharedSQLContext

class CachedKafkaProducerSuite extends SharedSQLContext with PrivateMethodTester with KafkaTest {

  type KP = KafkaProducer[Array[Byte], Array[Byte]]

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    CachedKafkaProducer.clear()
  }

  test("Should return the cached instance on calling getOrCreate with same params.") {
    val kafkaParams: ju.HashMap[String, Object] = generateKafkaParams
    val producer = CachedKafkaProducer.getOrCreate(kafkaParams)
    val producer2 = CachedKafkaProducer.getOrCreate(kafkaParams)
    assert(producer.kafkaProducer == producer2.kafkaProducer)
    assert(producer.inUseCount.intValue() == 2)
    val map = CachedKafkaProducer.getAsMap
    assert(map.size == 1)
  }

  test("Should close the correct kafka producer for the given kafkaPrams.") {
    val kafkaParams: ju.HashMap[String, Object] = generateKafkaParams
    val producer: CachedKafkaProducer = CachedKafkaProducer.getOrCreate(kafkaParams)
    kafkaParams.put("acks", "1")
    val producer2: CachedKafkaProducer = CachedKafkaProducer.getOrCreate(kafkaParams)
    // With updated conf, a new producer instance should be created.
    assert(producer.kafkaProducer != producer2.kafkaProducer)

    val map = CachedKafkaProducer.getAsMap
    assert(map.size == 2)
    producer2.inUseCount.decrementAndGet()

    CachedKafkaProducer.close(kafkaParams)
    assert(producer2.isClosed)
    val map2 = CachedKafkaProducer.getAsMap
    assert(map2.size == 1)
    import scala.collection.JavaConverters._
    val (seq: Seq[(String, Object)], _producer: CachedKafkaProducer) = map2.asScala.toArray.apply(0)
    assert(_producer.kafkaProducer == producer.kafkaProducer)
   }

  test("Should not close a producer in-use.") {
    val kafkaParams: ju.HashMap[String, Object] = generateKafkaParams
    val producer: CachedKafkaProducer = CachedKafkaProducer.getOrCreate(kafkaParams)
    assert(producer.inUseCount.intValue() > 0)
    CachedKafkaProducer.close(kafkaParams)
    assert(producer.inUseCount.intValue() > 0)
    assert(!producer.isClosed, "An in-use producer should not be closed.")
  }

 private def generateKafkaParams: ju.HashMap[String, Object] = {
    val kafkaParams = new ju.HashMap[String, Object]()
    kafkaParams.put("acks", "0")
    kafkaParams.put("bootstrap.servers", "127.0.0.1:9022")
    kafkaParams.put("key.serializer", classOf[ByteArraySerializer].getName)
    kafkaParams.put("value.serializer", classOf[ByteArraySerializer].getName)
    kafkaParams
  }
}
