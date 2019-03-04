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
import java.util.concurrent.ConcurrentMap

import scala.collection.JavaConverters._

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.scalatest.PrivateMethodTester

import org.apache.spark.{TaskContext, TaskContextImpl}
import org.apache.spark.sql.test.SharedSQLContext

class CachedKafkaProducerSuite extends SharedSQLContext with PrivateMethodTester with KafkaTest {

  type KP = KafkaProducer[Array[Byte], Array[Byte]]

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    CachedKafkaProducer.clear()
  }

  test("Should not throw exception on calling close with non-existing key.") {
    val kafkaParams = getKafkaParams()
    CachedKafkaProducer.close(kafkaParams)
    assert(getCacheMap().size === 0)
  }

  test("Should return the cached instance on calling getOrCreate with same params.") {
    val kafkaParams = getKafkaParams()
    val producer = CachedKafkaProducer.getOrCreate(kafkaParams)
    val producer2 = CachedKafkaProducer.getOrCreate(kafkaParams)
    assert(producer === producer2)
    val cacheMap = getCacheMap()
    assert(cacheMap.size === 1)
  }

  test("Should close the correct kafka producer for the given kafkaPrams.") {
    val kafkaParams = getKafkaParams()
    val producer: KP = CachedKafkaProducer.getOrCreate(kafkaParams)
    kafkaParams.put("acks", "1")
    val producer2: KP = CachedKafkaProducer.getOrCreate(kafkaParams)
    // With updated conf, a new producer instance should be created.
    assert(producer != producer2)
    val cacheMap = getCacheMap()
    assert(cacheMap.size === 2)

    CachedKafkaProducer.close(kafkaParams)
    val cacheMap2 = getCacheMap()
    assert(cacheMap2.size === 1)
    assert(getCacheMapItem(cacheMap2, 0) === producer)
  }

  test("Should return new instance on calling getOrCreate with same params but task retry.") {
    val kafkaParams = getKafkaParams()
    val taskContext = new TaskContextImpl(0, 0, 0, 0, attemptNumber = 0, null, null, null)
    TaskContext.setTaskContext(taskContext)
    val producer = CachedKafkaProducer.getOrCreate(kafkaParams)
    val retryTaskContext = new TaskContextImpl(0, 0, 0, 0, attemptNumber = 1, null, null, null)
    TaskContext.setTaskContext(retryTaskContext)
    val producer2 = CachedKafkaProducer.getOrCreate(kafkaParams)
    assert(producer != producer2)
    val cacheMap = getCacheMap()
    assert(cacheMap.size === 1)
    assert(getCacheMapItem(cacheMap, 0) === producer2)
  }

  private def getKafkaParams(): ju.HashMap[String, Object] = {
    val kafkaParams = new ju.HashMap[String, Object]()
    kafkaParams.put("acks", "0")
    // Here only host should be resolvable, it does not need a running instance of kafka server.
    kafkaParams.put("bootstrap.servers", "127.0.0.1:9022")
    kafkaParams.put("key.serializer", classOf[ByteArraySerializer].getName)
    kafkaParams.put("value.serializer", classOf[ByteArraySerializer].getName)
    kafkaParams
  }

  private def getCacheMap(): ConcurrentMap[Seq[(String, Object)], KP] = {
    val getAsMap = PrivateMethod[ConcurrentMap[Seq[(String, Object)], KP]]('getAsMap)
    CachedKafkaProducer.invokePrivate(getAsMap())
  }

  private def getCacheMapItem(map: ConcurrentMap[Seq[(String, Object)], KP], offset: Int): KP = {
    val (_: Seq[(String, Object)], _producer: KP) = map.asScala.toArray.apply(0)
    _producer
  }
}
