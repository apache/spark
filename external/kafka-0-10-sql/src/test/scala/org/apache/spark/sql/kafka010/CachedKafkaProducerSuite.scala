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
import java.util.concurrent.{Executors, TimeUnit}

import scala.collection.mutable
import scala.util.Random

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer

import org.apache.spark.{SparkConf, SparkException, TestUtils}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils


class CachedKafkaProducerSuite extends SharedSQLContext with KafkaTest {

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    CachedKafkaProducer.clear()
  }

  test("Should return the cached instance on calling acquire with same params.") {
    val kafkaParams = generateKafkaParams
    val producer = CachedKafkaProducer.acquire(kafkaParams)
    val producer2 = CachedKafkaProducer.acquire(kafkaParams)
    assert(producer.kafkaProducer == producer2.kafkaProducer)
    assert(producer.getInUseCount == 2)
    val map = CachedKafkaProducer.getAsMap
    assert(map.size == 1)
  }

  test("Should return the new instance on calling acquire with different params.") {
    val kafkaParams = generateKafkaParams
    val producer = CachedKafkaProducer.acquire(kafkaParams)
    kafkaParams.remove("acks") // mutate the kafka params.
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
    // Kafka first tries to fetch metadata and reports failures as, " not present in metadata after
    // max.block.ms time."
    TestUtils.assertExceptionMsg(ex, "org.apache.kafka.common.errors.TimeoutException")
    // Since failing kafka producer is released on error and also invalidated, it should not be in
    // cache.
    val map = CachedKafkaProducer.getAsMap
    assert(map.size == 0)
  }

  test("Should not close a producer in-use.") {
    val kafkaParams = generateKafkaParams
    val producer: CachedKafkaProducer = CachedKafkaProducer.acquire(kafkaParams)
    producer.kafkaProducer // initializing the producer.
    assert(producer.getInUseCount == 1)
    // Explicitly cause the producer from guava cache to be evicted.
    CachedKafkaProducer.evict(producer.getKafkaParams)
    assert(producer.getInUseCount == 1)
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

class CachedKafkaProducerStressSuite extends KafkaContinuousTest with KafkaTest {

  override val brokerProps = Map("auto.create.topics.enable" -> "false")

  override def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.kafka.producer.cache.timeout", "2ms")
  }

  test("concurrent use of CachedKafkaProducer") {
    val topic = "topic" + Random.nextInt()
    testUtils.createTopic(topic, 1)
    val kafkaParams: Map[String, Object] = Map("bootstrap.servers" -> testUtils.brokerAddress,
      "key.serializer" -> classOf[ByteArraySerializer].getName,
      "value.serializer" -> classOf[ByteArraySerializer].getName)

    import scala.collection.JavaConverters._

    val numThreads = 100
    val numConcurrentProducers = 500

    val kafkaParamsUniqueMap = mutable.HashMap.empty[Int, ju.Map[String, Object]]
    (1 to numConcurrentProducers).map {
      i => kafkaParamsUniqueMap.put(i, kafkaParams.updated("retries", s"$i").asJava)
    }

    def acquire(i: Int): CachedKafkaProducer = {
      val producer = CachedKafkaProducer.acquire(kafkaParamsUniqueMap(i))
      producer.kafkaProducer // materialize producer for the first time.
      assert(!producer.isClosed, "Acquired producer cannot be closed.")
      producer
    }

    def release(producer: CachedKafkaProducer): Unit = {
      if (producer != null) {
        CachedKafkaProducer.release(producer, Random.nextBoolean())
        if (producer.getInUseCount > 0) {
          assert(!producer.isClosed, "Should not close an inuse producer.")
        }
      }
    }
    val data = (1 to 100).map(_.toString)

    val threadPool = Executors.newFixedThreadPool(numThreads)
    try {
      val futuresAcquire = (1 to 10 * numConcurrentProducers).map { i =>
        threadPool.submit(new Runnable {
          override def run(): Unit = {
            val producer = acquire(i % numConcurrentProducers + 1)
            data.foreach { d =>
              val record = new ProducerRecord[Array[Byte], Array[Byte]](topic, 0, null, d.getBytes)
              producer.kafkaProducer.send(record)
            }
            release(producer)
          }
        })
      }
      futuresAcquire.foreach(_.get(1, TimeUnit.MINUTES))
    } finally {
      threadPool.shutdown()
      CachedKafkaProducer.clear()
    }
  }

  /*
   * The following stress suite will cause frequent eviction of kafka producers from
   * the guava cache. Since these producers remain in use, because they are used by
   * multiple tasks, they stay in close queue till they are released finally. This test
   * will cause new tasks to use fresh instance of kafka producers and as a result it
   * simulates a stress situation, where multiple producers are requested from CachedKafkaProducer
   * and at the same time there will be multiple releases. It is supposed to catch a race
   * condition if any, due to multiple threads requesting and releasing producers.
   */
  test("Single source and multiple kafka sink with 2ms cache timeout.") {

    val df = spark.readStream
      .format("rate")
      .option("numPartitions", "100")
      .option("rowsPerSecond", "200")
      .load()
      .selectExpr("CAST(timestamp AS STRING) key", "CAST(value AS STRING) value")

    val checkpointDir = Utils.createTempDir()
    val topic = newTopic()
    testUtils.createTopic(topic, 100)
    var queries: Seq[StreamingQuery] = Seq.empty[StreamingQuery]
    try {
      failAfter(streamingTimeout) {
        queries = for (i <- 1 to 10) yield {
          df.writeStream
            .format("kafka")
            .option("checkpointLocation", checkpointDir.getCanonicalPath + i)
            .option("kafka.bootstrap.servers", testUtils.brokerAddress)
            // to make it create 5 unique producers.
            .option("kafka.max.block.ms", s"100${i % 5}")
            .option("topic", topic)
            .trigger(Trigger.Continuous(500))
            .queryName(s"kafkaStream$i")
            .start()
        }
        Thread.sleep(15000)

        queries.foreach { q =>
          assert(q.exception.isEmpty, "None of the queries should fail.")
        }
      }
    } finally {
      queries.foreach { q =>
        q.stop()
      }
    }
  }
}
