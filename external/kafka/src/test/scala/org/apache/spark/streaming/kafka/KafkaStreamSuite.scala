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

package org.apache.spark.streaming.kafka

import java.io.File
import java.net.InetSocketAddress
import java.util
import java.util.{Properties, Random}

import akka.actor.FSM.->
import org.apache.spark.rdd.RDD
import org.slf4j.{LoggerFactory, Logger}

import scala.collection.mutable

import kafka.consumer._
import kafka.message.MessageAndMetadata
import kafka.admin.CreateTopicCommand
import kafka.common.{KafkaException, TopicAndPartition}
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}
import kafka.utils.ZKStringSerializer
import kafka.serializer.{StringDecoder, StringEncoder}
import kafka.server.{KafkaConfig, KafkaServer}

import org.I0Itec.zkclient.ZkClient

import org.apache.zookeeper.server.ZooKeeperServer
import org.apache.zookeeper.server.NIOServerCnxnFactory

import org.apache.spark.streaming.kafka.KafkaWriter._
import org.apache.spark.streaming.{StreamingContext, TestSuiteBase}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

import scala.collection.mutable.ArrayBuffer

class KafkaStreamSuite extends TestSuiteBase {
  import KafkaTestUtils._

  val zkHost = "localhost"
  var zkPort: Int = 0
  val zkConnectionTimeout = 6000
  val zkSessionTimeout = 6000

  protected var brokerPort = 9092
  protected var brokerConf: KafkaConfig = _
  protected var zookeeper: EmbeddedZookeeper = _
  protected var zkClient: ZkClient = _
  protected var server: KafkaServer = _
  protected var producer: Producer[String, String] = _

  override def useManualClock = false

  override def beforeFunction() {
    // Zookeeper server startup
    zookeeper = new EmbeddedZookeeper(s"$zkHost:$zkPort")
    // Get the actual zookeeper binding port
    zkPort = zookeeper.actualPort
    logInfo("==================== 0 ====================")

    zkClient = new ZkClient(s"$zkHost:$zkPort", zkSessionTimeout, zkConnectionTimeout,
      ZKStringSerializer)
    logInfo("==================== 1 ====================")

    // Kafka broker startup
    var bindSuccess: Boolean = false
    while(!bindSuccess) {
      try {
        val brokerProps = getBrokerConfig(brokerPort, s"$zkHost:$zkPort")
        brokerConf = new KafkaConfig(brokerProps)
        server = new KafkaServer(brokerConf)
        logInfo("==================== 2 ====================")
        server.startup()
        logInfo("==================== 3 ====================")
        bindSuccess = true
      } catch {
        case e: KafkaException =>
          if (e.getMessage != null && e.getMessage.contains("Socket server failed to bind to")) {
            brokerPort += 1
          }
        case e: Exception => throw new Exception("Kafka server create failed", e)
      }
    }

    Thread.sleep(2000)
    logInfo("==================== 4 ====================")
    super.beforeFunction()
  }

  override def afterFunction() {
    producer.close()
    server.shutdown()
    brokerConf.logDirs.foreach { f => Utils.deleteRecursively(new File(f)) }

    zkClient.close()
    zookeeper.shutdown()

    super.afterFunction()
  }

  test("Kafka input stream") {
    val ssc = new StreamingContext(master, framework, batchDuration)
    val topic = "topic1"
    val sent = Map("a" -> 5, "b" -> 3, "c" -> 10)
    createTopic(topic)
    produceAndSendMessage(topic, sent)

    val kafkaParams = Map("zookeeper.connect" -> s"$zkHost:$zkPort",
      "group.id" -> s"test-consumer-${random.nextInt(10000)}",
      "auto.offset.reset" -> "smallest")

    val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      Map(topic -> 1),
      StorageLevel.MEMORY_ONLY)
    val result = new mutable.HashMap[String, Long]()
    stream.map { case (k, v) => v }
      .countByValue()
      .foreachRDD { r =>
        val ret = r.collect()
        ret.toMap.foreach { kv =>
          val count = result.getOrElseUpdate(kv._1, 0) + kv._2
          result.put(kv._1, count)
        }
      }
    ssc.start()
    ssc.awaitTermination(3000)

    assert(sent.size === result.size)
    sent.keys.foreach { k => assert(sent(k) === result(k).toInt) }

    ssc.stop()
  }

  test("Test writing back to Kafka") {
    val ssc = new StreamingContext(master, framework, batchDuration)
    val toBe = new mutable.Queue[RDD[String]]()
    var j = 0
    while (j < 9) {
      toBe.enqueue(ssc.sc.makeRDD(Seq(j.toString, (j + 1).toString, (j + 2).toString)))
      j += 3
    }
    val instream = ssc.queueStream(toBe)
    val producerConf = new Properties()
    producerConf.put("serializer.class", "kafka.serializer.DefaultEncoder")
    producerConf.put("key.serializer.class", "kafka.serializer.StringEncoder")
    producerConf.put("metadata.broker.list", s"localhost:$brokerPort")
    producerConf.put("request.required.acks", "1")
    instream.writeToKafka(producerConf,
      (x: String) => new KeyedMessage[String, Array[Byte]]("topic1", null,x.getBytes))
    ssc.start()
    var i = 0
    val expectedResults = (0 to 8).map(_.toString).toSeq
    val actualResults = new ArrayBuffer[String]()
    val kafkaParams = Map("zookeeper.connect" -> s"$zkHost:$zkPort",
      "group.id" -> s"test-consumer-${random.nextInt(10000)}",
      "auto.offset.reset" -> "smallest", "topic" -> "topic1")
    val consumer = new KafkaConsumer(kafkaParams)
    consumer.initTopicList(List("topic1"))
    while (i < 9) {
      val fetchedMsg = new String(consumer.getNextMessage("topic1").message
        .asInstanceOf[Array[Byte]])
      actualResults += fetchedMsg
      i += 1
    }
    val actualResultSorted = actualResults.sorted
    assert(expectedResults.toSeq === actualResultSorted.toSeq)
    ssc.stop()
  }

  private def createTestMessage(topic: String, sent: Map[String, Int])
    : Seq[KeyedMessage[String, String]] = {
    val messages = for ((s, freq) <- sent; i <- 0 until freq) yield {
      new KeyedMessage[String, String](topic, s)
    }
    messages.toSeq
  }

  def createTopic(topic: String) {
    CreateTopicCommand.createTopic(zkClient, topic, 1, 1, "0")
    logInfo("==================== 5 ====================")
    // wait until metadata is propagated
    waitUntilMetadataIsPropagated(Seq(server), topic, 0, 1000)
  }

  def produceAndSendMessage(topic: String, sent: Map[String, Int]) {
    val brokerAddr = brokerConf.hostName + ":" + brokerConf.port
    producer = new Producer[String, String](new ProducerConfig(getProducerConfig(brokerAddr)))
    producer.send(createTestMessage(topic, sent): _*)
    logInfo("==================== 6 ====================")
  }

}

object KafkaTestUtils {
  val random = new Random()

  def getBrokerConfig(port: Int, zkConnect: String): Properties = {
    val props = new Properties()
    props.put("broker.id", "0")
    props.put("host.name", "localhost")
    props.put("port", port.toString)
    props.put("log.dir", Utils.createTempDir().getAbsolutePath)
    props.put("zookeeper.connect", zkConnect)
    props.put("log.flush.interval.messages", "1")
    props.put("replica.socket.timeout.ms", "1500")
    props
  }

  def getProducerConfig(brokerList: String): Properties = {
    val props = new Properties()
    props.put("metadata.broker.list", brokerList)
    props.put("serializer.class", classOf[StringEncoder].getName)
    props
  }

  def waitUntilTrue(condition: () => Boolean, waitTime: Long): Boolean = {
    val startTime = System.currentTimeMillis()
    while (true) {
      if (condition())
        return true
      if (System.currentTimeMillis() > startTime + waitTime)
        return false
      Thread.sleep(waitTime.min(100L))
    }
    // Should never go to here
    throw new RuntimeException("unexpected error")
  }

  def waitUntilMetadataIsPropagated(servers: Seq[KafkaServer], topic: String, partition: Int,
      timeout: Long) {
    assert(waitUntilTrue(() =>
      servers.foldLeft(true)(_ && _.apis.leaderCache.keySet.contains(
        TopicAndPartition(topic, partition))), timeout),
      s"Partition [$topic, $partition] metadata not propagated after timeout")
  }

  class EmbeddedZookeeper(val zkConnect: String) {
    val random = new Random()
    val snapshotDir = Utils.createTempDir()
    val logDir = Utils.createTempDir()

    val zookeeper = new ZooKeeperServer(snapshotDir, logDir, 500)
    val (ip, port) = {
      val splits = zkConnect.split(":")
      (splits(0), splits(1).toInt)
    }
    val factory = new NIOServerCnxnFactory()
    factory.configure(new InetSocketAddress(ip, port), 16)
    factory.startup(zookeeper)

    val actualPort = factory.getLocalPort

    def shutdown() {
      factory.shutdown()
      Utils.deleteRecursively(snapshotDir)
      Utils.deleteRecursively(logDir)
    }
  }

  class KafkaConsumer(config: Map[String, String]) {
    val props = new Properties()
    for ((k,v) <- config) {
      props.put(k, v)
    }
    val consumer: ConsumerConnector = kafka.consumer.Consumer.create(new ConsumerConfig(props))
    private var consumerMap: scala.collection.Map[String, List[KafkaStream[Array[Byte],
      Array[Byte]]]] = null

    private final val logger: Logger = LoggerFactory.getLogger(classOf[KafkaConsumer])

    def initTopicList(topics: List[String]) {
      val topicCountMap = new mutable.HashMap[String, Int]
      for (topic <- topics) {
        topicCountMap(topic) = 1
      }
      consumerMap = consumer.createMessageStreams(topicCountMap.asInstanceOf[collection
      .Map[String, Int]])
    }

    def getNextMessage(topic: String): MessageAndMetadata[_, _] = {
      val streams: scala.List[KafkaStream[Array[Byte], Array[Byte]]] = consumerMap(topic)
      val stream: KafkaStream[Array[Byte], Array[Byte]] = streams(0)
      val it: ConsumerIterator[Array[Byte], Array[Byte]] = stream.iterator()
      try {
        if (it.hasNext()) {
          it.next()
        }
        else {
          null
        }
      }
      catch {
        case e: ConsumerTimeoutException => {
          logger.error("0 messages available to fetch for the topic " + topic)
          null
        }
      }
    }

    def shutdown(): Unit = {
      consumer.shutdown()
    }
  }
}
