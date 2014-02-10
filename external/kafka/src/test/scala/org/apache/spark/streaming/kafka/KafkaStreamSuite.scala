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
import java.util.{Properties, Random}

import scala.collection.mutable

import kafka.admin.CreateTopicCommand
import kafka.common.TopicAndPartition
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}
import kafka.utils.ZKStringSerializer
import kafka.serializer.StringEncoder
import kafka.server.{KafkaConfig, KafkaServer}

import org.apache.spark.streaming.{StreamingContext, TestSuiteBase}
import org.apache.zookeeper.server.ZooKeeperServer
import org.apache.zookeeper.server.NIOServerCnxnFactory

import org.I0Itec.zkclient.ZkClient

class KafkaStreamSuite extends TestSuiteBase {
  val zkConnect = "localhost:2181"
  var zookeeper: EmbeddedZookeeper = _
  var zkClient: ZkClient = _
  val zkConnectionTimeout = 6000
  val zkSessionTimeout = 6000

  val brokerPort = 9092
  val brokerProps = getBrokerConfig(brokerPort)
  val brokerConf = new KafkaConfig(brokerProps)
  var server: KafkaServer = _

  override def beforeFunction() {
    // Zookeeper server startup
    zookeeper = new EmbeddedZookeeper(zkConnect)
    logInfo("==================== 0 ====================")
    zkClient = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    logInfo("==================== 1 ====================")

    // Kafka broker startup
    server = new KafkaServer(brokerConf)
    logInfo("==================== 2 ====================")
    server.startup()
    logInfo("==================== 3 ====================")
    Thread.sleep(2000)
    logInfo("==================== 4 ====================")
    super.beforeFunction()
  }

  override def afterFunction() {
    server.shutdown()
    brokerConf.logDirs.foreach { f => KafkaStreamSuite.deleteDir(new File(f)) }

    zkClient.close()
    zookeeper.shutdown()

    super.afterFunction()
  }

  ignore("kafka input stream") {
    val ssc = new StreamingContext(master, framework, batchDuration)
    val topic = "topic1"
    val sent = Map("a" -> 5, "b" -> 3, "c" -> 10)

    val stream = KafkaUtils.createStream(ssc, zkConnect, "group", Map(topic -> 1))
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
    produceAndSendTestMessage(topic, sent)
    ssc.awaitTermination(10000)

    assert(sent.size === result.size)
    sent.keys.foreach { k => assert(sent(k) === result(k).toInt) }

    ssc.stop()
  }

  private def getBrokerConfig(port: Int): Properties = {
    val props = new Properties()
    props.put("broker.id", "0")
    props.put("host.name", "localhost")
    props.put("port", port.toString)
    props.put("log.dir", KafkaStreamSuite.tmpDir().getAbsolutePath)
    props.put("zookeeper.connect", zkConnect)
    props.put("log.flush.interval.messages", "1")
    props.put("replica.socket.timeout.ms", "1500")
    props
  }

  private def getProducerConfig(brokerList: String): Properties = {
    val props = new Properties()
    props.put("metadata.broker.list", brokerList)
    props.put("serializer.class", classOf[StringEncoder].getName)
    props
  }

  private def createTestMessage(topic: String, sent: Map[String, Int])
    : Seq[KeyedMessage[String, String]] = {
    val messages = for ((s, freq) <- sent; i <- 0 until freq) yield {
      new KeyedMessage[String, String](topic, s)
    }
    messages.toSeq
  }

  def produceAndSendTestMessage(topic: String, sent: Map[String, Int]) {
    val brokerAddr = brokerConf.hostName + ":" + brokerConf.port
    val producer = new Producer[String, String](new ProducerConfig(getProducerConfig(brokerAddr)))
    CreateTopicCommand.createTopic(zkClient, topic, 1, 1, "0")
    logInfo("==================== 5 ====================")
    // wait until metadata is propagated
    Thread.sleep(1000)
    assert(server.apis.leaderCache.keySet.contains(TopicAndPartition(topic, 0)))
    producer.send(createTestMessage(topic, sent): _*)
    Thread.sleep(1000)

    logInfo("==================== 6 ====================")
    producer.close()
  }
}

object KafkaStreamSuite {
  val random = new Random()

  def tmpDir(): File = {
    val tmp = System.getProperty("java.io.tmpdir")
    val f = new File(tmp, "spark-kafka-" + random.nextInt(1000))
    f.mkdirs()
    f
  }

  def deleteDir(file: File) {
    if (file.isFile) {
      file.delete()
    } else {
      for (f <- file.listFiles()) {
        deleteDir(f)
      }
      file.delete()
    }
  }
}

class EmbeddedZookeeper(val zkConnect: String) {
  val random = new Random()
  val snapshotDir = KafkaStreamSuite.tmpDir()
  val logDir = KafkaStreamSuite.tmpDir()

  val zookeeper = new ZooKeeperServer(snapshotDir, logDir, 500)
  val(ip, port) = {
    val splits = zkConnect.split(":")
    (splits(0), splits(1).toInt)
  }
  val factory = new NIOServerCnxnFactory()
  factory.configure(new InetSocketAddress(ip, port), 16)
  factory.startup(zookeeper)

  def shutdown() {
    factory.shutdown()
    KafkaStreamSuite.deleteDir(snapshotDir)
    KafkaStreamSuite.deleteDir(logDir)
  }
}
