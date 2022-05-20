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

import java.io.{File, IOException}
import java.net.{InetAddress, InetSocketAddress}
import java.nio.charset.StandardCharsets
import java.util.{Collections, Properties, UUID}
import java.util.concurrent.TimeUnit
import javax.security.auth.login.Configuration

import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.control.NonFatal

import com.google.common.io.Files
import kafka.api.Request
import kafka.server.{HostedPartition, KafkaConfig, KafkaServer}
import kafka.server.checkpoints.OffsetCheckpointFile
import kafka.zk.KafkaZkClient
import org.apache.hadoop.minikdc.MiniKdc
import org.apache.hadoop.security.UserGroupInformation
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol.{PLAINTEXT, SASL_PLAINTEXT}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.utils.SystemTime
import org.apache.zookeeper.client.ZKClientConfig
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}
import org.apache.zookeeper.server.auth.SASLAuthenticationProvider
import org.scalatest.Assertions._
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.kafka010.KafkaTokenUtil
import org.apache.spark.util.{SecurityUtils, ShutdownHookManager, Utils}

/**
 * This is a helper class for Kafka test suites. This has the functionality to set up
 * and tear down local Kafka servers, and to push data using Kafka producers.
 *
 * The reason to put Kafka test utility class in src is to test Python related Kafka APIs.
 */
class KafkaTestUtils(
    withBrokerProps: Map[String, Object] = Map.empty,
    secure: Boolean = false) extends Logging {

  private val JAVA_AUTH_CONFIG = "java.security.auth.login.config"

  private val localCanonicalHostName = InetAddress.getLoopbackAddress().getCanonicalHostName()
  logInfo(s"Local host name is $localCanonicalHostName")

  private var kdc: MiniKdc = _

  // Zookeeper related configurations
  private val zkHost = localCanonicalHostName
  private var zkPort: Int = 0
  private val zkConnectionTimeout = 60000
  private val zkSessionTimeout = 10000

  private var zookeeper: EmbeddedZookeeper = _
  private var zkClient: KafkaZkClient = _

  // Kafka broker related configurations
  private val brokerHost = localCanonicalHostName
  private var brokerPort = 0
  private var brokerConf: KafkaConfig = _

  private val brokerServiceName = "kafka"
  private val clientUser = s"client/$localCanonicalHostName"
  private var clientKeytabFile: File = _

  // Kafka broker server
  private var server: KafkaServer = _
  private var adminClient: AdminClient = _

  // Kafka producer
  private var producer: Producer[String, String] = _

  // Flag to test whether the system is correctly started
  private var kdcReady = false
  private var zkReady = false
  private var brokerReady = false
  private var leakDetector: AnyRef = null

  def zkAddress: String = {
    assert(zkReady, "Zookeeper not setup yet or already torn down, cannot get zookeeper address")
    s"$zkHost:$zkPort"
  }

  def brokerAddress: String = {
    assert(brokerReady, "Kafka not setup yet or already torn down, cannot get broker address")
    s"$brokerHost:$brokerPort"
  }

  def zookeeperClient: KafkaZkClient = {
    assert(zkReady, "Zookeeper not setup yet or already torn down, cannot get zookeeper client")
    Option(zkClient).getOrElse(
      throw new IllegalStateException("Zookeeper client is not yet initialized"))
  }

  def clientPrincipal: String = {
    assert(kdcReady, "KDC should be set up beforehand")
    clientUser + "@" + kdc.getRealm()
  }

  def clientKeytab: String = {
    assert(kdcReady, "KDC should be set up beforehand")
    clientKeytabFile.getAbsolutePath()
  }

  private def setUpMiniKdc(): Unit = {
    val kdcDir = Utils.createTempDir()
    val kdcConf = MiniKdc.createConf()
    kdcConf.setProperty(MiniKdc.DEBUG, "true")
    // The port for MiniKdc service gets selected in the constructor, but will be bound
    // to it later in MiniKdc.start() -> MiniKdc.initKDCServer() -> KdcServer.start().
    // In meantime, when some other service might capture the port during this progress, and
    // cause BindException.
    // This makes our tests which have dedicated JVMs and rely on MiniKDC being flaky
    //
    // https://issues.apache.org/jira/browse/HADOOP-12656 get fixed in Hadoop 2.8.0.
    //
    // The workaround here is to periodically repeat this process with a timeout , since we are
    // using Hadoop 2.7.4 as default.
    // https://issues.apache.org/jira/browse/SPARK-31631
    eventually(timeout(60.seconds), interval(1.second)) {
      try {
        kdc = new MiniKdc(kdcConf, kdcDir)
        kdc.start()
      } catch {
        case NonFatal(e) =>
          if (kdc != null) {
            kdc.stop()
            kdc = null
          }
          throw e
      }
    }
    // TODO https://issues.apache.org/jira/browse/SPARK-30037
    // Need to build spark's own MiniKDC and customize krb5.conf like Kafka
    rewriteKrb5Conf()
    kdcReady = true
  }

  /**
   * In this method we rewrite krb5.conf to make kdc and client use the same enctypes
   */
  private def rewriteKrb5Conf(): Unit = {
    val krb5Conf = Utils
      .tryWithResource(Source.fromFile(kdc.getKrb5conf, "UTF-8"))(_.getLines().toList)
    var rewritten = false
    val addedConfig =
      addedKrb5Config("default_tkt_enctypes", "aes128-cts-hmac-sha1-96") +
        addedKrb5Config("default_tgs_enctypes", "aes128-cts-hmac-sha1-96")
    val rewriteKrb5Conf = krb5Conf.map(s =>
      if (s.contains("libdefaults")) {
        rewritten = true
        s + addedConfig
      } else {
        s
      }).filter(!_.trim.startsWith("#")).mkString(System.lineSeparator())

    val krb5confStr = if (!rewritten) {
      "[libdefaults]" + addedConfig + System.lineSeparator() +
        System.lineSeparator() + rewriteKrb5Conf
    } else {
      rewriteKrb5Conf
    }

    kdc.getKrb5conf.delete()
    Files.write(krb5confStr, kdc.getKrb5conf, StandardCharsets.UTF_8)
    logDebug(s"krb5.conf file content: $krb5confStr")
  }

  private def addedKrb5Config(key: String, value: String): String = {
    System.lineSeparator() + s"    $key=$value"
  }

  private def createKeytabsAndJaasConfigFile(): String = {
    assert(kdcReady, "KDC should be set up beforehand")
    val baseDir = Utils.createTempDir()

    val zkServerUser = s"zookeeper/$localCanonicalHostName"
    val zkServerKeytabFile = new File(baseDir, "zookeeper.keytab")
    kdc.createPrincipal(zkServerKeytabFile, zkServerUser)
    logDebug(s"Created keytab file: ${zkServerKeytabFile.getAbsolutePath()}")

    val zkClientUser = s"zkclient/$localCanonicalHostName"
    val zkClientKeytabFile = new File(baseDir, "zkclient.keytab")
    kdc.createPrincipal(zkClientKeytabFile, zkClientUser)
    logDebug(s"Created keytab file: ${zkClientKeytabFile.getAbsolutePath()}")

    val kafkaServerUser = s"kafka/$localCanonicalHostName"
    val kafkaServerKeytabFile = new File(baseDir, "kafka.keytab")
    kdc.createPrincipal(kafkaServerKeytabFile, kafkaServerUser)
    logDebug(s"Created keytab file: ${kafkaServerKeytabFile.getAbsolutePath()}")

    clientKeytabFile = new File(baseDir, "client.keytab")
    kdc.createPrincipal(clientKeytabFile, clientUser)
    logDebug(s"Created keytab file: ${clientKeytabFile.getAbsolutePath()}")

    val file = new File(baseDir, "jaas.conf");
    val realm = kdc.getRealm()
    val content =
      s"""
      |Server {
      |  ${SecurityUtils.getKrb5LoginModuleName()} required
      |  useKeyTab=true
      |  storeKey=true
      |  useTicketCache=false
      |  refreshKrb5Config=true
      |  keyTab="${zkServerKeytabFile.getAbsolutePath()}"
      |  principal="$zkServerUser@$realm";
      |};
      |
      |Client {
      |  ${SecurityUtils.getKrb5LoginModuleName()} required
      |  useKeyTab=true
      |  storeKey=true
      |  useTicketCache=false
      |  refreshKrb5Config=true
      |  keyTab="${zkClientKeytabFile.getAbsolutePath()}"
      |  principal="$zkClientUser@$realm";
      |};
      |
      |KafkaServer {
      |  ${SecurityUtils.getKrb5LoginModuleName()} required
      |  serviceName="$brokerServiceName"
      |  useKeyTab=true
      |  storeKey=true
      |  keyTab="${kafkaServerKeytabFile.getAbsolutePath()}"
      |  principal="$kafkaServerUser@$realm";
      |};
      """.stripMargin.trim
    Files.write(content, file, StandardCharsets.UTF_8)
    logDebug(s"Created JAAS file: ${file.getPath}")
    logDebug(s"JAAS file content: $content")
    file.getAbsolutePath()
  }

  // Set up the Embedded Zookeeper server and get the proper Zookeeper port
  private def setupEmbeddedZookeeper(): Unit = {
    // Zookeeper server startup
    zookeeper = new EmbeddedZookeeper(s"$zkHost:$zkPort")
    // Get the actual zookeeper binding port
    zkPort = zookeeper.actualPort
    zkClient = KafkaZkClient(s"$zkHost:$zkPort", isSecure = false, zkSessionTimeout,
      zkConnectionTimeout, 1, new SystemTime(), "test", new ZKClientConfig)
    zkReady = true
  }

  // Set up the Embedded Kafka server
  private def setupEmbeddedKafkaServer(): Unit = {
    assert(zkReady, "Zookeeper should be set up beforehand")

    val protocolName = if (!secure) PLAINTEXT.name else SASL_PLAINTEXT.name

    // Kafka broker startup
    Utils.startServiceOnPort(brokerPort, port => {
      brokerPort = port
      brokerConf = new KafkaConfig(brokerConfiguration, doLog = false)
      server = new KafkaServer(brokerConf)
      server.startup()
      brokerPort = server.boundPort(new ListenerName(protocolName))
      (server, brokerPort)
    }, new SparkConf(), "KafkaBroker")

    adminClient = AdminClient.create(adminClientConfiguration)
    brokerReady = true
  }

  /** setup the whole embedded servers, including Zookeeper and Kafka brokers */
  def setup(): Unit = {
    // Set up a KafkaTestUtils leak detector so that we can see where the leak KafkaTestUtils is
    // created.
    val exception = new SparkException("It was created at: ")
    leakDetector = ShutdownHookManager.addShutdownHook { () =>
      logError("Found a leak KafkaTestUtils.", exception)
    }

    if (secure) {
      SecurityUtils.setGlobalKrbDebug(true)
      setUpMiniKdc()
      val jaasConfigFile = createKeytabsAndJaasConfigFile()
      System.setProperty(JAVA_AUTH_CONFIG, jaasConfigFile)
      Configuration.getConfiguration.refresh()
    } else {
      System.clearProperty(JAVA_AUTH_CONFIG)
    }
    setupEmbeddedZookeeper()
    setupEmbeddedKafkaServer()
    eventually(timeout(1.minute)) {
      assert(zkClient.getAllBrokersInCluster.nonEmpty, "Broker was not up in 60 seconds")
    }
  }

  /** Teardown the whole servers, including Kafka broker and Zookeeper */
  def teardown(): Unit = {
    if (leakDetector != null) {
      ShutdownHookManager.removeShutdownHook(leakDetector)
    }
    brokerReady = false
    zkReady = false
    kdcReady = false

    if (producer != null) {
      producer.close()
      producer = null
    }

    if (adminClient != null) {
      adminClient.close()
      adminClient = null
    }

    if (server != null) {
      server.shutdown()
      server.awaitShutdown()
      server = null
    }

    // On Windows, `logDirs` is left open even after Kafka server above is completely shut down
    // in some cases. It leads to test failures on Windows if the directory deletion failure
    // throws an exception.
    brokerConf.logDirs.foreach { f =>
      try {
        Utils.deleteRecursively(new File(f))
      } catch {
        case e: IOException if Utils.isWindows =>
          logWarning(e.getMessage)
      }
    }

    if (zkClient != null) {
      zkClient.close()
      zkClient = null
    }

    if (zookeeper != null) {
      zookeeper.shutdown()
      zookeeper = null
    }

    System.clearProperty(JAVA_AUTH_CONFIG)
    Configuration.getConfiguration.refresh()
    if (kdc != null) {
      kdc.stop()
      kdc = null
    }
    UserGroupInformation.reset()
    SecurityUtils.setGlobalKrbDebug(false)
  }

  /** Create a Kafka topic and wait until it is propagated to the whole cluster */
  def createTopic(topic: String, partitions: Int, overwrite: Boolean = false): Unit = {
    var created = false
    while (!created) {
      try {
        val newTopic = new NewTopic(topic, partitions, 1.shortValue())
        adminClient.createTopics(Collections.singleton(newTopic))
        created = true
      } catch {
        // Workaround fact that TopicExistsException is in kafka.common in 0.10.0 and
        // org.apache.kafka.common.errors in 0.10.1 (!)
        case e: Exception if (e.getClass.getSimpleName == "TopicExistsException") && overwrite =>
          deleteTopic(topic)
      }
    }
    // wait until metadata is propagated
    (0 until partitions).foreach { p =>
      waitUntilMetadataIsPropagated(topic, p)
    }
  }

  def getAllTopicsAndPartitionSize(): Seq[(String, Int)] = {
    zkClient.getPartitionsForTopics(zkClient.getAllTopicsInCluster()).mapValues(_.size).toSeq
  }

  /** Create a Kafka topic and wait until it is propagated to the whole cluster */
  def createTopic(topic: String): Unit = {
    createTopic(topic, 1)
  }

  /** Delete a Kafka topic and wait until it is propagated to the whole cluster */
  def deleteTopic(topic: String): Unit = {
    val partitions = zkClient.getPartitionsForTopics(Set(topic))(topic).size
    adminClient.deleteTopics(Collections.singleton(topic))
    verifyTopicDeletionWithRetries(topic, partitions, List(this.server))
  }

  /** Add new partitions to a Kafka topic */
  def addPartitions(topic: String, partitions: Int): Unit = {
    adminClient.createPartitions(
      Map(topic -> NewPartitions.increaseTo(partitions)).asJava,
      new CreatePartitionsOptions)
    // wait until metadata is propagated
    (0 until partitions).foreach { p =>
      waitUntilMetadataIsPropagated(topic, p)
    }
  }

  def sendMessages(topic: String, msgs: Array[String]): Seq[(String, RecordMetadata)] = {
    sendMessages(topic, msgs, None)
  }

  def sendMessages(
      topic: String,
      msgs: Array[String],
      part: Option[Int]): Seq[(String, RecordMetadata)] = {
    val records = msgs.map { msg =>
      val builder = new RecordBuilder(topic, msg)
      part.foreach { p => builder.partition(p) }
      builder.build()
    }
    sendMessages(records)
  }

  def sendMessage(msg: ProducerRecord[String, String]): Seq[(String, RecordMetadata)] = {
    sendMessages(Array(msg))
  }

  def sendMessages(msgs: Seq[ProducerRecord[String, String]]): Seq[(String, RecordMetadata)] = {
    producer = new KafkaProducer[String, String](producerConfiguration)
    val offsets = try {
      msgs.map { msg =>
        val metadata = producer.send(msg).get(10, TimeUnit.SECONDS)
        logInfo(s"\tSent ($msg) to partition ${metadata.partition}, offset ${metadata.offset}")
        (msg.value(), metadata)
      }
    } finally {
      if (producer != null) {
        producer.close()
        producer = null
      }
    }
    offsets
  }

  def cleanupLogs(): Unit = {
    server.logManager.cleanupLogs()
  }

  private def getOffsets(topics: Set[String], offsetSpec: OffsetSpec): Map[TopicPartition, Long] = {
    val listOffsetsParams = adminClient.describeTopics(topics.asJava).all().get().asScala
      .flatMap { topicDescription =>
        topicDescription._2.partitions().asScala.map { topicPartitionInfo =>
          new TopicPartition(topicDescription._1, topicPartitionInfo.partition())
        }
      }.map(_ -> offsetSpec).toMap.asJava
    val partitionOffsets = adminClient.listOffsets(listOffsetsParams).all().get().asScala
      .map(result => result._1 -> result._2.offset()).toMap
    partitionOffsets
  }

  def getEarliestOffsets(topics: Set[String]): Map[TopicPartition, Long] = {
    getOffsets(topics, OffsetSpec.earliest())
  }

  def getLatestOffsets(topics: Set[String]): Map[TopicPartition, Long] = {
    getOffsets(topics, OffsetSpec.latest())
  }

  def listConsumerGroups(): ListConsumerGroupsResult = {
    adminClient.listConsumerGroups()
  }

  protected def brokerConfiguration: Properties = {
    val props = new Properties()
    props.put("broker.id", "0")
    props.put("listeners", s"PLAINTEXT://127.0.0.1:$brokerPort")
    props.put("log.dir", Utils.createTempDir().getAbsolutePath)
    props.put("zookeeper.connect", zkAddress)
    props.put("zookeeper.connection.timeout.ms", "60000")
    props.put("log.flush.interval.messages", "1")
    props.put("replica.socket.timeout.ms", "1500")
    props.put("delete.topic.enable", "true")
    props.put("group.initial.rebalance.delay.ms", "10")

    // Change the following settings as we have only 1 broker
    props.put("offsets.topic.num.partitions", "1")
    props.put("offsets.topic.replication.factor", "1")
    props.put("transaction.state.log.replication.factor", "1")
    props.put("transaction.state.log.min.isr", "1")

    if (secure) {
      props.put("listeners", "SASL_PLAINTEXT://127.0.0.1:0")
      props.put("advertised.listeners", "SASL_PLAINTEXT://127.0.0.1:0")
      props.put("inter.broker.listener.name", "SASL_PLAINTEXT")
      props.put("delegation.token.master.key", UUID.randomUUID().toString)
      props.put("sasl.enabled.mechanisms", "GSSAPI,SCRAM-SHA-512")
    }

    // Can not use properties.putAll(propsMap.asJava) in scala-2.12
    // See https://github.com/scala/bug/issues/10418
    withBrokerProps.foreach { case (k, v) => props.put(k, v) }
    props
  }

  private def adminClientConfiguration: Properties = {
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, s"$brokerHost:$brokerPort")
    setAuthenticationConfigIfNeeded(props)
    props
  }

  private def producerConfiguration: Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", brokerAddress)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props.put("key.serializer", classOf[StringSerializer].getName)
    // wait for all in-sync replicas to ack sends
    props.put("acks", "all")
    setAuthenticationConfigIfNeeded(props)
    props
  }

  /** Call `f` with a `KafkaProducer` that has initialized transactions. */
  def withTransactionalProducer(f: KafkaProducer[String, String] => Unit): Unit = {
    val props = producerConfiguration
    props.put("transactional.id", UUID.randomUUID().toString)
    val producer = new KafkaProducer[String, String](props)
    try {
      producer.initTransactions()
      f(producer)
    } finally {
      producer.close()
    }
  }

  private def setAuthenticationConfigIfNeeded(props: Properties): Unit = {
    if (secure) {
      val jaasParams = KafkaTokenUtil.getKeytabJaasParams(
        clientKeytabFile.getAbsolutePath, clientPrincipal, brokerServiceName)
      props.put(SaslConfigs.SASL_JAAS_CONFIG, jaasParams)
      props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SASL_PLAINTEXT.name)
    }
  }

  /** Verify topic is deleted in all places, e.g, brokers, zookeeper. */
  private def verifyTopicDeletion(
      topic: String,
      numPartitions: Int,
      servers: Seq[KafkaServer]): Unit = {
    val topicAndPartitions = (0 until numPartitions).map(new TopicPartition(topic, _))

    // wait until admin path for delete topic is deleted, signaling completion of topic deletion
    assert(!zkClient.isTopicMarkedForDeletion(topic), "topic is still marked for deletion")
    assert(!zkClient.topicExists(topic), "topic still exists")
    // ensure that the topic-partition has been deleted from all brokers' replica managers
    assert(servers.forall(server => topicAndPartitions.forall(tp =>
      server.replicaManager.getPartition(tp) == HostedPartition.None)),
      s"topic $topic still exists in the replica manager")
    // ensure that logs from all replicas are deleted if delete topic is marked successful
    assert(servers.forall(server => topicAndPartitions.forall(tp =>
      server.getLogManager.getLog(tp).isEmpty)),
      s"topic $topic still exists in log manager")
    // ensure that topic is removed from all cleaner offsets
    assert(servers.forall(server => topicAndPartitions.forall { tp =>
      val checkpoints = server.getLogManager.liveLogDirs.map { logDir =>
        new OffsetCheckpointFile(new File(logDir, "cleaner-offset-checkpoint")).read()
      }
      checkpoints.forall(checkpointsPerLogDir => !checkpointsPerLogDir.contains(tp))
    }), s"checkpoint for topic $topic still exists")
    // ensure the topic is gone
    assert(
      !zkClient.getAllTopicsInCluster().contains(topic),
      s"topic $topic still exists on zookeeper")
  }

  /** Verify topic is deleted. Retry to delete the topic if not. */
  private def verifyTopicDeletionWithRetries(
      topic: String,
      numPartitions: Int,
      servers: Seq[KafkaServer]): Unit = {
    eventually(timeout(1.minute), interval(200.milliseconds)) {
      try {
        verifyTopicDeletion(topic, numPartitions, servers)
      } catch {
        case e: Throwable =>
          // As pushing messages into Kafka updates Zookeeper asynchronously, there is a small
          // chance that a topic will be recreated after deletion due to the asynchronous update.
          // Hence, delete the topic and retry.
          adminClient.deleteTopics(Collections.singleton(topic))
          throw e
      }
    }
  }

  private def waitUntilMetadataIsPropagated(topic: String, partition: Int): Unit = {
    def isPropagated = server.dataPlaneRequestProcessor.metadataCache
        .getPartitionInfo(topic, partition) match {
      case Some(partitionState) =>
        zkClient.getLeaderForPartition(new TopicPartition(topic, partition)).isDefined &&
          Request.isValidBrokerId(partitionState.leader) &&
          !partitionState.replicas.isEmpty

      case _ =>
        false
    }
    eventually(timeout(1.minute)) {
      assert(isPropagated, s"Partition [$topic, $partition] metadata not propagated after timeout")
    }
  }

  /**
   * Wait until the latest offset of the given `TopicPartition` is not less than `offset`.
   */
  def waitUntilOffsetAppears(topicPartition: TopicPartition, offset: Long): Unit = {
    eventually(timeout(1.minute)) {
      val currentOffset = getLatestOffsets(Set(topicPartition.topic)).get(topicPartition)
      assert(currentOffset.nonEmpty && currentOffset.get >= offset)
    }
  }

  private class EmbeddedZookeeper(val zkConnect: String) {
    private val ZOOKEEPER_AUTH_PROVIDER = "zookeeper.authProvider.1"

    val snapshotDir = Utils.createTempDir()
    val logDir = Utils.createTempDir()

    if (secure) {
      System.setProperty(ZOOKEEPER_AUTH_PROVIDER, classOf[SASLAuthenticationProvider].getName)
    } else {
      System.clearProperty(ZOOKEEPER_AUTH_PROVIDER)
    }
    val zookeeper = new ZooKeeperServer(snapshotDir, logDir, 500)
    val (ip, port) = {
      val splits = zkConnect.split(":")
      (splits(0), splits(1).toInt)
    }
    val factory = new NIOServerCnxnFactory()
    factory.configure(new InetSocketAddress(ip, port), 16)
    factory.startup(zookeeper)

    val actualPort = factory.getLocalPort

    def shutdown(): Unit = {
      factory.shutdown()
      // The directories are not closed even if the ZooKeeper server is shut down.
      // Please see ZOOKEEPER-1844, which is fixed in 3.4.6+. It leads to test failures
      // on Windows if the directory deletion failure throws an exception.
      try {
        Utils.deleteRecursively(snapshotDir)
      } catch {
        case e: IOException if Utils.isWindows =>
          logWarning(e.getMessage)
      }
      try {
        Utils.deleteRecursively(logDir)
      } catch {
        case e: IOException if Utils.isWindows =>
          logWarning(e.getMessage)
      }
      System.clearProperty(ZOOKEEPER_AUTH_PROVIDER)
    }
  }
}
