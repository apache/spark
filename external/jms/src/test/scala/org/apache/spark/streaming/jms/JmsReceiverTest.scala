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

package org.apache.spark.streaming.jms

import java.net.ServerSocket
import java.util.{Properties, UUID}
import javax.jms._
import javax.naming.{Context, InitialContext}

import org.apache.activemq.{ActiveMQSession, ActiveMQConnectionFactory}
import org.apache.activemq.broker.BrokerService
import org.apache.commons.lang3.RandomUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration
import scala.language.postfixOps

class JmsReceiverTest extends SparkFunSuite with BeforeAndAfter with Eventually {
  private val batchDuration = Milliseconds(500)
  private val master = "local[2]"
  private val framework = this.getClass.getSimpleName
  private var ssc: StreamingContext = _
  private var broker: BrokerService = _
  private var brokerUrl: String = _
  val results = ArrayBuffer[String]()

  var connectionFactory: ActiveMQConnectionFactory = _

  before {

    brokerUrl = s"tcp://localhost:${findFreePort()}"

    startBroker

    ssc = new StreamingContext(master, framework, batchDuration)
    results.clear()
  }

  def startBroker: Boolean = {
    broker = new BrokerService()

    broker.setPersistent(false)
    val systemUsage = broker.getSystemUsage()
    systemUsage.getTempUsage.setLimit(100 * 1024 * 1024)
    broker.addConnector(brokerUrl)
    broker.start()
    broker.waitUntilStarted()
  }

  after {
    ssc.stop(true)
    stopBroker
    System.gc()
  }

  def stopBroker: Unit = {
    broker.stop()
    broker.waitUntilStopped()
  }

  import duration._

  val numbOfMessages: Int = 1000

  test("receiver on Topic sync") {
    val destName: String = "dynamicTopics/BARR.FOO"
    val testsub: String = "testSub"
    testDestination(DurableTopicJmsDestinationInfo(destName, testsub),
      destName,
      true,
      true,
      Session.CLIENT_ACKNOWLEDGE)
  }

  test("receiver on queue sync") {
    val destName: String = "dynamicQueues/FOO.BARR"
    testDestination(QueueJmsDestinationInfo(destName), destName, false, true,
      Session.CLIENT_ACKNOWLEDGE)
  }

  test("receiver on Topic async") {
    val destName: String = "dynamicTopics/BARR.FOO"
    val testsub: String = "testSub"
    testDestination(DurableTopicJmsDestinationInfo(destName, testsub),
      destName,
      true,
      false,
      Session.CLIENT_ACKNOWLEDGE)
  }

  test("receiver on queue async") {
    val destName: String = "dynamicQueues/FOO.BARR"
    testDestination(QueueJmsDestinationInfo(destName), destName, false, false,
      Session.CLIENT_ACKNOWLEDGE)
  }
  // INDIVIDUAL_ACKNOWLEDGE Not supported with ActiveMQ on topics
  ignore("receiver on Topic async reliable") {
    val destName: String = "dynamicTopics/BARR.FOOR"
    val testsub: String = "testSub"
    testDestination(DurableTopicJmsDestinationInfo(destName, testsub),
      destName,
      true,
      false,
      ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE)
  }

  test("receiver on queue async reliable") {
    val destName: String = "dynamicQueues/FOO.BARR"
    testDestination(QueueJmsDestinationInfo(destName), destName, false, false,
      ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE)
  }




  test("stop and start sync") {
    testStopStart(false)

  }

  test("stop and start async") {
    testStopStart(true)

  }

  def testStopStart(sync: Boolean) {

    val destName: String = "dynamicQueues/FOOO.BARR"
    val props = new Properties()
    props.setProperty(Context.INITIAL_CONTEXT_FACTORY,
      "org.apache.activemq.jndi.ActiveMQInitialContextFactory")
    props.setProperty(Context.PROVIDER_URL, brokerUrl)


    val intialContext = new InitialContext(props)

    val converter: Message => Option[String] = {
      case msg: TextMessage =>
        Some(msg.getText)
      case _ =>
        None
    }



    val stream = if (sync) {
      JmsStreamUtils.createSynchronousJmsQueueStream(ssc,
        JndiMessageConsumerFactory(props, QueueJmsDestinationInfo(destName)),
        converter,
        1000,
        1.second,
        10.seconds
      )
    }
    else {
      JmsStreamUtils.createAsynchronousJmsQueueStream(ssc,
        JndiMessageConsumerFactory(props, QueueJmsDestinationInfo(destName)),
        converter,
        ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE)
    }

    stream.foreachRDD {
      rdd =>
        results ++= rdd.collect()
    }

    stream.start()
    ssc.start()
    sendMessages(destName, false, props)
    eventually(timeout(10000 milliseconds), interval(1000 milliseconds)) {
      assert(results.size == numbOfMessages)

    }

    stopBroker
    results.clear()
    Thread.sleep(1000)

    startBroker

    sendMessages(destName, false, props)

    eventually(timeout(10000 milliseconds), interval(1000 milliseconds)) {
      assert(results.size == numbOfMessages)

    }

  }

  private def testDestination(jmsDestinationInfo: JmsDestinationInfo, destName: String,
                              preSendMessage: Boolean, sync: Boolean, ackMode: Int) = {
    results.clear()

    val props = new Properties()
    props.setProperty(Context.INITIAL_CONTEXT_FACTORY,
      "org.apache.activemq.jndi.ActiveMQInitialContextFactory")
    props.setProperty(Context.PROVIDER_URL, brokerUrl)

    val propsSpark = new Properties()
    propsSpark.setProperty("clientID", UUID.randomUUID().toString)
    propsSpark.setProperty(Context.INITIAL_CONTEXT_FACTORY,
      "org.apache.activemq.jndi.ActiveMQInitialContextFactory")
    propsSpark.setProperty(Context.PROVIDER_URL, brokerUrl)

    val converter: Message => Option[String] = {
      case msg: TextMessage =>
        Some(msg.getText)
      case _ =>
        None
    }


    val stream = if (sync) {
      JmsStreamUtils.createSynchronousJmsQueueStream(ssc,
        JndiMessageConsumerFactory(propsSpark, jmsDestinationInfo),
        converter,
        1000,
        1.second,
        10.seconds
      )
    }
    else {
      JmsStreamUtils.createAsynchronousJmsQueueStream(ssc,
        JndiMessageConsumerFactory(propsSpark, jmsDestinationInfo),
        converter,
        ackMode)
    }


    stream.foreachRDD {
      rdd =>
        results ++= rdd.collect()
    }

    stream.start()
    ssc.start()
    sendMessages(destName, preSendMessage, props)

    eventually(timeout(10000 milliseconds), interval(1000 milliseconds)) {
      assert(results.size == numbOfMessages)
      assert(results.map(_.toInt).sum == (0 until numbOfMessages).sum)

    }
  }

  def sendMessages(destName: String,
                   preSendMessage: Boolean,
                   props: Properties): Unit = {
    val intialContext = new InitialContext(props)

    val dest = intialContext.lookup(destName).asInstanceOf[Destination]

    val connectionFactory = intialContext
      .lookup("ConnectionFactory").asInstanceOf[ConnectionFactory]
    val connection = connectionFactory.createConnection()
    connection.start()
    val session: Session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val publisher = session.createProducer(dest)
    if (preSendMessage) {
      // Topics throw messages away till connection so we sen meesages till the first comes through
      var count = 0
      while (results.isEmpty && count < 10) {

        val textMessage = session.createTextMessage()
        count = count + 1
        textMessage.setText(count.toString)
        publisher.send(textMessage)
        Thread.sleep(1000)
      }


      eventually(timeout(10000 milliseconds), interval(1000 milliseconds)) {
        assert(results.contains(count.toString))
      }
      results.clear()
    }

    0 until numbOfMessages foreach {
      num =>
        val textMessage = session.createTextMessage()
        textMessage.setText(num.toString)
        publisher.send(textMessage)
    }
    connection.close()
  }

  private def findFreePort(): Int = {
    val candidatePort = RandomUtils.nextInt(1024, 65536)
    Utils.startServiceOnPort(candidatePort, (trialPort: Int) => {
      val socket = new ServerSocket(trialPort)
      socket.close()
      (null, trialPort)
    }, new SparkConf())._2
  }
}
