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
import java.util.concurrent.{Executors, ThreadFactory}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition

import org.apache.spark.internal.Logging
import org.apache.spark.sql.kafka010.KafkaOffsetReader.ConsumerStrategy
import org.apache.spark.sql.types._
import org.apache.spark.util.{ThreadUtils, UninterruptibleThread}


private[kafka010] trait KafkaOffsetReader {

  /**
   * Closes the connection to Kafka, and cleans up state.
   */
  def close()

  /**
   * Set consumer position to specified offsets, making sure all assignments are set.
   */
  def fetchSpecificStartingOffsets(
    partitionOffsets: Map[TopicPartition, Long]): Map[TopicPartition, Long]

  /**
   * Fetch the earliest offsets of partitions.
   */
  def fetchEarliestOffsets(): Map[TopicPartition, Long]

  /**
   * Fetch the latest offsets of partitions.
   */
  def fetchLatestOffsets(): Map[TopicPartition, Long]

  /**
   * Fetch the earliest offsets for newly discovered partitions. The return result may not contain
   * some partitions if they are deleted.
   */
  def fetchNewPartitionEarliestOffsets(
    newPartitions: Seq[TopicPartition]): Map[TopicPartition, Long]
}

/**
 * This class uses Kafka's own [[KafkaConsumer]] API to read data offsets from Kafka.
 *
 * - The [[ConsumerStrategy]] class defines which Kafka topics and partitions should be read
 *   by this source. These strategies directly correspond to the different consumption options
 *   in . This class is designed to return a configured [[KafkaConsumer]] that is used by the
 *   [[KafkaSource]] to query for the offsets. See the docs on
 *   [[org.apache.spark.sql.kafka010.KafkaOffsetReader.ConsumerStrategy]] for more details.
 */
private[kafka010] class KafkaOffsetReaderImpl(
    consumerStrategy: ConsumerStrategy,
    driverKafkaParams: ju.Map[String, Object],
    readerOptions: Map[String, String],
    driverGroupIdPrefix: String)
  extends KafkaOffsetReader with Logging {

  /**
   * A KafkaConsumer used in the driver to query the latest Kafka offsets. This only queries the
   * offsets and never commits them.
   */
  protected var consumer = createConsumer()

  private val maxOffsetFetchAttempts =
    readerOptions.getOrElse("fetchOffset.numRetries", "3").toInt

  private val offsetFetchAttemptIntervalMs =
    readerOptions.getOrElse("fetchOffset.retryIntervalMs", "1000").toLong

  private var groupId: String = null

  private var nextId = 0

  private def nextGroupId(): String = {
    groupId = driverGroupIdPrefix + "-" + nextId
    nextId += 1
    groupId
  }

  override def toString(): String = consumerStrategy.toString

  def close(): Unit = consumer.close()

  def fetchSpecificStartingOffsets(
      partitionOffsets: Map[TopicPartition, Long]): Map[TopicPartition, Long] =
    withRetriesWithoutInterrupt {
      // Poll to get the latest assigned partitions
      consumer.poll(0)
      val partitions = consumer.assignment()
      consumer.pause(partitions)
      assert(partitions.asScala == partitionOffsets.keySet,
        "If startingOffsets contains specific offsets, you must specify all TopicPartitions.\n" +
          "Use -1 for latest, -2 for earliest, if you don't care.\n" +
          s"Specified: ${partitionOffsets.keySet} Assigned: ${partitions.asScala}")
      logDebug(s"Partitions assigned to consumer: $partitions. Seeking to $partitionOffsets")

      partitionOffsets.foreach {
        case (tp, -1) => consumer.seekToEnd(ju.Arrays.asList(tp))
        case (tp, -2) => consumer.seekToBeginning(ju.Arrays.asList(tp))
        case (tp, off) => consumer.seek(tp, off)
      }
      partitionOffsets.map {
        case (tp, _) => tp -> consumer.position(tp)
      }
    }

  def fetchEarliestOffsets(): Map[TopicPartition, Long] = withRetriesWithoutInterrupt {
    // Poll to get the latest assigned partitions
    consumer.poll(0)
    val partitions = consumer.assignment()
    consumer.pause(partitions)
    logDebug(s"Partitions assigned to consumer: $partitions. Seeking to the beginning")

    consumer.seekToBeginning(partitions)
    val partitionOffsets = partitions.asScala.map(p => p -> consumer.position(p)).toMap
    logDebug(s"Got earliest offsets for partition : $partitionOffsets")
    partitionOffsets
  }

  def fetchLatestOffsets(): Map[TopicPartition, Long] = withRetriesWithoutInterrupt {
    // Poll to get the latest assigned partitions
    consumer.poll(0)
    val partitions = consumer.assignment()
    consumer.pause(partitions)
    logDebug(s"Partitions assigned to consumer: $partitions. Seeking to the end.")

    consumer.seekToEnd(partitions)
    val partitionOffsets = partitions.asScala.map(p => p -> consumer.position(p)).toMap
    logDebug(s"Got latest offsets for partition : $partitionOffsets")
    partitionOffsets
  }

  def fetchNewPartitionEarliestOffsets(
      newPartitions: Seq[TopicPartition]): Map[TopicPartition, Long] = {
    if (newPartitions.isEmpty) {
      Map.empty[TopicPartition, Long]
    } else {
      withRetriesWithoutInterrupt {
        // Poll to get the latest assigned partitions
        consumer.poll(0)
        val partitions = consumer.assignment()
        consumer.pause(partitions)
        logDebug(s"\tPartitions assigned to consumer: $partitions")

        // Get the earliest offset of each partition
        consumer.seekToBeginning(partitions)
        val partitionOffsets = newPartitions.filter { p =>
          // When deleting topics happen at the same time, some partitions may not be in
          // `partitions`. So we need to ignore them
          partitions.contains(p)
        }.map(p => p -> consumer.position(p)).toMap
        logDebug(s"Got earliest offsets for new partitions: $partitionOffsets")
        partitionOffsets
      }
    }
  }

  /**
   * Helper function that does multiple retries on the a body of code that returns offsets.
   * Retries are needed to handle transient failures. For e.g. race conditions between getting
   * assignment and getting position while topics/partitions are deleted can cause NPEs.
   *
   * This method also makes sure `body` won't be interrupted to workaround a potential issue in
   * `KafkaConsumer.poll`. (KAFKA-1894)
   */
  private def withRetriesWithoutInterrupt(
      body: => Map[TopicPartition, Long]): Map[TopicPartition, Long] = {
    // Make sure `KafkaConsumer.poll` won't be interrupted (KAFKA-1894)
    assert(Thread.currentThread().isInstanceOf[UninterruptibleThread])

    synchronized {
      var result: Option[Map[TopicPartition, Long]] = None
      var attempt = 1
      var lastException: Throwable = null
      while (result.isEmpty && attempt <= maxOffsetFetchAttempts
        && !Thread.currentThread().isInterrupted) {
        Thread.currentThread match {
          case ut: UninterruptibleThread =>
            // "KafkaConsumer.poll" may hang forever if the thread is interrupted (E.g., the query
            // is stopped)(KAFKA-1894). Hence, we just make sure we don't interrupt it.
            //
            // If the broker addresses are wrong, or Kafka cluster is down, "KafkaConsumer.poll" may
            // hang forever as well. This cannot be resolved in KafkaSource until Kafka fixes the
            // issue.
            ut.runUninterruptibly {
              try {
                result = Some(body)
              } catch {
                case NonFatal(e) =>
                  lastException = e
                  logWarning(s"Error in attempt $attempt getting Kafka offsets: ", e)
                  attempt += 1
                  Thread.sleep(offsetFetchAttemptIntervalMs)
                  resetConsumer()
              }
            }
          case _ =>
            throw new IllegalStateException(
              "Kafka APIs must be executed on a o.a.spark.util.UninterruptibleThread")
        }
      }
      if (Thread.interrupted()) {
        throw new InterruptedException()
      }
      if (result.isEmpty) {
        assert(attempt > maxOffsetFetchAttempts)
        assert(lastException != null)
        throw lastException
      }
      result.get
    }
  }

  /**
   * Create a consumer using the new generated group id. We always use a new consumer to avoid
   * just using a broken consumer to retry on Kafka errors, which likely will fail again.
   */
  private def createConsumer(): Consumer[Array[Byte], Array[Byte]] = synchronized {
    val newKafkaParams = new ju.HashMap[String, Object](driverKafkaParams)
    newKafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, nextGroupId())
    consumerStrategy.createConsumer(newKafkaParams)
  }

  private def resetConsumer(): Unit = synchronized {
    consumer.close()
    consumer = createConsumer()
  }
}

/**
 * The Kafka Consumer must be called in an UninterruptibleThread. This naturally occurs
 * in Structured Streaming, but not in Spark SQL, which will use this call to communicate
 * with Kafak for obtaining offsets.
 *
 * @param kafkaOffsetReader Basically in instance of [[KafkaOffsetReaderImpl]] that
 *                          this class wraps and executes in an [[UninterruptibleThread]]
 */
private[kafka010] class UninterruptibleKafkaOffsetReader(kafkaOffsetReader: KafkaOffsetReader)
  extends KafkaOffsetReader with Logging {

  val kafkaReaderThread = Executors.newSingleThreadExecutor(new ThreadFactory {
    override def newThread(r: Runnable): Thread = {
      val t = new UninterruptibleThread("Kafka Offset Reader") {
        override def run(): Unit = {
          r.run()
        }
      }
      t.setDaemon(true)
      t
    }
  })
  val execContext = ExecutionContext.fromExecutorService(kafkaReaderThread)

  override def close(): Unit = {
    kafkaOffsetReader.close()
    kafkaReaderThread.shutdownNow()
  }

  override def fetchSpecificStartingOffsets(
    partitionOffsets: Map[TopicPartition, Long]): Map[TopicPartition, Long] = {
    val future = Future {
      kafkaOffsetReader.fetchSpecificStartingOffsets(partitionOffsets)
    }(execContext)
    ThreadUtils.awaitResult(future, Duration.Inf)
  }

  override def fetchEarliestOffsets(): Map[TopicPartition, Long] = {
      val future = Future {
        kafkaOffsetReader.fetchEarliestOffsets()
      }(execContext)
      ThreadUtils.awaitResult(future, Duration.Inf)
  }

  override def fetchLatestOffsets(): Map[TopicPartition, Long] = {
    val future = Future {
      kafkaOffsetReader.fetchLatestOffsets()
    }(execContext)
    ThreadUtils.awaitResult(future, Duration.Inf)
  }

  override def fetchNewPartitionEarliestOffsets(
    newPartitions: Seq[TopicPartition]): Map[TopicPartition, Long] = {
    val future = Future {
      kafkaOffsetReader.fetchNewPartitionEarliestOffsets(newPartitions)
    }(execContext)
    ThreadUtils.awaitResult(future, Duration.Inf)
  }
}

private[kafka010] object KafkaOffsetReader {

  def kafkaSchema: StructType = StructType(Seq(
    StructField("key", BinaryType),
    StructField("value", BinaryType),
    StructField("topic", StringType),
    StructField("partition", IntegerType),
    StructField("offset", LongType),
    StructField("timestamp", TimestampType),
    StructField("timestampType", IntegerType)
  ))

  sealed trait ConsumerStrategy {
    def createConsumer(kafkaParams: ju.Map[String, Object]): Consumer[Array[Byte], Array[Byte]]
  }

  case class AssignStrategy(partitions: Array[TopicPartition]) extends ConsumerStrategy {
    override def createConsumer(
        kafkaParams: ju.Map[String, Object]): Consumer[Array[Byte], Array[Byte]] = {
      val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](kafkaParams)
      consumer.assign(ju.Arrays.asList(partitions: _*))
      consumer
    }

    override def toString: String = s"Assign[${partitions.mkString(", ")}]"
  }

  case class SubscribeStrategy(topics: Seq[String]) extends ConsumerStrategy {
    override def createConsumer(
        kafkaParams: ju.Map[String, Object]): Consumer[Array[Byte], Array[Byte]] = {
      val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](kafkaParams)
      consumer.subscribe(topics.asJava)
      consumer
    }

    override def toString: String = s"Subscribe[${topics.mkString(", ")}]"
  }

  case class SubscribePatternStrategy(topicPattern: String)
    extends ConsumerStrategy {
    override def createConsumer(
        kafkaParams: ju.Map[String, Object]): Consumer[Array[Byte], Array[Byte]] = {
      val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](kafkaParams)
      consumer.subscribe(
        ju.regex.Pattern.compile(topicPattern),
        new NoOpConsumerRebalanceListener())
      consumer
    }

    override def toString: String = s"SubscribePattern[$topicPattern]"
  }
}
