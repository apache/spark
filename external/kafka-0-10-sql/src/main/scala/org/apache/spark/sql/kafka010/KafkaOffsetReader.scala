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
import org.apache.kafka.common.TopicPartition

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._
import org.apache.spark.util.{ThreadUtils, UninterruptibleThread}

/**
 * This class uses Kafka's own [[KafkaConsumer]] API to read data offsets from Kafka.
 * The [[ConsumerStrategy]] class defines which Kafka topics and partitions should be read
 * by this source. These strategies directly correspond to the different consumption options
 * in. This class is designed to return a configured [[KafkaConsumer]] that is used by the
 * [[KafkaSource]] to query for the offsets. See the docs on
 * [[org.apache.spark.sql.kafka010.ConsumerStrategy]]
 * for more details.
 *
 * Note: This class is not ThreadSafe
 */
private[kafka010] class KafkaOffsetReader(
    consumerStrategy: ConsumerStrategy,
    driverKafkaParams: ju.Map[String, Object],
    readerOptions: Map[String, String],
    driverGroupIdPrefix: String) extends Logging {
  /**
   * Used to ensure execute fetch operations execute in an UninterruptibleThread
   */
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

  /**
   * Place [[groupId]] and [[nextId]] here so that they are initialized before any consumer is
   * created -- see SPARK-19564.
   */
  private var groupId: String = null
  private var nextId = 0

  /**
   * A KafkaConsumer used in the driver to query the latest Kafka offsets. This only queries the
   * offsets and never commits them.
   */
  protected var consumer = createConsumer()

  private val maxOffsetFetchAttempts =
    readerOptions.getOrElse("fetchOffset.numRetries", "3").toInt

  private val offsetFetchAttemptIntervalMs =
    readerOptions.getOrElse("fetchOffset.retryIntervalMs", "1000").toLong

  private def nextGroupId(): String = {
    groupId = driverGroupIdPrefix + "-" + nextId
    nextId += 1
    groupId
  }

  override def toString(): String = consumerStrategy.toString

  /**
   * Closes the connection to Kafka, and cleans up state.
   */
  def close(): Unit = {
    runUninterruptibly {
      consumer.close()
    }
    kafkaReaderThread.shutdown()
  }

  /**
   * @return The Set of TopicPartitions for a given topic
   */
  def fetchTopicPartitions(): Set[TopicPartition] = runUninterruptibly {
    assert(Thread.currentThread().isInstanceOf[UninterruptibleThread])
    // Poll to get the latest assigned partitions
    consumer.poll(0)
    val partitions = consumer.assignment()
    consumer.pause(partitions)
    partitions.asScala.toSet
  }

  /**
   * Resolves the specific offsets based on Kafka seek positions.
   * This method resolves offset value -1 to the latest and -2 to the
   * earliest Kafka seek position.
   */
  def fetchSpecificOffsets(
      partitionOffsets: Map[TopicPartition, Long]): Map[TopicPartition, Long] =
    runUninterruptibly {
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
          case (tp, KafkaOffsetRangeLimit.LATEST) =>
            consumer.seekToEnd(ju.Arrays.asList(tp))
          case (tp, KafkaOffsetRangeLimit.EARLIEST) =>
            consumer.seekToBeginning(ju.Arrays.asList(tp))
          case (tp, off) => consumer.seek(tp, off)
        }
        partitionOffsets.map {
          case (tp, _) => tp -> consumer.position(tp)
        }
      }
    }

  /**
   * Fetch the earliest offsets for the topic partitions that are indicated
   * in the [[ConsumerStrategy]].
   */
  def fetchEarliestOffsets(): Map[TopicPartition, Long] = runUninterruptibly {
    withRetriesWithoutInterrupt {
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
  }

  /**
   * Fetch the latest offsets for the topic partitions that are indicated
   * in the [[ConsumerStrategy]].
   */
  def fetchLatestOffsets(): Map[TopicPartition, Long] = runUninterruptibly {
    withRetriesWithoutInterrupt {
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
  }

  /**
   * Fetch the earliest offsets for specific topic partitions.
   * The return result may not contain some partitions if they are deleted.
   */
  def fetchEarliestOffsets(
      newPartitions: Seq[TopicPartition]): Map[TopicPartition, Long] = {
    if (newPartitions.isEmpty) {
      Map.empty[TopicPartition, Long]
    } else {
      runUninterruptibly {
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
  }

  /**
   * This method ensures that the closure is called in an [[UninterruptibleThread]].
   * This is required when communicating with the [[KafkaConsumer]]. In the case
   * of streaming queries, we are already running in an [[UninterruptibleThread]],
   * however for batch mode this is not the case.
   */
  private def runUninterruptibly[T](body: => T): T = {
    if (!Thread.currentThread.isInstanceOf[UninterruptibleThread]) {
      val future = Future {
        body
      }(execContext)
      ThreadUtils.awaitResult(future, Duration.Inf)
    } else {
      body
    }
  }

  /**
   * Helper function that does multiple retries on a body of code that returns offsets.
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
}
