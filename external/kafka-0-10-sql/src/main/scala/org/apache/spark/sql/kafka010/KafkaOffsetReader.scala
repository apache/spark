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
import java.util.concurrent.Executors

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
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
    val driverKafkaParams: ju.Map[String, Object],
    readerOptions: Map[String, String],
    driverGroupIdPrefix: String) extends Logging {
  /**
   * Used to ensure execute fetch operations execute in an UninterruptibleThread
   */
  val kafkaReaderThread = Executors.newSingleThreadExecutor((r: Runnable) => {
    val t = new UninterruptibleThread("Kafka Offset Reader") {
      override def run(): Unit = {
        r.run()
      }
    }
    t.setDaemon(true)
    t
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
  @volatile protected var _consumer: Consumer[Array[Byte], Array[Byte]] = null

  protected def consumer: Consumer[Array[Byte], Array[Byte]] = synchronized {
    assert(Thread.currentThread().isInstanceOf[UninterruptibleThread])
    if (_consumer == null) {
      val newKafkaParams = new ju.HashMap[String, Object](driverKafkaParams)
      if (driverKafkaParams.get(ConsumerConfig.GROUP_ID_CONFIG) == null) {
        newKafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, nextGroupId())
      }
      _consumer = consumerStrategy.createConsumer(newKafkaParams)
    }
    _consumer
  }

  private val maxOffsetFetchAttempts =
    readerOptions.getOrElse(KafkaSourceProvider.FETCH_OFFSET_NUM_RETRY, "3").toInt

  private val offsetFetchAttemptIntervalMs =
    readerOptions.getOrElse(KafkaSourceProvider.FETCH_OFFSET_RETRY_INTERVAL_MS, "1000").toLong

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
    if (_consumer != null) runUninterruptibly { stopConsumer() }
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
   *
   * @param partitionOffsets the specific offsets to resolve
   * @param reportDataLoss callback to either report or log data loss depending on setting
   */
  def fetchSpecificOffsets(
      partitionOffsets: Map[TopicPartition, Long],
      reportDataLoss: String => Unit): KafkaSourceOffset = {
    val fetched = runUninterruptibly {
      withRetriesWithoutInterrupt {
        // Poll to get the latest assigned partitions
        consumer.poll(0)
        val partitions = consumer.assignment()

        // Call `position` to wait until the potential offset request triggered by `poll(0)` is
        // done. This is a workaround for KAFKA-7703, which an async `seekToBeginning` triggered by
        // `poll(0)` may reset offsets that should have been set by another request.
        partitions.asScala.map(p => p -> consumer.position(p)).foreach(_ => {})

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

    partitionOffsets.foreach {
      case (tp, off) if off != KafkaOffsetRangeLimit.LATEST &&
        off != KafkaOffsetRangeLimit.EARLIEST =>
        if (fetched(tp) != off) {
          reportDataLoss(
            s"startingOffsets for $tp was $off but consumer reset to ${fetched(tp)}")
        }
      case _ =>
        // no real way to check that beginning or end is reasonable
    }
    KafkaSourceOffset(fetched)
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
   *
   * Kafka may return earliest offsets when we are requesting latest offsets if `poll` is called
   * right before `seekToEnd` (KAFKA-7703). As a workaround, we will call `position` right after
   * `poll` to wait until the potential offset request triggered by `poll(0)` is done.
   *
   * In addition, to avoid other unknown issues, we also use the given `knownOffsets` to audit the
   * latest offsets returned by Kafka. If we find some incorrect offsets (a latest offset is less
   * than an offset in `knownOffsets`), we will retry at most `maxOffsetFetchAttempts` times. When
   * a topic is recreated, the latest offsets may be less than offsets in `knownOffsets`. We cannot
   * distinguish this with KAFKA-7703, so we just return whatever we get from Kafka after retrying.
   */
  def fetchLatestOffsets(
      knownOffsets: Option[PartitionOffsetMap]): PartitionOffsetMap = runUninterruptibly {
    withRetriesWithoutInterrupt {
      // Poll to get the latest assigned partitions
      consumer.poll(0)
      val partitions = consumer.assignment()

      // Call `position` to wait until the potential offset request triggered by `poll(0)` is
      // done. This is a workaround for KAFKA-7703, which an async `seekToBeginning` triggered by
      // `poll(0)` may reset offsets that should have been set by another request.
      partitions.asScala.map(p => p -> consumer.position(p)).foreach(_ => {})

      consumer.pause(partitions)
      logDebug(s"Partitions assigned to consumer: $partitions. Seeking to the end.")

      if (knownOffsets.isEmpty) {
        consumer.seekToEnd(partitions)
        partitions.asScala.map(p => p -> consumer.position(p)).toMap
      } else {
        var partitionOffsets: PartitionOffsetMap = Map.empty

        /**
         * Compare `knownOffsets` and `partitionOffsets`. Returns all partitions that have incorrect
         * latest offset (offset in `knownOffsets` is great than the one in `partitionOffsets`).
         */
        def findIncorrectOffsets(): Seq[(TopicPartition, Long, Long)] = {
          var incorrectOffsets = ArrayBuffer[(TopicPartition, Long, Long)]()
          partitionOffsets.foreach { case (tp, offset) =>
            knownOffsets.foreach(_.get(tp).foreach { knownOffset =>
              if (knownOffset > offset) {
                val incorrectOffset = (tp, knownOffset, offset)
                incorrectOffsets += incorrectOffset
              }
            })
          }
          incorrectOffsets
        }

        // Retry to fetch latest offsets when detecting incorrect offsets. We don't use
        // `withRetriesWithoutInterrupt` to retry because:
        //
        // - `withRetriesWithoutInterrupt` will reset the consumer for each attempt but a fresh
        //    consumer has a much bigger chance to hit KAFKA-7703.
        // - Avoid calling `consumer.poll(0)` which may cause KAFKA-7703.
        var incorrectOffsets: Seq[(TopicPartition, Long, Long)] = Nil
        var attempt = 0
        do {
          consumer.seekToEnd(partitions)
          partitionOffsets = partitions.asScala.map(p => p -> consumer.position(p)).toMap
          attempt += 1

          incorrectOffsets = findIncorrectOffsets()
          if (incorrectOffsets.nonEmpty) {
            logWarning("Found incorrect offsets in some partitions " +
              s"(partition, previous offset, fetched offset): $incorrectOffsets")
            if (attempt < maxOffsetFetchAttempts) {
              logWarning("Retrying to fetch latest offsets because of incorrect offsets")
              Thread.sleep(offsetFetchAttemptIntervalMs)
            }
          }
        } while (incorrectOffsets.nonEmpty && attempt < maxOffsetFetchAttempts)

        logDebug(s"Got latest offsets for partition : $partitionOffsets")
        partitionOffsets
      }
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

  private def stopConsumer(): Unit = synchronized {
    assert(Thread.currentThread().isInstanceOf[UninterruptibleThread])
    if (_consumer != null) _consumer.close()
  }

  private def resetConsumer(): Unit = synchronized {
    stopConsumer()
    _consumer = null  // will automatically get reinitialized again
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
