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

import java.nio.file.Files
import java.util.{Properties, Timer, TimerTask}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.concurrent.duration._

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, Producer, ProducerRecord, RecordMetadata}

import org.apache.spark.benchmark.BenchmarkBase
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.execution.streaming.RealTimeTrigger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamingQueryListener

/**
 * Stateless Kafka-to-Kafka RTM benchmark. Reads from an input Kafka topic, applies a
 * stateless transformation, and writes results to an output Kafka topic using
 * [[RealTimeTrigger]]. After the run it reports e2e latency percentiles.
 *
 * The benchmark spins up a real local-cluster Spark context and a live embedded Kafka
 * broker, so a single run takes several minutes.
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark sql test jar> <spark sql kafka 0-10 test jar>
 *   2. build/sbt "sql-kafka-0-10/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql-kafka-0-10/Test/runMain <this class>"
 *      Results will be written to:
 *      "connector/kafka-0-10-sql/benchmarks/RTMKafkaKafkaBenchmark-results.txt".
 * }}}
 */
object RTMKafkaKafkaBenchmark extends BenchmarkBase with Logging {

  private val topicId = new AtomicInteger(0)
  private var spark: SparkSession = _
  private var testUtils: KafkaTestUtils = _

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    // BenchmarkBase.main does not wrap this call in try/finally, so we must own
    // teardown ourselves — partial setup, a timeout, or a getLatencies failure
    // would otherwise leak the embedded Kafka broker and local-cluster workers.
    testUtils = new KafkaTestUtils(Map.empty)
    try {
      testUtils.setup()
      spark = SparkSession.builder()
        .master("local-cluster[3, 5, 1024]")
        .appName(this.getClass.getCanonicalName)
        .getOrCreate()
      runBenchmark("RTM stateless kafka-to-kafka") {
        benchmark(60.seconds.toMillis, 4)
      }
    } finally {
      cleanup()
    }
  }

  /**
   * Idempotent cleanup of the Spark session and embedded Kafka broker. Safe to call
   * after any combination of partial setup, normal completion, or exception.
   */
  private def cleanup(): Unit = {
    if (spark != null) {
      try {
        spark.stop()
      } catch {
        case t: Throwable => logWarning("Failed to stop SparkSession during cleanup", t)
      }
      spark = null
    }
    if (testUtils != null) {
      try {
        testUtils.teardown()
      } catch {
        case t: Throwable => logWarning("Failed to teardown KafkaTestUtils during cleanup", t)
      }
      testUtils = null
    }
  }

  private def newTopic(): String = s"topic-${topicId.getAndIncrement()}"

  def benchmark(longRunningBatchDurationMs: Long, numBatches: Long): Unit = {
    val inputTopic = newTopic()
    testUtils.createTopic(inputTopic, partitions = 5)

    val outputTopic = newTopic()
    testUtils.createTopic(outputTopic, partitions = 5)

    spark.conf.set(SQLConf.STREAMING_POLLING_DELAY.key, 10)

    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", inputTopic)
      .option("kafka.fetch.max.wait.ms", "10")
      .option("kafka.max.partition.fetch.bytes", "10485760") // 10MB
      .load()

    val currentTimestampUDF = udf(() => System.currentTimeMillis())

    val streamWithObserved = kafkaStream
      .withColumn("value", base64(col("value")))
      .withColumn(
        "headers",
        array(
          struct(
            lit("source-timestamp") as "key",
            unix_millis(col("timestamp")).cast("STRING").cast("BINARY") as "value")))
      .withColumn("temp-timestamp", currentTimestampUDF())
      .withColumn(
        "latency",
        col("temp-timestamp").cast("long") - unix_millis(col("timestamp")).cast("long"))
      .observe(
        name = "observedLatency",
        avg(col("latency")).as("avg"),
        max(col("latency")).as("max"),
        percentile_approx(col("latency"), lit(0.99), lit(10000)).as("p99"),
        percentile_approx(col("latency"), lit(0.5), lit(10000)).as("p50"))
      .drop(col("latency"))
      .drop(col("temp-timestamp"))
      .drop(col("timestamp"))

    val query = streamWithObserved.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("topic", outputTopic)
      .option("checkpointLocation", Files.createTempDirectory("rtm-benchmark").toString)
      .option("kafka.buffer.memory", "67108864") // 64MB
      .option("kafka.compression.type", "snappy")
      .outputMode("update")
      .queryName("rtm-kafka-kafka")
      .trigger(RealTimeTrigger.apply(s"${longRunningBatchDurationMs} milliseconds"))
      .start()

    val dataGenThread = new Thread(() => {
      genData(testUtils.brokerAddress, inputTopic, 1000)
    })
    dataGenThread.start()

    val latch = new CountDownLatch(1)
    val listener = new StreamingQueryListener {
      override def onQueryStarted(
          event: StreamingQueryListener.QueryStartedEvent): Unit = {}

      override def onQueryTerminated(
          event: StreamingQueryListener.QueryTerminatedEvent): Unit = {}

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        if (event.progress.batchId == numBatches - 1) {
          latch.countDown()
        }
      }
    }
    spark.streams.addListener(listener)

    val timeoutMs = numBatches * longRunningBatchDurationMs * 2 + 60 * 1000
    val completed = try {
      latch.await(timeoutMs, TimeUnit.MILLISECONDS)
    } finally {
      spark.streams.removeListener(listener)
      query.stop()
      dataGenThread.interrupt()
      dataGenThread.join(30 * 1000)
    }
    if (!completed) {
      throw new RuntimeException(
        s"Benchmark timed out waiting for $numBatches batches to complete after ${timeoutMs}ms.")
    }

    getLatencies(longRunningBatchDurationMs, numBatches, outputTopic)
  }

  private def genData(url: String, topicName: String, throughput: Long): Unit = {
    logInfo(s"Producing to $url topic $topicName at $throughput records / sec")

    val props: Properties = new Properties()
    props.put("bootstrap.servers", url)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer: Producer[String, String] = new KafkaProducer[String, String](props)
    val success = new AtomicLong(0)
    val timer = new Timer()

    try {
      timer.scheduleAtFixedRate(
        new TimerTask() {
          override def run(): Unit = {
            logInfo("Throughput: " + success.getAndSet(0) + " requests/sec")
          }
        },
        1000,
        1000
      )

      var i = 0L
      val startTime = System.nanoTime
      val delay = (Math.pow(10, 9) / throughput).asInstanceOf[Long]
      var nextDeadline = startTime + delay
      while (true) {
        var currentTime = System.nanoTime
        if (currentTime >= nextDeadline) {
          i += 1
          nextDeadline = startTime + (i * delay)
          producer.send(
            new ProducerRecord[String, String](
              topicName,
              java.lang.Long.toString(i),
              java.lang.Long.toString(System.currentTimeMillis())
            ),
            new Callback {
              override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
                if (e != null) {
                  logError("Got exception producing to kafka", e)
                } else {
                  success.incrementAndGet()
                }
              }
            }
          )
          currentTime = System.nanoTime

          val sleepTimeNs =
            if ((nextDeadline - currentTime) > 0) nextDeadline - currentTime
            else 0
          if (sleepTimeNs > 0) {
            val sleepTimeMs = sleepTimeNs.nanoseconds.toMillis
            val sleepTimeNano = (sleepTimeNs - sleepTimeMs.milliseconds.toNanos).toInt
            Thread.sleep(sleepTimeMs, sleepTimeNano)
          }
        }
      }
    } catch {
      case _: InterruptedException => // expected on shutdown
    } finally {
      timer.cancel()
      producer.close()
    }
  }

  private def printLatenciesTable(viewName: String, colName: String): Unit = {
    val results = spark.sqlContext
      .sql(s"""SELECT percentile_approx($colName, Array(0.0, 0.5, 0.9, 0.95, 0.99, 1.0), 10000)
              | FROM $viewName""".stripMargin)
      .collect()(0)(0)

    if (results == null) {
      throw new RuntimeException(
        s"No results found in table $viewName when trying to print latency for $colName. " +
          s"The benchmark may need more batches or a longer duration to produce enough data."
      )
    }

    val latencies = results.asInstanceOf[scala.collection.Seq[_]]

    val percentiles = Array("p0", "p50", "p90", "p95", "p99", "p100")
    val latenciesTable = percentiles
      .zip(latencies)
      .map(pair => pair._1 + ": " + pair._2)
      .mkString("\n")

    val message =
      s"Kafka to kafka query ${colName} in milliseconds is\n" + latenciesTable + "\n"

    output match {
      case Some(out) => out.write(message.getBytes)
      case None => logInfo("\n" + message)
    }
  }

  private def getLatencies(
      longRunningBatchDurationMs: Long,
      numBatches: Long,
      outputTopic: String): Unit = {
    val kafkaSinkData = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", outputTopic)
      .option("includeHeaders", "true")
      .load()
      .withColumn("headers-map", map_from_entries(col("headers")))
      .withColumn("source-timestamp",
        col("headers-map.source-timestamp").cast("STRING").cast("BIGINT"))
      .withColumn("sink-timestamp", unix_millis(col("timestamp")))

    val numRecordsInSink = kafkaSinkData.count()
    val minimumSourceTimestamp =
      kafkaSinkData.agg(min("source-timestamp")).collect()(0)(0).asInstanceOf[Long]

    val numBatchesToFilter = 2
    val timeFilterThresholdMs = longRunningBatchDurationMs * numBatchesToFilter
    val filteredSink = kafkaSinkData
      .withColumn("time", col("source-timestamp") - minimumSourceTimestamp)
      .filter(col("time") > timeFilterThresholdMs)

    if (filteredSink.count() == 0) {
      if (numRecordsInSink > 0) {
        throw new RuntimeException(
          s"There were ${numRecordsInSink} records in the Kafka sink topic $outputTopic, " +
            s"but none remained after filtering the first ${numBatchesToFilter} batch(es) " +
            s"(${timeFilterThresholdMs} ms). Run more batches (current: ${numBatches})."
        )
      } else {
        throw new RuntimeException(
          s"No results were found in the Kafka sink topic $outputTopic. " +
            s"The query may not have produced results or the sink topic was incorrect."
        )
      }
    }

    val sinkWithLatencies = filteredSink
      .withColumn("e2e_latency", col("sink-timestamp") - col("source-timestamp"))
    sinkWithLatencies.createOrReplaceTempView("sink_with_latencies")

    printLatenciesTable("sink_with_latencies", "e2e_latency")
  }

  private def unix_millis(column: Column): Column = {
    (column.cast("timestamp").cast("double") * 1000).cast("long")
  }
}
