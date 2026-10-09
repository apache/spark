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

package org.apache.spark.sql.kafka010.benchmark

import java.io.File
import java.time.{Duration => JavaDuration}
import java.util.{Locale, Properties, Timer, TimerTask}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}

import scala.concurrent.duration._

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, Producer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.execution.streaming.runtime.SerializedOffset
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.kafka010.{KafkaSourceOffset, KafkaTestUtils}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.util.Utils

/**
 * Shared Kafka-to-Kafka latency harness for stateful Real-Time Mode benchmarks.
 *
 * Each concrete benchmark runs the same logical query and deterministic input pattern twice. RTM
 * uses a five-minute batch duration and MBM uses an explicit zero-millisecond processing-time
 * trigger. Both runs use fresh Kafka topics and checkpoint directories, but otherwise have
 * identical Spark, Kafka, state-store, input-rate, and partition settings.
 *
 * Latency is measured in the same way as [[RTMKafkaKafkaBenchmark]]: the input Kafka record
 * timestamp is carried through the stateful operator in a `source-timestamp` header, then
 * subtracted from the output Kafka record timestamp. Both modes filter the first ten minutes
 * based on the source record timestamp before calculating latency percentiles.
 * As in the stateless benchmark, the output timestamp is Kafka CreateTime, so the measurement ends
 * when the sink producer creates the output record rather than when a consumer can observe it.
 *
 * The default 30-minute observation window gives RTM several five-minute batch boundaries. Short
 * development runs can override the observation and warm-up windows with
 * `SPARK_RTM_STATEFUL_BENCHMARK_OBSERVATION_SECONDS` and
 * `SPARK_RTM_STATEFUL_BENCHMARK_WARMUP_SECONDS`. The observation window must remain longer than
 * two five-minute RTM batches so that every run crosses at least two batch boundaries. A published
 * comparison should use the defaults.
 * Input production stops at the observation deadline. The query then drains to the recorded Kafka
 * end offsets, so delayed output from those inputs remains part of the latency distribution.
 * `SPARK_RTM_STATEFUL_BENCHMARK_MODE_ORDER` may be set to `MBM,RTM` to check for run-order bias.
 */
private[benchmark] abstract class RTMKafkaStatefulBenchmarkBase
    extends BenchmarkBase with Logging {

  protected def benchmarkName: String

  protected def benchmarkDetails: String

  /** Returns key, value, and sourceTimestampMs columns for the Kafka sink preparation. */
  protected def buildStatefulQuery(kafkaStream: DataFrame): DataFrame

  /** Deterministic input key for one-based record number `recordNumber`. */
  protected def inputKey(recordNumber: Long): String

  protected def inputValue(recordNumber: Long): String = recordNumber.toString

  /** Whether percentile ratios compare equivalent output populations in RTM and MBM. */
  protected def percentileRatiosAreComparable: Boolean = true

  protected def expectedSinkRecordCount(numSourceRecords: Long): Option[Long] = None

  protected def validateSinkOutput(
      isRtm: Boolean,
      numSourceRecords: Long,
      numSinkRecords: Long,
      kafkaSourceData: DataFrame,
      kafkaSinkData: DataFrame): Unit = {}

  // ----- Benchmark dimensions -----

  protected final val recordsPerSecond = 1000L

  private val rtmBatchDuration = 5.minutes
  private val observationWindow = durationFromEnv(
    "SPARK_RTM_STATEFUL_BENCHMARK_OBSERVATION_SECONDS",
    30.minutes)
  private val warmupWindow = durationFromEnv(
    "SPARK_RTM_STATEFUL_BENCHMARK_WARMUP_SECONDS",
    10.minutes,
    allowZero = true)
  private val maximumUnprocessedTail = 5.seconds
  private val expectedRecords = recordsPerSecond * observationWindow.toMillis / 1000L

  // ----- Spark topology -----

  private val sparkMaster = "local-cluster[3, 5, 2048]"
  private val numPartitions = 5
  private val shufflePartitions = 5

  // ----- Streaming, Kafka, and state-store settings -----

  private val streamingPollingDelayMs = 10
  private val kafkaFetchMaxWaitMs = "10"
  private val kafkaMaxPartitionFetchBytes = "10485760"
  private val kafkaBufferMemoryBytes = "67108864"
  private val rocksDBChangelogConf =
    "spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled"
  private val rocksDBTrackRowsConf =
    "spark.sql.streaming.stateStore.rocksdb.trackTotalNumberOfRows"

  // ----- Mutable state -----

  private val topicId = new AtomicInteger(0)
  private var spark: SparkSession = _
  private var testUtils: KafkaTestUtils = _

  private case class BenchmarkMode(name: String, isRtm: Boolean)

  private val modesByName = Map(
    "RTM" -> BenchmarkMode("RTM", isRtm = true),
    "MBM" -> BenchmarkMode("MBM", isRtm = false))

  private case class ModeRun(
      mode: BenchmarkMode,
      batchesAtObservationEnd: Long,
      recordsProduced: Long,
      inputTopic: String,
      outputTopic: String)

  private case class Latencies(
      mode: BenchmarkMode,
      batchesAtObservationEnd: Long,
      recordsProduced: Long,
      numSourceRecords: Long,
      numSinkRecords: Long,
      numSamples: Long,
      unprocessedTailMs: Long,
      p0: Double,
      p50: Double,
      p90: Double,
      p95: Double,
      p99: Double,
      p100: Double)

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    require(
      observationWindow > warmupWindow,
      s"observationWindow ($observationWindow) must exceed warmupWindow ($warmupWindow)")
    require(
      observationWindow > 2 * rtmBatchDuration,
      s"observationWindow ($observationWindow) must exceed two RTM batch durations " +
        s"(${2 * rtmBatchDuration})")
    testUtils = new KafkaTestUtils(Map.empty)
    try {
      testUtils.setup()
      spark = SparkSession.builder()
        .master(sparkMaster)
        .appName(this.getClass.getCanonicalName)
        .config(
          SQLConf.STATE_STORE_PROVIDER_CLASS.key,
          classOf[RocksDBStateStoreProvider].getName)
        .config(SQLConf.STATE_STORE_CHECKPOINT_FORMAT_VERSION.key, 2)
        .config(rocksDBChangelogConf, true)
        .config(rocksDBTrackRowsConf, false)
        .getOrCreate()
      runBenchmark(s"RTM vs MBM Kafka $benchmarkName latency") {
        benchmark()
      }
    } finally {
      cleanup()
    }
  }

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

  private def durationFromEnv(
      name: String,
      default: FiniteDuration,
      allowZero: Boolean = false): FiniteDuration = {
    sys.env.get(name).map { value =>
      val seconds = value.toLong
      require(
        if (allowZero) seconds >= 0 else seconds > 0,
        s"$name must be ${if (allowZero) "non-negative" else "positive"}: $value")
      seconds.seconds
    }.getOrElse(default)
  }

  private def modeOrder: Seq[BenchmarkMode] = {
    val names = sys.env
      .getOrElse("SPARK_RTM_STATEFUL_BENCHMARK_MODE_ORDER", "RTM,MBM")
      .split(",")
      .map(_.trim.toUpperCase(Locale.ROOT))
      .toSeq
    require(
      names.length == 2 && names.toSet == modesByName.keySet,
      "SPARK_RTM_STATEFUL_BENCHMARK_MODE_ORDER must be RTM,MBM or MBM,RTM")
    names.map(modesByName)
  }

  private def newTopic(): String = s"topic-${topicId.getAndIncrement()}"

  private def withTempDir[T](f: File => T): T = {
    val dir = Utils.createTempDir()
    try f(dir) finally Utils.deleteRecursively(dir)
  }

  private def benchmark(): Unit = {
    spark.conf.set(SQLConf.STREAMING_POLLING_DELAY.key, streamingPollingDelayMs)
    spark.conf.set(SQLConf.SHUFFLE_PARTITIONS.key, shufflePartitions)

    val configuredModeOrder = modeOrder
    // Finish both streaming runs before launching the batch queries that calculate percentiles.
    // This avoids warming Spark's batch SQL path between the first and second streaming run.
    val runs = configuredModeOrder.map(runOneMode)
    val recordCounts = runs.map(_.recordsProduced)
    val maximumCountDifference = math.max(100L, expectedRecords / 50L)
    require(
      recordCounts.max - recordCounts.min <= maximumCountDifference,
      s"RTM and MBM input counts differ by more than $maximumCountDifference: " +
        runs.map(run => s"${run.mode.name}=${run.recordsProduced}").mkString(", "))
    val results = runs.map(getLatencies)
    val sampleCounts = results.map(_.numSamples)
    val maximumSampleCountDifference = math.max(100L, sampleCounts.max / 50L)
    if (percentileRatiosAreComparable) {
      require(
        sampleCounts.max - sampleCounts.min <= maximumSampleCountDifference,
        s"RTM and MBM sample counts differ by more than $maximumSampleCountDifference: " +
          results.map(result => s"${result.mode.name}=${result.numSamples}").mkString(", "))
    }
    printComparison(configuredModeOrder, results)
  }

  private def runOneMode(mode: BenchmarkMode): ModeRun = withTempDir { checkpointDir =>
    val inputTopic = newTopic()
    val outputTopic = newTopic()
    testUtils.createTopic(inputTopic, partitions = numPartitions)
    testUtils.createTopic(outputTopic, partitions = numPartitions)

    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", inputTopic)
      .option("startingOffsets", "earliest")
      .option("kafka.fetch.max.wait.ms", kafkaFetchMaxWaitMs)
      .option("kafka.max.partition.fetch.bytes", kafkaMaxPartitionFetchBytes)
      .load()

    val outputWithHeaders = buildStatefulQuery(kafkaStream)
      .withColumn(
        "headers",
        array(
          struct(
            lit("source-timestamp").as("key"),
            col("sourceTimestampMs").cast("STRING").cast("BINARY").as("value"))))
      .select(
        col("key").cast("BINARY").as("key"),
        col("value").cast("BINARY").as("value"),
        col("headers"))

    val queryName =
      s"${mode.name.toLowerCase(Locale.ROOT)}-${getClass.getSimpleName.stripSuffix("$")}"
    val writer = outputWithHeaders.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("topic", outputTopic)
      .option("checkpointLocation", checkpointDir.getAbsolutePath)
      .option("kafka.buffer.memory", kafkaBufferMemoryBytes)
      .option("kafka.compression.type", "snappy")
      .outputMode("update")
      .queryName(queryName)

    val recordsProduced = new AtomicLong(0)
    val producerFailure = new AtomicReference[Throwable]()
    val stopProducer = new AtomicBoolean(false)
    var query: StreamingQuery = null
    var dataGenThread: Thread = null
    var batchesAtObservationEnd = 0L

    try {
      query = if (mode.isRtm) {
        writer.trigger(Trigger.RealTime(rtmBatchDuration)).start()
      } else {
        writer.trigger(Trigger.ProcessingTime(0L)).start()
      }

      dataGenThread = new Thread(
        () => {
          try {
            genData(mode.name, inputTopic, recordsProduced, producerFailure, stopProducer)
          } catch {
            case t: Throwable => producerFailure.compareAndSet(null, t)
          }
        },
        s"${mode.name.toLowerCase(Locale.ROOT)}-stateful-benchmark-data-generator")
      dataGenThread.setDaemon(true)
      dataGenThread.start()

      awaitObservationWindow(mode, query, producerFailure)
      batchesAtObservationEnd = Option(query.lastProgress).map(_.batchId + 1L).getOrElse(0L)
      stopProducer.set(true)
      joinDataGenerator(mode, dataGenThread)
      Option(producerFailure.get()).foreach { failure =>
        throw new RuntimeException(s"[${mode.name}] Kafka data generator failed", failure)
      }
      val expectedEndOffsets = testUtils.getLatestOffsets(Set(inputTopic))
      awaitSourceOffsets(mode, query, expectedEndOffsets, producerFailure)
    } finally {
      stopProducer.set(true)
      try {
        joinDataGenerator(mode, dataGenThread)
      } finally {
        if (query != null) {
          query.stop()
        }
      }
    }

    Option(producerFailure.get()).foreach { failure =>
      throw new RuntimeException(s"[${mode.name}] Kafka data generator failed", failure)
    }

    val minimumRecords = expectedRecords * 95L / 100L
    require(
      recordsProduced.get() >= minimumRecords,
      s"[${mode.name}] produced only ${recordsProduced.get()} records; expected at least " +
        s"$minimumRecords over $observationWindow")
    val minimumCompletedRtmBatches =
      observationWindow.toMillis / rtmBatchDuration.toMillis - 1L
    if (mode.isRtm && minimumCompletedRtmBatches > 0) {
      require(
        batchesAtObservationEnd >= minimumCompletedRtmBatches,
        s"[RTM] completed only $batchesAtObservationEnd batch(es) over $observationWindow; " +
          s"expected at least $minimumCompletedRtmBatches")
    }

    logInfo(s"[${mode.name}] completed $batchesAtObservationEnd batch(es) during the " +
      s"observation window and produced ${recordsProduced.get()} records")
    ModeRun(
      mode,
      batchesAtObservationEnd,
      recordsProduced.get(),
      inputTopic,
      outputTopic)
  }

  private def joinDataGenerator(mode: BenchmarkMode, dataGenThread: Thread): Unit = {
    if (dataGenThread != null && dataGenThread.isAlive) {
      dataGenThread.join(30.seconds.toMillis)
      if (dataGenThread.isAlive) {
        dataGenThread.interrupt()
        dataGenThread.join(5.seconds.toMillis)
      }
      require(
        !dataGenThread.isAlive,
        s"[${mode.name}] Kafka data generator did not stop within 35 seconds")
    }
  }

  private def querySourceOffsets(query: StreamingQuery): Option[Map[TopicPartition, Long]] = {
    Option(query.lastProgress).flatMap { progress =>
      progress.sources.headOption.flatMap { source =>
        Option(source.endOffset)
          .filter(offset => offset.nonEmpty && offset != "null")
          .map(offset => KafkaSourceOffset(SerializedOffset(offset)).partitionToOffsets)
      }
    }
  }

  private def requireSourceOffsets(
      mode: BenchmarkMode,
      query: StreamingQuery,
      expectedEndOffsets: Map[TopicPartition, Long]): Unit = {
    val actualEndOffsets = querySourceOffsets(query)
    require(
      actualEndOffsets.contains(expectedEndOffsets),
      s"[${mode.name}] query did not consume all input offsets: " +
        s"expected=$expectedEndOffsets actual=$actualEndOffsets")
  }

  private def awaitSourceOffsets(
      mode: BenchmarkMode,
      query: StreamingQuery,
      expectedEndOffsets: Map[TopicPartition, Long],
      producerFailure: AtomicReference[Throwable]): Unit = {
    val timeout = if (mode.isRtm) 2 * rtmBatchDuration + 1.minute else 1.minute
    val deadlineNs = System.nanoTime() + timeout.toNanos
    var terminatedEarly = false
    while (
        !terminatedEarly &&
        producerFailure.get() == null &&
        !querySourceOffsets(query).contains(expectedEndOffsets) &&
        System.nanoTime() < deadlineNs) {
      terminatedEarly = query.awaitTermination(1000L)
    }
    if (terminatedEarly) {
      throw new RuntimeException(s"[${mode.name}] query terminated while draining input")
    }
    Option(producerFailure.get()).foreach { failure =>
      throw new RuntimeException(s"[${mode.name}] Kafka data generator failed", failure)
    }
    requireSourceOffsets(mode, query, expectedEndOffsets)
  }

  private def awaitObservationWindow(
      mode: BenchmarkMode,
      query: StreamingQuery,
      producerFailure: AtomicReference[Throwable]): Unit = {
    val deadlineNs = System.nanoTime() + observationWindow.toNanos
    var terminatedEarly = false
    while (!terminatedEarly && producerFailure.get() == null && System.nanoTime() < deadlineNs) {
      val remainingMs = math.max(1L, (deadlineNs - System.nanoTime()).nanos.toMillis)
      terminatedEarly = query.awaitTermination(math.min(remainingMs, 1000L))
    }
    if (terminatedEarly) {
      throw new RuntimeException(
        s"[${mode.name}] query terminated before the $observationWindow observation window")
    }
    Option(producerFailure.get()).foreach { failure =>
      throw new RuntimeException(s"[${mode.name}] Kafka data generator failed", failure)
    }
  }

  private def genData(
      mode: String,
      topicName: String,
      totalSuccess: AtomicLong,
      producerFailure: AtomicReference[Throwable],
      stopProducer: AtomicBoolean): Unit = {
    logInfo(
      s"[$mode] producing to ${testUtils.brokerAddress} topic $topicName at " +
        s"$recordsPerSecond records/sec")

    val props = new Properties()
    props.put("bootstrap.servers", testUtils.brokerAddress)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer: Producer[String, String] = new KafkaProducer[String, String](props)
    val recentSuccess = new AtomicLong(0)
    val timer = new Timer(true)

    try {
      timer.scheduleAtFixedRate(
        new TimerTask() {
          override def run(): Unit = {
            logInfo(s"[$mode] throughput: ${recentSuccess.getAndSet(0)} requests/sec")
          }
        },
        1000,
        1000)

      var recordNumber = 0L
      val startTime = System.nanoTime()
      val delayNs = (1.second.toNanos / recordsPerSecond)
      var nextDeadline = startTime + delayNs
      while (!stopProducer.get() && producerFailure.get() == null) {
        var currentTime = System.nanoTime()
        if (currentTime >= nextDeadline) {
          recordNumber += 1
          nextDeadline = startTime + (recordNumber + 1L) * delayNs
          producer.send(
            new ProducerRecord[String, String](
              topicName,
              inputKey(recordNumber),
              inputValue(recordNumber)),
            new Callback {
              override def onCompletion(metadata: RecordMetadata, error: Exception): Unit = {
                if (error != null) {
                  producerFailure.compareAndSet(null, error)
                } else {
                  totalSuccess.incrementAndGet()
                  recentSuccess.incrementAndGet()
                }
              }
            })
          currentTime = System.nanoTime()
        }
        val sleepTimeNs = math.max(0L, nextDeadline - currentTime)
        if (sleepTimeNs > 0) {
          val sleepTimeMs = sleepTimeNs.nanos.toMillis
          val sleepTimeNanos = (sleepTimeNs - sleepTimeMs.millis.toNanos).toInt
          Thread.sleep(sleepTimeMs, sleepTimeNanos)
        }
      }
    } catch {
      case _: InterruptedException =>
    } finally {
      timer.cancel()
      producer.close(JavaDuration.ofSeconds(20))
    }
  }

  private def getLatencies(run: ModeRun): Latencies = {
    val kafkaSourceData = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", run.inputTopic)
      .option("startingOffsets", "earliest")
      .load()
      .select(
        col("key"),
        toUnixMillis(col("timestamp")).as("source-timestamp"))

    val sourceStats = kafkaSourceData
      .agg(
        count(lit(1)).as("num-source-records"),
        min("source-timestamp").as("minimum-source-timestamp"),
        max("source-timestamp").as("maximum-source-timestamp"))
      .collect()(0)
    val numSourceRecords = sourceStats.getLong(0)
    require(
      numSourceRecords > 0,
      s"[${run.mode.name}] no records found in Kafka input topic ${run.inputTopic}")
    require(
      numSourceRecords == run.recordsProduced,
      s"[${run.mode.name}] Kafka input contains $numSourceRecords records, but the producer " +
        s"acknowledged ${run.recordsProduced}")
    val minimumSourceTimestamp = sourceStats.getLong(1)
    val maximumSourceTimestamp = sourceStats.getLong(2)

    val kafkaSinkData = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", run.outputTopic)
      .option("startingOffsets", "earliest")
      .option("includeHeaders", "true")
      .load()
      .withColumn("headers-map", map_from_entries(col("headers")))
      .withColumn(
        "source-timestamp",
        col("headers-map.source-timestamp").cast("STRING").cast("BIGINT"))
      .withColumn("sink-timestamp", toUnixMillis(col("timestamp")))

    val sinkStats = kafkaSinkData
      .agg(
        count(lit(1)).as("num-sink-records"),
        count(col("source-timestamp")).as("num-timestamped-sink-records"),
        max("source-timestamp").as("maximum-processed-source-timestamp"))
      .collect()(0)
    val numRecordsInSink = sinkStats.getLong(0)
    if (numRecordsInSink == 0) {
      throw new RuntimeException(
        s"[${run.mode.name}] no results found in Kafka sink topic ${run.outputTopic}")
    }
    val numTimestampedSinkRecords = sinkStats.getLong(1)
    require(
      numTimestampedSinkRecords == numRecordsInSink,
      s"[${run.mode.name}] $numTimestampedSinkRecords of $numRecordsInSink sink rows contain " +
        "a source timestamp")
    expectedSinkRecordCount(numSourceRecords).foreach { expectedSinkRecords =>
      require(
        numRecordsInSink == expectedSinkRecords,
        s"[${run.mode.name}] sink contains $numRecordsInSink rows for $numSourceRecords inputs; " +
          s"expected exactly $expectedSinkRecords")
    }
    val maximumProcessedSourceTimestamp = sinkStats.getLong(2)
    val unprocessedTailMs = maximumSourceTimestamp - maximumProcessedSourceTimestamp
    require(
      unprocessedTailMs >= 0 && unprocessedTailMs <= maximumUnprocessedTail.toMillis,
      s"[${run.mode.name}] newest sink output trails the newest input by " +
        s"$unprocessedTailMs ms; expected at most ${maximumUnprocessedTail.toMillis} ms")
    validateSinkOutput(
      run.mode.isRtm,
      numSourceRecords,
      numRecordsInSink,
      kafkaSourceData,
      kafkaSinkData)

    val filteredSink = if (warmupWindow.toMillis == 0) {
      kafkaSinkData
    } else {
      kafkaSinkData.filter(
        col("source-timestamp") - minimumSourceTimestamp > warmupWindow.toMillis)
    }
    val numSamples = filteredSink.count()
    if (numSamples == 0) {
      throw new RuntimeException(
        s"[${run.mode.name}] no records remained after filtering $warmupWindow of warm-up")
    }

    val percentiles = filteredSink
      .withColumn("e2e-latency", col("sink-timestamp") - col("source-timestamp"))
      .selectExpr(
        "transform(percentile_approx(`e2e-latency`, " +
          "array(0.0, 0.5, 0.9, 0.95, 0.99, 1.0), 10000), " +
          "x -> CAST(x AS DOUBLE)) AS p")
      .collect()(0)
      .getSeq[Double](0)

    Latencies(
      run.mode,
      run.batchesAtObservationEnd,
      run.recordsProduced,
      numSourceRecords,
      numRecordsInSink,
      numSamples,
      unprocessedTailMs,
      percentiles(0),
      percentiles(1),
      percentiles(2),
      percentiles(3),
      percentiles(4),
      percentiles(5))
  }

  private def printComparison(
      configuredModeOrder: Seq[BenchmarkMode],
      unorderedResults: Seq[Latencies]): Unit = {
    val resultsByMode = unorderedResults.map(result => result.mode.name -> result).toMap
    val results = Seq("RTM", "MBM").map(resultsByMode)
    val envHeader = s"${Benchmark.getJVMOSInfo()}\n${Benchmark.getProcessorName()}\n"

    val sb = new StringBuilder
    sb.append(envHeader)
    sb.append(s"\nKafka-to-Kafka $benchmarkName e2e latency in milliseconds\n")
    sb.append(s"RTM trigger=RealTime(${rtmBatchDuration.toMinutes}m) ")
    sb.append("MBM trigger=ProcessingTime(0ms)\n")
    sb.append(s"modeOrder=${configuredModeOrder.map(_.name).mkString(",")} ")
    sb.append(s"observationWindow=${observationWindow.toSeconds}s ")
    sb.append(s"warmupFiltered=${warmupWindow.toSeconds}s ")
    sb.append(s"recordsPerSecond=$recordsPerSecond\n")
    sb.append(s"master=$sparkMaster inputPartitions=$numPartitions ")
    sb.append(s"shufflePartitions=$shufflePartitions\n")
    sb.append("stateStore=RocksDB checkpointFormatVersion=2 changelogCheckpointing=true ")
    sb.append("trackTotalNumberOfRows=false\n")
    sb.append(s"workload=$benchmarkDetails\n")
    sb.append('\n')

    val header = f"${"Mode"}%-6s ${"obsBatch"}%8s ${"produced"}%10s ${"sourceRows"}%10s " +
      f"${"sinkRows"}%10s ${"samples"}%10s ${"tailMs"}%8s ${"p0"}%10s ${"p50"}%10s " +
      f"${"p90"}%10s ${"p95"}%10s ${"p99"}%10s ${"p100"}%10s"
    sb.append(header).append('\n')
    sb.append("-" * header.length).append('\n')
    results.foreach { result =>
      sb.append(
        f"${result.mode.name}%-6s ${result.batchesAtObservationEnd}%8d " +
          f"${result.recordsProduced}%10d ${result.numSourceRecords}%10d " +
          f"${result.numSinkRecords}%10d ${result.numSamples}%10d " +
          f"${result.unprocessedTailMs}%8d " +
          f"${result.p0}%10.1f ${result.p50}%10.1f ${result.p90}%10.1f " +
          f"${result.p95}%10.1f ${result.p99}%10.1f ${result.p100}%10.1f\n")
    }

    if (percentileRatiosAreComparable) {
      val rtm = resultsByMode("RTM")
      val mbm = resultsByMode("MBM")
      def ratio(mbmValue: Double, rtmValue: Double): String = {
        if (rtmValue <= 0) "n/a" else f"${mbmValue / rtmValue}%.2fx"
      }
      sb.append("\nMBM / RTM ratio over each mode's emitted samples ")
      sb.append("(>1 means RTM has lower latency)\n")
      sb.append(
        f"${"ratio"}%-6s ${""}%8s ${""}%10s ${""}%10s ${""}%10s ${""}%10s " +
          f"${""}%8s ${ratio(mbm.p0, rtm.p0)}%10s ${ratio(mbm.p50, rtm.p50)}%10s " +
          f"${ratio(mbm.p90, rtm.p90)}%10s ${ratio(mbm.p95, rtm.p95)}%10s " +
          f"${ratio(mbm.p99, rtm.p99)}%10s ${ratio(mbm.p100, rtm.p100)}%10s")
    } else {
      sb.append("\nPercentile ratio omitted: RTM and MBM may emit different native sample ")
      sb.append("populations.")
    }

    val message = sb.toString()
    output match {
      case Some(out) =>
        out.write(message.getBytes)
        // Always echo the comparison when BenchmarkBase is writing a result file.
        // scalastyle:off println
        println(message)
        // scalastyle:on println
      case None => logInfo("\n" + message)
    }
  }

  protected final def toUnixMillis(column: Column): Column = {
    (column.cast("timestamp").cast("double") * 1000).cast("long")
  }
}
