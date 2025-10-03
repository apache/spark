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

package org.apache.spark.sql.streaming.sources

import java.util

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.connector.catalog.{SessionConfigSupport, SupportsRead, SupportsWrite, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory, Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns, SupportsPushDownV2Filters}
import org.apache.spark.sql.connector.read.streaming.{ContinuousPartitionReaderFactory, ContinuousStream, MicroBatchStream, Offset, PartitionOffset}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, PhysicalWriteInfo, Write, WriteBuilder, WriterCommitMessage}
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2ScanRelation
import org.apache.spark.sql.execution.streaming.ContinuousTrigger
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.execution.streaming.runtime.{RateStreamOffset, StreamingQueryWrapper}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.SimpleTableProvider
import org.apache.spark.sql.sources.{DataSourceRegister, Filter, StreamSinkProvider}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, StreamTest, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.tags.SlowSQLTest
import org.apache.spark.util.Utils

@SlowSQLTest
class FakeDataStream(
    val requiredSchema: StructType = null,
    val pushedV1Filters: Array[Filter] = Array.empty,
    val predicates: Array[Predicate] = Array.empty) extends MicroBatchStream with ContinuousStream {
  override def deserializeOffset(json: String): Offset = RateStreamOffset(Map())
  override def commit(end: Offset): Unit = {}
  override def stop(): Unit = {}
  override def initialOffset(): Offset = RateStreamOffset(Map())
  override def latestOffset(): Offset = RateStreamOffset(Map())
  override def mergeOffsets(offsets: Array[PartitionOffset]): Offset = RateStreamOffset(Map())
  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    throw new IllegalStateException("fake source - cannot actually read")
  }
  override def planInputPartitions(start: Offset): Array[InputPartition] = {
    throw new IllegalStateException("fake source - cannot actually read")
  }
  override def createReaderFactory(): PartitionReaderFactory = {
    throw new IllegalStateException("fake source - cannot actually read")
  }
  override def createContinuousReaderFactory(): ContinuousPartitionReaderFactory = {
    throw new IllegalStateException("fake source - cannot actually read")
  }
}

class PushdownStreamingScanBuilderV1
  extends ScanBuilder
    with Scan
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns {

  import org.apache.spark.sql.sources._
  import org.apache.spark.sql.types._

  private val fullSchema = StructType(Seq(
    StructField("i", IntegerType),
    StructField("j", IntegerType)))
  var requiredSchema: StructType = fullSchema
  var pushedV1Filters: Array[Filter] = Array.empty

  override def pruneColumns(schema: StructType): Unit = {
    requiredSchema = schema
  }

  override def readSchema(): StructType = requiredSchema

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val (supported, _) = filters.partition {
      case GreaterThan("i", _: Int) => true
      case _ => false
    }
    this.pushedV1Filters = supported
    filters
  }

  override def pushedFilters(): Array[Filter] = pushedV1Filters

  override def build(): Scan = this

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream =
    new FakeDataStream(requiredSchema, pushedV1Filters, Array.empty)
  override def toContinuousStream(checkpointLocation: String): ContinuousStream =
    new FakeDataStream(requiredSchema, pushedV1Filters, Array.empty)
}

class PushdownStreamingScanBuilderV2
  extends ScanBuilder
    with Scan
    with SupportsPushDownV2Filters
    with SupportsPushDownRequiredColumns {

  var requiredSchema: StructType = PushdownStreamingSourceV2.fullSchema
  var predicates: Array[Predicate] = Array.empty

  override def pruneColumns(schema: StructType): Unit = {
    requiredSchema = schema
  }

  override def readSchema(): StructType = requiredSchema

  override def pushPredicates(predicates: Array[Predicate]): Array[Predicate] = {
    val (supported, _) = predicates.partition(_.name() == "=")
    this.predicates = supported
    predicates
  }

  override def pushedPredicates(): Array[Predicate] = predicates

  override def build(): Scan = this

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream =
    new FakeDataStream(requiredSchema, Array.empty, predicates)
  override def toContinuousStream(checkpointLocation: String): ContinuousStream =
    new FakeDataStream(requiredSchema, Array.empty, predicates)
}

object PushdownStreamingSourceV1 {
  @volatile var lastBuilder: PushdownStreamingScanBuilderV1 = _
  val fullSchema = StructType(Seq(
    StructField("i", IntegerType),
    StructField("j", IntegerType),
    StructField("k", LongType)
  ))
}

class PushdownStreamingSourceV1
  extends DataSourceRegister
    with SimpleTableProvider {
  override def shortName(): String = "pushdown-streaming-v1"
  override def getTable(options: CaseInsensitiveStringMap): Table = new Table with SupportsRead {
    override def name(): String = "pushdown_streaming_v1"
    override def schema(): StructType = PushdownStreamingSourceV1.fullSchema
    override def capabilities(): util.Set[TableCapability] =
      util.EnumSet.of(MICRO_BATCH_READ, CONTINUOUS_READ)
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      val b = new PushdownStreamingScanBuilderV1
      PushdownStreamingSourceV1.lastBuilder = b
      b
    }
  }
}

object PushdownStreamingSourceV2 {
  @volatile var lastBuilder: PushdownStreamingScanBuilderV2 = _
  val fullSchema = StructType(Seq(
    StructField("i", IntegerType),
    StructField("j", IntegerType),
    StructField("k", LongType)
  ))
}

class PushdownStreamingSourceV2
  extends DataSourceRegister
    with SimpleTableProvider {
  override def shortName(): String = "pushdown-streaming-v2"
  override def getTable(options: CaseInsensitiveStringMap): Table = new Table with SupportsRead {
    override def name(): String = "pushdown_streaming_v2"
    override def schema(): StructType = PushdownStreamingSourceV2.fullSchema
    override def capabilities(): util.Set[TableCapability] =
      util.EnumSet.of(MICRO_BATCH_READ, CONTINUOUS_READ)
    override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
      val b = new PushdownStreamingScanBuilderV2
      PushdownStreamingSourceV2.lastBuilder = b
      b
    }
  }
}

class StreamingDataSourceV2PushdownSuite extends StreamTest {

  override def beforeAll(): Unit = {
    super.beforeAll()
    val fakeCheckpoint = Utils.createTempDir()
    spark.conf.set(SQLConf.CHECKPOINT_LOCATION.key, fakeCheckpoint.getCanonicalPath)
  }

  test("streaming v2 pushdown: v1 filters and column pruning (micro-batch)") {
    val q = spark.readStream
      .format("pushdown-streaming-v1")
      .load()
      .where("i > 3")
      .select("j")
      .writeStream
      .format("fake-write-microbatch-continuous")
      .trigger(Trigger.AvailableNow())
      .start()

    try {
      eventually(timeout(streamingTimeout)) {
        val stream = q.asInstanceOf[StreamingQueryWrapper].streamingQuery.logicalPlan.collect {
          case r: StreamingDataSourceV2ScanRelation => r.stream
        }.head.asInstanceOf[FakeDataStream]
        assert(stream.requiredSchema.fieldNames.sorted.sameElements(Array("i", "j")))
        assert(stream.pushedV1Filters.map(_.toString).sameElements(Array("GreaterThan(i,3)")))
      }
    } finally {
      q.stop()
    }
  }

  test("streaming v2 pushdown: v2 predicates and column pruning (micro-batch)") {
    val q = spark.readStream
      .format("pushdown-streaming-v2")
      .load()
      .where("i = 5")
      .select("j")
      .writeStream
      .format("fake-write-microbatch-continuous")
      .trigger(Trigger.AvailableNow())
      .start()

    try {
      eventually(timeout(streamingTimeout)) {
        val stream = q.asInstanceOf[StreamingQueryWrapper].streamingQuery.logicalPlan.collect {
          case r: StreamingDataSourceV2ScanRelation => r.stream
        }.head.asInstanceOf[FakeDataStream]
        assert(stream.requiredSchema.fieldNames.sorted.sameElements(Array("i", "j")))
        assert(stream.predicates.map(_.toString()).sameElements(Array("i = 5")))
      }
    } finally {
      q.stop()
    }
  }

  test("streaming v2 pushdown: v1 filters and column pruning (continuous)") {
    val q = spark.readStream
      .format("pushdown-streaming-v1")
      .load()
      .where("i > 100")
      .select("j")
      .writeStream
      .format("fake-write-microbatch-continuous")
      .trigger(Trigger.Continuous(1000))
      .start()

    try {
      eventually(timeout(streamingTimeout)) {
        val stream = q.asInstanceOf[StreamingQueryWrapper].streamingQuery.logicalPlan.collect {
          case r: StreamingDataSourceV2ScanRelation => r.stream
        }.head.asInstanceOf[FakeDataStream]
        assert(stream.requiredSchema.fieldNames.sorted.sameElements(Array("i", "j")))
        assert(stream.pushedV1Filters.map(_.toString).sameElements(Array("GreaterThan(i,100)")))
      }
    } finally {
      q.stop()
    }
  }

  test("streaming v2 pushdown: v2 predicates and column pruning (continuous)") {
    val q = spark.readStream
      .format("pushdown-streaming-v2")
      .load()
      .where("i = 500")
      .select("j")
      .writeStream
      .format("fake-write-microbatch-continuous")
      .trigger(Trigger.Continuous(1000))
      .start()

    try {
      eventually(timeout(streamingTimeout)) {
        val stream = q.asInstanceOf[StreamingQueryWrapper].streamingQuery.logicalPlan.collect {
          case r: StreamingDataSourceV2ScanRelation => r.stream
        }.head.asInstanceOf[FakeDataStream]
        assert(stream.requiredSchema.fieldNames.sorted.sameElements(Array("i", "j")))
        assert(stream.predicates.map(_.toString()).sameElements(Array("i = 500")))
      }
    } finally {
      q.stop()
    }
  }
}

class FakeScanBuilder extends ScanBuilder with Scan {
  override def build(): Scan = this
  override def readSchema(): StructType = StructType(Seq())
  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = new FakeDataStream
  override def toContinuousStream(checkpointLocation: String): ContinuousStream = new FakeDataStream
}

class FakeWriteBuilder extends WriteBuilder {
  override def build(): Write = {
    new Write {
      override def toStreaming: StreamingWrite = new FakeStreamingWrite
    }
  }
}

class FakeStreamingWrite extends StreamingWrite {
  override def createStreamingWriterFactory(
      info: PhysicalWriteInfo): StreamingDataWriterFactory = {
    throw new IllegalStateException("fake sink - cannot actually write")
  }
  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    throw new IllegalStateException("fake sink - cannot actually write")
  }
  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    throw new IllegalStateException("fake sink - cannot actually write")
  }
}

trait FakeStreamingWriteTable extends Table with SupportsWrite {
  override def name(): String = "fake"
  override def schema(): StructType = StructType(Seq())
  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(STREAMING_WRITE)
  }
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new FakeWriteBuilder
  }
}

class FakeReadMicroBatchOnly
    extends DataSourceRegister
    with SimpleTableProvider
    with SessionConfigSupport {
  override def shortName(): String = "fake-read-microbatch-only"

  override def keyPrefix: String = shortName()

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    LastReadOptions.options = options
    new Table with SupportsRead {
      override def name(): String = "fake"
      override def schema(): StructType = StructType(Seq())
      override def capabilities(): util.Set[TableCapability] = {
        util.EnumSet.of(MICRO_BATCH_READ)
      }
      override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
        new FakeScanBuilder
      }
    }
  }
}

class FakeReadContinuousOnly
    extends DataSourceRegister
    with SimpleTableProvider
    with SessionConfigSupport {
  override def shortName(): String = "fake-read-continuous-only"

  override def keyPrefix: String = shortName()

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    LastReadOptions.options = options
    new Table with SupportsRead {
      override def name(): String = "fake"
      override def schema(): StructType = StructType(Seq())
      override def capabilities(): util.Set[TableCapability] = {
        util.EnumSet.of(CONTINUOUS_READ)
      }
      override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
        new FakeScanBuilder
      }
    }
  }
}

class FakeReadBothModes extends DataSourceRegister with SimpleTableProvider {
  override def shortName(): String = "fake-read-microbatch-continuous"

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new Table with SupportsRead {
      override def name(): String = "fake"
      override def schema(): StructType = StructType(Seq())
      override def capabilities(): util.Set[TableCapability] = {
        util.EnumSet.of(MICRO_BATCH_READ, CONTINUOUS_READ)
      }
      override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
        new FakeScanBuilder
      }
    }
  }
}

class FakeReadNeitherMode extends DataSourceRegister with SimpleTableProvider {
  override def shortName(): String = "fake-read-neither-mode"

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new Table {
      override def name(): String = "fake"
      override def schema(): StructType = StructType(Nil)
      override def capabilities(): util.Set[TableCapability] =
        util.EnumSet.noneOf(classOf[TableCapability])
    }
  }
}

class FakeWriteOnly
    extends DataSourceRegister
    with SimpleTableProvider
    with SessionConfigSupport {
  override def shortName(): String = "fake-write-microbatch-continuous"

  override def keyPrefix: String = shortName()

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    LastWriteOptions.options = options
    new Table with FakeStreamingWriteTable {
      override def name(): String = "fake"
      override def schema(): StructType = StructType(Nil)
    }
  }
}

class FakeNoWrite extends DataSourceRegister with SimpleTableProvider {
  override def shortName(): String = "fake-write-neither-mode"
  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new Table {
      override def name(): String = "fake"
      override def schema(): StructType = StructType(Nil)
      override def capabilities(): util.Set[TableCapability] =
        util.EnumSet.noneOf(classOf[TableCapability])
    }
  }
}

class FakeWriteSupportingExternalMetadata
    extends DataSourceRegister
    with TableProvider {
  override def shortName(): String = "fake-write-supporting-external-metadata"

  override def supportsExternalMetadata(): Boolean = true

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    throw new IllegalArgumentException(
      "Data stream writer should not require inferring table schema the data source supports" +
      " external Metadata.")
  }

  override def getTable(
      tableSchema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    new Table with FakeStreamingWriteTable {
      override def name(): String = "fake"
      override def schema(): StructType = tableSchema
    }
  }
}

case class FakeWriteV1FallbackException() extends Exception

class FakeSink extends Sink {
  override def addBatch(batchId: Long, data: DataFrame): Unit = {}
}

class FakeWriteSupportProviderV1Fallback extends DataSourceRegister
  with SimpleTableProvider with StreamSinkProvider {

  override def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {
    new FakeSink()
  }

  override def shortName(): String = "fake-write-v1-fallback"

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new Table with FakeStreamingWriteTable {
      override def name(): String = "fake"
      override def schema(): StructType = StructType(Nil)
    }
  }
}

object LastReadOptions {
  var options: CaseInsensitiveStringMap = _

  def clear(): Unit = {
    options = null
  }
}

object LastWriteOptions {
  var options: CaseInsensitiveStringMap = _

  def clear(): Unit = {
    options = null
  }
}

@SlowSQLTest
class StreamingDataSourceV2Suite extends StreamTest {

  override def beforeAll(): Unit = {
    super.beforeAll()
    val fakeCheckpoint = Utils.createTempDir()
    spark.conf.set(SQLConf.CHECKPOINT_LOCATION.key, fakeCheckpoint.getCanonicalPath)
  }

  override def afterEach(): Unit = {
    LastReadOptions.clear()
    LastWriteOptions.clear()
  }

  val readFormats = Seq(
    "fake-read-microbatch-only",
    "fake-read-continuous-only",
    "fake-read-microbatch-continuous",
    "fake-read-neither-mode")
  val writeFormats = Seq(
    "fake-write-microbatch-continuous",
    "fake-write-neither-mode")
  val triggers = Seq(
    // NOTE: the test uses the deprecated Trigger.Once() by intention, do not change.
    Trigger.Once(),
    Trigger.AvailableNow(),
    Trigger.ProcessingTime(1000),
    Trigger.Continuous(1000))

  private def testCase(readFormat: String, writeFormat: String, trigger: Trigger): Unit = {
    testPositiveCaseWithQuery(readFormat, writeFormat, trigger)(_ => ())
  }

  private def testPositiveCaseWithQuery(
      readFormat: String,
      writeFormat: String,
      trigger: Trigger)(check: StreamingQuery => Unit): Unit = {
    val query = spark.readStream
      .format(readFormat)
      .load()
      .writeStream
      .format(writeFormat)
      .trigger(trigger)
      .start()
    check(query)
    query.stop()
  }

  private def testPostCreationNegativeCase(
      readFormat: String,
      writeFormat: String,
      trigger: Trigger,
      condition: String,
      parameters: Map[String, String]) = {
    val query = spark.readStream
      .format(readFormat)
      .load()
      .writeStream
      .format(writeFormat)
      .trigger(trigger)
      .start()

    eventually(timeout(streamingTimeout)) {
      assert(query.exception.isDefined)
      assert(query.exception.get.cause != null)
      checkErrorMatchPVals(
        exception = query.exception.get.cause.asInstanceOf[SparkUnsupportedOperationException],
        condition = condition,
        parameters = parameters
      )
    }
  }

  test("SPARK-33369: Skip schema inference in DataStreamWriter.start() if table provider " +
    "supports external metadata") {
    testPositiveCaseWithQuery(
      "fake-read-microbatch-continuous", "fake-write-supporting-external-metadata",
      Trigger.AvailableNow()) { v2Query =>
      val sink = v2Query.asInstanceOf[StreamingQueryWrapper].streamingQuery.sink
      assert(sink.isInstanceOf[Table])
      assert(sink.columns.isEmpty)
    }
  }

  test("disabled v2 write") {
    // Ensure the V2 path works normally and generates a V2 sink..
    testPositiveCaseWithQuery(
      "fake-read-microbatch-continuous", "fake-write-v1-fallback",
      Trigger.AvailableNow()) { v2Query =>
      assert(v2Query.asInstanceOf[StreamingQueryWrapper].streamingQuery.sink
        .isInstanceOf[Table])
    }

    // Ensure we create a V1 sink with the config. Note the config is a comma separated
    // list, including other fake entries.
    val fullSinkName = classOf[FakeWriteSupportProviderV1Fallback].getName
    withSQLConf(SQLConf.DISABLED_V2_STREAMING_WRITERS.key -> s"a,b,c,test,$fullSinkName,d,e") {
      testPositiveCaseWithQuery(
        "fake-read-microbatch-continuous", "fake-write-v1-fallback",
        Trigger.AvailableNow()) { v1Query =>
        assert(v1Query.asInstanceOf[StreamingQueryWrapper].streamingQuery.sink
          .isInstanceOf[FakeSink])
      }
    }
  }

  Seq(
    Tuple2(classOf[FakeReadMicroBatchOnly], Trigger.AvailableNow()),
    Tuple2(classOf[FakeReadContinuousOnly], Trigger.Continuous(1000))
  ).foreach { case (source, trigger) =>
    test(s"SPARK-25460: session options are respected in structured streaming sources - $source") {
      // `keyPrefix` and `shortName` are the same in this test case
      val readSource = source.getConstructor().newInstance().shortName()
      val writeSource = "fake-write-microbatch-continuous"

      val readOptionName = "optionA"
      withSQLConf(s"spark.datasource.$readSource.$readOptionName" -> "true") {
        testPositiveCaseWithQuery(readSource, writeSource, trigger) { _ =>
          eventually(timeout(streamingTimeout)) {
            // Write options should not be set.
            assert(!LastWriteOptions.options.containsKey(readOptionName))
            assert(LastReadOptions.options.getBoolean(readOptionName, false))
          }
        }
      }

      val writeOptionName = "optionB"
      withSQLConf(s"spark.datasource.$writeSource.$writeOptionName" -> "true") {
        testPositiveCaseWithQuery(readSource, writeSource, trigger) { _ =>
          eventually(timeout(streamingTimeout)) {
            // Read options should not be set.
            assert(!LastReadOptions.options.containsKey(writeOptionName))
            assert(LastWriteOptions.options.getBoolean(writeOptionName, false))
          }
        }
      }
    }
  }

  // Get a list of (read, write, trigger) tuples for test cases.
  val cases = readFormats.flatMap { read =>
    writeFormats.flatMap { write =>
      triggers.map(t => (write, t))
    }.map {
      case (write, t) => (read, write, t)
    }
  }

  for ((read, write, trigger) <- cases) {
    testQuietly(s"stream with read format $read, write format $write, trigger $trigger") {
      val sourceTable = DataSource.lookupDataSource(read, spark.sessionState.conf).getConstructor()
        .newInstance().asInstanceOf[SimpleTableProvider].getTable(CaseInsensitiveStringMap.empty())

      val sinkTable = DataSource.lookupDataSource(write, spark.sessionState.conf).getConstructor()
        .newInstance().asInstanceOf[SimpleTableProvider].getTable(CaseInsensitiveStringMap.empty())

      import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._
      trigger match {
        // Invalid - can't read at all
        case _ if !sourceTable.supportsAny(MICRO_BATCH_READ, CONTINUOUS_READ) =>
          checkError(
            exception = intercept[SparkUnsupportedOperationException] {
              testCase(read, write, trigger)
            },
            condition = "_LEGACY_ERROR_TEMP_2049",
            parameters = Map(
              "className" -> "fake-read-neither-mode",
              "operator" -> "reading"
            )
          )

        // Invalid - can't write
        case _ if !sinkTable.supports(STREAMING_WRITE) =>
          checkError(
            exception = intercept[SparkUnsupportedOperationException] {
              testCase(read, write, trigger)
            },
            condition = "_LEGACY_ERROR_TEMP_2049",
            parameters = Map(
              "className" -> "fake-write-neither-mode",
              "operator" -> "writing"
            )
          )

        case _: ContinuousTrigger =>
          if (sourceTable.supports(CONTINUOUS_READ)) {
            // Valid microbatch queries.
            testCase(read, write, trigger)
          } else {
            // Invalid - trigger is continuous but reader is not
            checkError(
              exception = intercept[SparkUnsupportedOperationException] {
                testCase(read, write, trigger)
              },
              condition = "_LEGACY_ERROR_TEMP_2253",
              parameters = Map("sourceName" -> "fake-read-microbatch-only")
            )
          }

        case microBatchTrigger =>
          if (sourceTable.supports(MICRO_BATCH_READ)) {
            // Valid continuous queries.
            testCase(read, write, trigger)
          } else {
            // Invalid - trigger is microbatch but reader is not
            testPostCreationNegativeCase(read, write, trigger,
              condition = "_LEGACY_ERROR_TEMP_2209",
              parameters = Map(
                "srcName" -> read,
                "disabledSources" -> "",
                "table" -> ".*"
              )
            )
          }
      }
    }
  }
}
