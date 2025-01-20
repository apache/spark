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

package org.apache.spark.sql.execution.streaming.state

import java.io._
import java.nio.charset.Charset
import java.util.UUID
import java.util.concurrent.Executors

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.util.Random

import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.rocksdb.CompressionType
import org.scalactic.source.Position
import org.scalatest.PrivateMethodTester
import org.scalatest.Tag

import org.apache.spark.{SparkConf, SparkException, SparkFunSuite, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.execution.streaming.{CheckpointFileManager, CreateAtomicTestManager, FileContextBasedCheckpointFileManager, FileSystemBasedCheckpointFileManager}
import org.apache.spark.sql.execution.streaming.CheckpointFileManager.{CancellableFSDataOutputStream, RenameBasedFSDataOutputStream}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.STREAMING_CHECKPOINT_FILE_MANAGER_CLASS
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types._
import org.apache.spark.tags.SlowSQLTest
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{ThreadUtils, Utils}
import org.apache.spark.util.ArrayImplicits._

class NoOverwriteFileSystemBasedCheckpointFileManager(path: Path, hadoopConf: Configuration)
  extends FileSystemBasedCheckpointFileManager(path, hadoopConf) {

  override def createAtomic(path: Path,
                            overwriteIfPossible: Boolean): CancellableFSDataOutputStream = {
    new RenameBasedFSDataOutputStream(this, path, overwriteIfPossible)
  }

  override def renameTempFile(srcPath: Path, dstPath: Path,
                              overwriteIfPossible: Boolean): Unit = {
    if (!fs.exists(dstPath)) {
      // only write if a file does not exist at this location
      super.renameTempFile(srcPath, dstPath, overwriteIfPossible)
    }
  }
}

class TestStateStoreChangelogWriterV101(
    fm: CheckpointFileManager,
    file: Path,
    compressionCodec: CompressionCodec)
  extends StateStoreChangelogWriterV1(fm, file, compressionCodec) {

  override def version: Short = 101

  writeVersion()
}

trait RocksDBStateStoreChangelogCheckpointingTestUtil {
  val rocksdbChangelogCheckpointingConfKey: String = RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX +
    ".changelogCheckpointing.enabled"

  def isChangelogCheckpointingEnabled: Boolean =
    SQLConf.get.getConfString(rocksdbChangelogCheckpointingConfKey) == "true"

  def snapshotVersionsPresent(dir: File): Seq[Long] = {
    dir.listFiles.filter(_.getName.endsWith(".zip"))
      .map(_.getName.stripSuffix(".zip").split("_"))
      .map {
        case Array(version, _) => version.toLong
        case Array(version) => version.toLong
      }
      .sorted
      .toImmutableArraySeq
  }

  def changelogVersionsPresent(dir: File): Seq[Long] = {
    dir.listFiles.filter(_.getName.endsWith(".changelog"))
      .map(_.getName.stripSuffix(".changelog").split("_"))
      .map {
        case Array(version, _) => version.toLong
        case Array(version) => version.toLong
      }
      .sorted
      .toImmutableArraySeq
  }
}

/**
 * Whenever using this in conjunction with AlsoTestWithRocksDBFeatures, this should
 * always be inherited first, so 'testWithChangelogCheckpointingEnabled' tests with
 * both Avro and UnsafeRow enabled.
 */
trait AlsoTestWithEncodingTypes extends SQLTestUtils {
  override protected def test(testName: String, testTags: Tag*)(testBody: => Any)
                             (implicit pos: Position): Unit = {
    Seq("unsaferow", "avro").foreach { encoding =>
      super.test(s"$testName (encoding = $encoding)", testTags: _*) {
        withSQLConf(SQLConf.STREAMING_STATE_STORE_ENCODING_FORMAT.key -> encoding) {
          testBody
        }
      }
    }
  }

  def usingAvroEncoding(): Boolean = {
    getCurrentEncoding() == StateStoreEncoding.Avro.toString
  }

  // Helper method to test with specific encoding
  protected def testWithEncoding(encoding: String)(testName: String)(testBody: => Any)
                                (implicit pos: Position): Unit = {
    super.test(s"$testName (encoding = $encoding)", Seq.empty: _*) {
      withSQLConf(SQLConf.STREAMING_STATE_STORE_ENCODING_FORMAT.key -> encoding) {
        testBody
      }
    }
  }

  protected def getCurrentEncoding(): String = {
    spark.conf.get(SQLConf.STREAMING_STATE_STORE_ENCODING_FORMAT.key)
  }
}

trait AlsoTestWithRocksDBFeatures
  extends SQLTestUtils with RocksDBStateStoreChangelogCheckpointingTestUtil {

  sealed trait TestMode
  case object TestWithChangelogCheckpointingEnabled extends TestMode
  case object TestWithChangelogCheckpointingDisabled extends TestMode
  case object TestWithBothChangelogCheckpointingEnabledAndDisabled extends TestMode

  override protected def test(testName: String, testTags: Tag*)(testBody: => Any)
                             (implicit pos: Position): Unit = {
    testWithChangelogCheckpointingEnabled(testName, testTags: _*)(testBody)
    testWithChangelogCheckpointingDisabled(testName, testTags: _*)(testBody)
  }

  def testWithChangelogCheckpointingEnabled(testName: String, testTags: Tag*)
                                        (testBody: => Any): Unit = {
    super.test(testName + " (with changelog checkpointing)", testTags: _*) {
      // in case tests have any code that needs to execute before every test
      super.beforeEach()
      withSQLConf(rocksdbChangelogCheckpointingConfKey -> "true",
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName) {
        testBody
      }
      // in case tests have any code that needs to execute after every test
      super.afterEach()
    }
  }

  def testWithChangelogCheckpointingDisabled(testName: String, testTags: Tag*)
                                           (testBody: => Any): Unit = {
    super.test(testName + " (without changelog checkpointing)", testTags: _*) {
      // in case tests have any code that needs to execute before every test
      super.beforeEach()
      withSQLConf(rocksdbChangelogCheckpointingConfKey -> "false",
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName) {
        testBody
      }
      // in case tests have any code that needs to execute after every test
      super.afterEach()
    }
  }

  def testWithRocksDBStateStore(
    testName: String, testTags: Tag*)(testBody: => Any): Unit = {
    super.test(testName, testTags: _*) {
      // in case tests have any code that needs to execute before every test
      super.beforeEach()
      withSQLConf(
        SQLConf.STATE_STORE_PROVIDER_CLASS.key -> classOf[RocksDBStateStoreProvider].getName) {
        testBody
      }
      // in case tests have any code that needs to execute after every test
      super.afterEach()
    }
  }

  def testWithColumnFamiliesAndEncodingTypes(
      testName: String,
      testMode: TestMode = TestWithBothChangelogCheckpointingEnabledAndDisabled)
      (testBody: Boolean => Any): Unit = {
    // For each encoding type
    Seq("unsaferow", "avro").foreach { encoding =>
      // Call testWithColumnFamilies for each encoding
      testWithColumnFamilies(s"$testName (encoding = $encoding)", testMode) { colFamiliesEnabled =>
        withSQLConf(SQLConf.STREAMING_STATE_STORE_ENCODING_FORMAT.key -> encoding) {
          testBody(colFamiliesEnabled)
        }
      }
    }
  }

  def testWithColumnFamilies(
      testName: String,
      testMode: TestMode,
      testTags: Tag*)
      (testBody: Boolean => Any): Unit = {
    Seq(true, false).foreach { colFamiliesEnabled =>
      testMode match {
        case TestWithChangelogCheckpointingEnabled =>
          testWithChangelogCheckpointingEnabled(s"$testName - " +
            s"with colFamiliesEnabled=$colFamiliesEnabled", testTags: _*) {
            testBody(colFamiliesEnabled)
          }

        case TestWithChangelogCheckpointingDisabled =>
          testWithChangelogCheckpointingDisabled(s"$testName - " +
            s"with colFamiliesEnabled=$colFamiliesEnabled", testTags: _*) {
            testBody(colFamiliesEnabled)
          }

        case TestWithBothChangelogCheckpointingEnabledAndDisabled =>
          testWithChangelogCheckpointingEnabled(s"$testName - " +
            s"with colFamiliesEnabled=$colFamiliesEnabled", testTags: _*) {
            testBody(colFamiliesEnabled)
          }
          testWithChangelogCheckpointingDisabled(s"$testName - " +
            s"with colFamiliesEnabled=$colFamiliesEnabled", testTags: _*) {
            testBody(colFamiliesEnabled)
          }

        case _ =>
          throw new IllegalArgumentException(s"Unknown test mode: $testMode")
      }
    }
  }

  def testWithStateStoreCheckpointIdsAndColumnFamilies(
      testName: String,
      testMode: TestMode,
      testTags: Tag*)
      (testBody: (Boolean, Boolean) => Any): Unit = {
    Seq(true, false).foreach { enableStateStoreCheckpointIds =>
      val newTestName = s"$testName - with enableStateStoreCheckpointIds = " +
        s"$enableStateStoreCheckpointIds"
      testWithColumnFamilies(newTestName, testMode, testTags: _*) { colFamiliesEnabled =>
        testBody(enableStateStoreCheckpointIds, colFamiliesEnabled)
      }
    }
  }

  def testWithStateStoreCheckpointIds(
    testName: String,
    testTags: Tag*)
    (testBody: Boolean => Any): Unit = {
    Seq(true, false).foreach { enableStateStoreCheckpointIds =>
      val newTestName = s"$testName - with enableStateStoreCheckpointIds = " +
        s"$enableStateStoreCheckpointIds"
      test(newTestName, testTags: _*) { enableStateStoreCheckpointIds =>
        testBody(enableStateStoreCheckpointIds)
      }
    }
  }

  def testWithStateStoreCheckpointIdsAndChangelogEnabled(
    testName: String,
    testTags: Tag*)
    (testBody: Boolean => Any): Unit = {
    Seq(true, false).foreach { enableStateStoreCheckpointIds =>
      val newTestName = s"$testName - with enableStateStoreCheckpointIds = " +
        s"$enableStateStoreCheckpointIds"
      testWithChangelogCheckpointingDisabled(newTestName, testTags: _*) {
        enableStateStoreCheckpointIds => testBody(enableStateStoreCheckpointIds)
      }
    }
  }
}

class OpenNumCountedTestInputStream(in: InputStream) extends FSDataInputStream(in) {
  import OpenNumCountedTestInputStream._

  addOpenStreams(this)

  override def close(): Unit = {
    removeOpenStream(this)
    super.close()
  }
}

class OpenStreamCountedTestFileManager(path: Path, hadoopConf: Configuration)
  extends FileContextBasedCheckpointFileManager(path, hadoopConf) {

  override def open(path: Path): FSDataInputStream = {
    val stream = new OpenNumCountedTestInputStream(super.open(path))
    stream
  }
}

object OpenNumCountedTestInputStream extends Logging {
  private val openStreams = mutable.Map.empty[FSDataInputStream, Throwable]

  def addOpenStreams(stream: FSDataInputStream): Unit = openStreams.synchronized {
    openStreams.put(stream, new Throwable())
  }

  def removeOpenStream(stream: FSDataInputStream): Unit = openStreams.synchronized {
    openStreams.remove(stream)
  }

  def clearOpenStreams(): Unit = openStreams.synchronized {
    openStreams.clear()
  }

  def assertNoOpenStreams(): Unit = openStreams.synchronized {
    val numOpen = openStreams.values.size
    if (numOpen > 0) {
      for (exc <- openStreams.values) {
        logWarning("Leaked filesystem connection created at:")
        exc.printStackTrace()
      }
      throw new IllegalStateException(s"There are $numOpen possibly leaked file streams.",
        openStreams.values.head)
    }
  }
}

class RocksDBStateEncoderSuite extends SparkFunSuite {

  // Helper method to create test schemas
  private def createTestSchemas() = {
    val keySchema = StructType(Seq(
      StructField("k1", IntegerType),
      StructField("k2", LongType),
      StructField("k3", DoubleType)
    ))
    val valueSchema = StructType(Seq(
      StructField("v1", StringType),
      StructField("v2", BooleanType)
    ))
    (keySchema, valueSchema)
  }

  // Create encoders for different key encoding strategies
  private def createTestEncoder(keyStateEncoderSpec: KeyStateEncoderSpec): RocksDBDataEncoder = {
    val (keySchema, valueSchema) = createTestSchemas()
    val stateSchemaInfo = Some(StateSchemaInfo(
      keySchemaId = 0,
      valueSchemaId = 0
    ))
    // Create test state schema provider
    val testProvider = new TestStateSchemaProvider()
    testProvider.captureSchema(
      StateStore.DEFAULT_COL_FAMILY_NAME,
      keySchema,
      valueSchema,
      keySchemaId = 0,
      valueSchemaId = 0
    )

    // Create encoder with column family info
    new AvroStateEncoder(
      keyStateEncoderSpec,
      valueSchema,
      Some(testProvider),
      Some(ColumnFamilyInfo(StateStore.DEFAULT_COL_FAMILY_NAME, 0))
    )
    new AvroStateEncoder(keyStateEncoderSpec, valueSchema, None, None)
  }

  private def createNoPrefixKeyEncoder(): RocksDBDataEncoder = {
    val (keySchema, _) = createTestSchemas()
    createTestEncoder(NoPrefixKeyStateEncoderSpec(keySchema))
  }

  private def createPrefixKeyScanEncoder(): RocksDBDataEncoder = {
    val (keySchema, _) = createTestSchemas()
    createTestEncoder(PrefixKeyScanStateEncoderSpec(keySchema, numColsPrefixKey = 2))
  }

  private def createRangeKeyScanEncoder(): RocksDBDataEncoder = {
    val (keySchema, _) = createTestSchemas()
    createTestEncoder(RangeKeyScanStateEncoderSpec(keySchema, orderingOrdinals = Seq(0, 1)))
  }

  test("verify schema ID handling in prefix and range scan key encoding") {
    val keySchema = StructType(Seq(
      StructField("k1", IntegerType),
      StructField("k2", LongType),
      StructField("k3", DoubleType)
    ))
    val valueSchema = StructType(Seq(
      StructField("v1", StringType)
    ))

    val testProvider = new TestStateSchemaProvider()
    testProvider.captureSchema(
      StateStore.DEFAULT_COL_FAMILY_NAME,
      keySchema,
      valueSchema,
      keySchemaId = 18,
      valueSchemaId = 0
    )

    // Create test row with some data
    val keyProj = UnsafeProjection.create(keySchema)
    val fullKeyRow = keyProj.apply(InternalRow(42, 123L, 3.14))

    // Test prefix scan encoding with schema evolution
    withClue("Testing prefix scan encoding: ") {
      val prefixKeySpec = PrefixKeyScanStateEncoderSpec(keySchema, numColsPrefixKey = 2)
      val encoder = new AvroStateEncoder(prefixKeySpec, valueSchema, Some(testProvider),
        Some(ColumnFamilyInfo(StateStore.DEFAULT_COL_FAMILY_NAME, 0)))

      // Then encode just the remaining key portion (which should include schema ID)
      val remainingKeyRow = keyProj.apply(InternalRow(null, null, 3.14))
      val encodedRemainingKey = encoder.encodeRemainingKey(remainingKeyRow)

      // Verify schema ID in remaining key bytes
      val decodedSchemaIdRow = encoder.decodeStateSchemaIdRow(encodedRemainingKey)
      assert(decodedSchemaIdRow.schemaId === 18,
        "Schema ID not preserved in prefix scan remaining key encoding")
    }

    testProvider.captureSchema(
      StateStore.DEFAULT_COL_FAMILY_NAME,
      keySchema,
      valueSchema,
      keySchemaId = 24,
      valueSchemaId = 0
    )

    // Test range scan encoding with schema evolution
    withClue("Testing range scan encoding: ") {
      val rangeScanSpec = RangeKeyScanStateEncoderSpec(keySchema, orderingOrdinals = Seq(0, 1))
      val encoder = new AvroStateEncoder(rangeScanSpec, valueSchema, Some(testProvider),
        Some(ColumnFamilyInfo(StateStore.DEFAULT_COL_FAMILY_NAME, 0)))

      // Encode remaining key (non-ordering columns)
      // For range scan, the remaining key schema only contains columns NOT in orderingOrdinals
      val remainingKeySchema = StructType(Seq(
        StructField("k3", DoubleType)  // Only the non-ordering column
      ))
      val remainingKeyProj = UnsafeProjection.create(remainingKeySchema)
      val remainingKeyRow = remainingKeyProj.apply(InternalRow(3.14))
      val encodedRemainingKey = encoder.encodeRemainingKey(remainingKeyRow)

      // Verify schema ID in remaining key bytes
      val decodedSchemaIdRow = encoder.decodeStateSchemaIdRow(encodedRemainingKey)
      assert(decodedSchemaIdRow.schemaId === 24,
        "Schema ID not preserved in range scan remaining key encoding")

      // Verify we can decode the remaining key correctly
      // The decoded row should only have the non-ordering column (k3)
      val decodedRemainingKey = encoder.decodeRemainingKey(encodedRemainingKey)
      assert(decodedRemainingKey.getDouble(0) === 3.14,
        "Data not preserved in range scan remaining key encoding")

      // Test the range scan key portion (ordering columns)
      val rangeScanKeySchema = StructType(Seq(
        StructField("k1", IntegerType),
        StructField("k2", LongType)
      ))
      val rangeScanProj = UnsafeProjection.create(rangeScanKeySchema)
      val rangeScanRow = rangeScanProj.apply(InternalRow(42, 123L))
      val encodedRangeScan = encoder.encodePrefixKeyForRangeScan(rangeScanRow)

      // Range scan portion should not have schema ID since it uses special encoding
      val decodedRangeScan = encoder.decodePrefixKeyForRangeScan(encodedRangeScan)
      assert(decodedRangeScan.getInt(0) === 42)
      assert(decodedRangeScan.getLong(1) === 123L)
    }
  }

  test("verify schema ID preservation through encode/decode cycle") {
    val encoders = Seq(
      ("NoPrefixKey", createNoPrefixKeyEncoder()),
      ("PrefixKeyScan", createPrefixKeyScanEncoder()),
      ("RangeKeyScan", createRangeKeyScanEncoder())
    )

    // Test a range of schema IDs including edge cases
    val testSchemaIds = Seq[Short](
      0, // Min value
      1, // Common case
      42, // Arbitrary value
      -1, // Negative value
      Short.MaxValue, // Max positive
      Short.MinValue // Max negative
    )

    encoders.foreach { case (encoderType, encoder) =>
      testSchemaIds.foreach { schemaId =>
        withClue(s"Testing $encoderType encoder with schema ID $schemaId: ") {
          val testData = Array[Byte](1, 2, 3, 4)
          val schemaIdRow = StateSchemaIdRow(schemaId, testData)

          // Encode the row
          val encoded = encoder.encodeWithStateSchemaId(schemaIdRow)

          // Verify schema ID directly in encoded bytes
          val encodedSchemaId = Platform.getShort(encoded, Platform.BYTE_ARRAY_OFFSET)
          assert(encodedSchemaId === schemaId,
            s"Schema ID mismatch in encoded bytes: expected $schemaId but got $encodedSchemaId")

          // Decode and verify
          val decoded = encoder.decodeStateSchemaIdRow(encoded)
          assert(decoded.schemaId === schemaId,
            s"Schema ID mismatch after decode: expected $schemaId but got ${decoded.schemaId}")

          // Also verify data wasn't corrupted
          assert(decoded.bytes === testData,
            "Data corruption detected in encode/decode cycle")
        }
      }
    }
  }

  test("verify schema ID handling in single value encoder") {
    val keySchema = StructType(Seq(
      StructField("k1", IntegerType)
    ))
    val valueSchema = StructType(Seq(
      StructField("v1", StringType),
      StructField("v2", IntegerType),
      StructField("v3", BooleanType)
    ))
    val testProvider = new TestStateSchemaProvider()
    testProvider.captureSchema(
      StateStore.DEFAULT_COL_FAMILY_NAME,
      keySchema,
      valueSchema,
      keySchemaId = 0,
      valueSchemaId = 42
    )


    val valueProj = UnsafeProjection.create(valueSchema)
    val value = valueProj.apply(InternalRow(UTF8String.fromString("hello"), 42, true))

    withClue("Testing single value encoder: ") {
      val keySpec = NoPrefixKeyStateEncoderSpec(keySchema)
      val stateSchemaInfo = Some(StateSchemaInfo(keySchemaId = 0, valueSchemaId = 42))
      val avroEncoder = new AvroStateEncoder(keySpec, valueSchema, Some(testProvider),
        Some(ColumnFamilyInfo(StateStore.DEFAULT_COL_FAMILY_NAME, 0)))
      val valueEncoder = new SingleValueStateEncoder(avroEncoder, valueSchema)

      // Encode value
      val encodedValue = valueEncoder.encodeValue(value)

      // Verify schema ID was included and preserved
      val decodedSchemaIdRow = avroEncoder.decodeStateSchemaIdRow(encodedValue)
      assert(decodedSchemaIdRow.schemaId === 42,
        "Schema ID not preserved in single value encoding")

      // Verify value was preserved
      val decodedValue = valueEncoder.decodeValue(encodedValue)
      assert(decodedValue.getString(0) === "hello")
      assert(decodedValue.getInt(1) === 42)
      assert(decodedValue.getBoolean(2) === true)
    }
  }
}

@SlowSQLTest
class RocksDBSuite extends AlsoTestWithRocksDBFeatures with SharedSparkSession
    with PrivateMethodTester {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(SQLConf.STATE_STORE_PROVIDER_CLASS, classOf[RocksDBStateStoreProvider].getName)
  }

  // In each test we verify opened streams are all closed
  private def hadoopConf: Configuration = {
    val fmClass = "org.apache.spark.sql.execution.streaming.state." +
      "OpenStreamCountedTestFileManager"
    val hadoopConf = new Configuration()
    hadoopConf.set(STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key, fmClass)
    hadoopConf
  }

  override def beforeEach(): Unit = {
    OpenNumCountedTestInputStream.clearOpenStreams()
  }

  override def afterEach(): Unit = {
    eventually(timeout(10.seconds), interval(2.seconds)) {
      OpenNumCountedTestInputStream.assertNoOpenStreams()
    }
  }

  testWithStateStoreCheckpointIdsAndColumnFamilies("RocksDB: check changelog and snapshot version",
      TestWithChangelogCheckpointingEnabled) {
    case (enableStateStoreCheckpointIds, colFamiliesEnabled) =>
      val remoteDir = Utils.createTempDir().toString
      val conf = dbConf.copy(minDeltasForSnapshot = 1)
      new File(remoteDir).delete() // to make sure that the directory gets created
      val versionToUniqueId = new mutable.HashMap[Long, String]()
      withDB(remoteDir, conf = conf,
        useColumnFamilies = colFamiliesEnabled,
        enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
        versionToUniqueId = versionToUniqueId) { db =>
        for (version <- 0 to 49) {
          db.load(version, versionToUniqueId.get(version))
          db.put(version.toString, version.toString)
          db.commit()
          if ((version + 1) % 5 == 0) db.doMaintenance()
        }
      }

      if (isChangelogCheckpointingEnabled) {
        assert(changelogVersionsPresent(remoteDir) === (1 to 50))
        assert(snapshotVersionsPresent(remoteDir) === Range.inclusive(5, 50, 5))
      } else {
        assert(changelogVersionsPresent(remoteDir) === Seq.empty)
        assert(snapshotVersionsPresent(remoteDir) === (1 to 50))
      }
  }

  testWithStateStoreCheckpointIdsAndColumnFamilies(s"RocksDB: load version that doesn't exist",
    TestWithBothChangelogCheckpointingEnabledAndDisabled) {
    case (enableStateStoreCheckpointIds, colFamiliesEnabled) =>
      val provider = new RocksDBStateStoreProvider()
      var ex = intercept[SparkException] {
        provider.getStore(-1)
      }
      checkError(
        ex,
        condition = "CANNOT_LOAD_STATE_STORE.UNEXPECTED_VERSION",
        parameters = Map("version" -> "-1")
      )
      ex = intercept[SparkException] {
        provider.getReadStore(-1)
      }
      checkError(
        ex,
        condition = "CANNOT_LOAD_STATE_STORE.UNEXPECTED_VERSION",
        parameters = Map("version" -> "-1")
      )

      val remoteDir = Utils.createTempDir().toString
      new File(remoteDir).delete() // to make sure that the directory gets created
      val versionToUniqueId = new mutable.HashMap[Long, String]()
      withDB(remoteDir, useColumnFamilies = colFamiliesEnabled,
          enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
          versionToUniqueId = versionToUniqueId) { db =>
        ex = intercept[SparkException] {
          db.load(1, versionToUniqueId.get(1))
        }
        checkError(
          ex,
          condition = "CANNOT_LOAD_STATE_STORE.CANNOT_READ_STREAMING_STATE_FILE",
          parameters = Map(
            "fileToRead" -> s"$remoteDir/1.changelog"
          )
        )
      }
  }

  testWithStateStoreCheckpointIdsAndColumnFamilies(
      "RocksDB: purge changelog and snapshots with minVersionsToDelete = 0",
    TestWithChangelogCheckpointingEnabled) {
    case (enableStateStoreCheckpointIds, colFamiliesEnabled) =>
      val remoteDir = Utils.createTempDir().toString
      new File(remoteDir).delete() // to make sure that the directory gets created
      val conf = dbConf.copy(enableChangelogCheckpointing = true,
        minVersionsToRetain = 3, minDeltasForSnapshot = 1, minVersionsToDelete = 0)
      withDB(remoteDir, conf = conf, useColumnFamilies = colFamiliesEnabled,
          enableStateStoreCheckpointIds = enableStateStoreCheckpointIds) { db =>
        db.load(0)
        db.commit()
        for (version <- 1 to 2) {
          db.load(version)
          db.commit()
          db.doMaintenance()
        }
        assert(snapshotVersionsPresent(remoteDir) === Seq(2, 3))
        assert(changelogVersionsPresent(remoteDir) == Seq(1, 2, 3))

        for (version <- 3 to 4) {
          db.load(version)
          db.commit()
        }
        assert(snapshotVersionsPresent(remoteDir) === Seq(2, 3))
        assert(changelogVersionsPresent(remoteDir) == (1 to 5))
        db.doMaintenance()
        // 3 is the latest snapshot <= maxSnapshotVersionPresent - minVersionsToRetain + 1
        assert(snapshotVersionsPresent(remoteDir) === Seq(3, 5))
        assert(changelogVersionsPresent(remoteDir) == (3 to 5))

        for (version <- 5 to 7) {
          db.load(version)
          db.commit()
        }
        assert(snapshotVersionsPresent(remoteDir) === Seq(3, 5))
        assert(changelogVersionsPresent(remoteDir) == (3 to 8))
        db.doMaintenance()
        // 5 is the latest snapshot <= maxSnapshotVersionPresent - minVersionsToRetain + 1
        assert(snapshotVersionsPresent(remoteDir) === Seq(5, 8))
        assert(changelogVersionsPresent(remoteDir) == (5 to 8))
      }
  }

  testWithStateStoreCheckpointIdsAndColumnFamilies(
    "RocksDB: purge version files with minVersionsToDelete > 0",
    TestWithBothChangelogCheckpointingEnabledAndDisabled) {
    case (enableStateStoreCheckpointIds, colFamiliesEnabled) =>
      val remoteDir = Utils.createTempDir().toString
      new File(remoteDir).delete() // to make sure that the directory gets created
      val conf = dbConf.copy(
        minVersionsToRetain = 3, minDeltasForSnapshot = 1, minVersionsToDelete = 3)
      withDB(remoteDir, conf = conf, useColumnFamilies = colFamiliesEnabled,
        enableStateStoreCheckpointIds = enableStateStoreCheckpointIds) { db =>
        // Commit 5 versions
        // stale versions: (1, 2)
        // keep versions: (3, 4, 5)
        for (version <- 0 to 4) {
          // Should upload latest snapshot but not delete any files
          // since number of stale versions < minVersionsToDelete
          db.load(version)
          db.commit()
          db.doMaintenance()
        }

        // Commit 1 more version
        // stale versions: (1, 2, 3)
        // keep versions: (4, 5, 6)
        db.load(5)
        db.commit()

        // Checkpoint directory before maintenance
        if (isChangelogCheckpointingEnabled) {
          assert(snapshotVersionsPresent(remoteDir) == (1 to 5))
          assert(changelogVersionsPresent(remoteDir) == (1 to 6))
        } else {
          assert(snapshotVersionsPresent(remoteDir) == (1 to 6))
        }

        // Should delete stale versions for zip files and change log files
        // since number of stale versions >= minVersionsToDelete
        db.doMaintenance()

        // Checkpoint directory after maintenance
        assert(snapshotVersionsPresent(remoteDir) == Seq(4, 5, 6))
        if (isChangelogCheckpointingEnabled) {
          assert(changelogVersionsPresent(remoteDir) == Seq(4, 5, 6))
        }
      }
  }

  testWithStateStoreCheckpointIdsAndColumnFamilies(
    "RocksDB: minDeltasForSnapshot",
    TestWithChangelogCheckpointingEnabled) {
    case (enableStateStoreCheckpointIds, colFamiliesEnabled) =>
      val remoteDir = Utils.createTempDir().toString
      new File(remoteDir).delete() // to make sure that the directory gets created
      val conf = dbConf.copy(enableChangelogCheckpointing = true, minDeltasForSnapshot = 3)
      val versionToUniqueId = new mutable.HashMap[Long, String]()
      withDB(remoteDir, conf = conf, useColumnFamilies = colFamiliesEnabled,
          enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
          versionToUniqueId = versionToUniqueId) { db =>
        for (version <- 0 to 1) {
          db.load(version, versionToUniqueId.get(version))
          db.commit()
          db.doMaintenance()
        }
        // Snapshot should not be created because minDeltasForSnapshot = 3
        assert(snapshotVersionsPresent(remoteDir) === Seq.empty)
        assert(changelogVersionsPresent(remoteDir) == Seq(1, 2))
        db.load(2, versionToUniqueId.get(2))
        db.commit()
        db.doMaintenance()
        assert(snapshotVersionsPresent(remoteDir) === Seq(3))
        db.load(3, versionToUniqueId.get(3))

        for (version <- 3 to 7) {
          db.load(version, versionToUniqueId.get(version))
          db.commit()
          db.doMaintenance()
        }
        assert(snapshotVersionsPresent(remoteDir) === Seq(3, 6))
        for (version <- 8 to 17) {
          db.load(version, versionToUniqueId.get(version))
          db.commit()
        }
        db.doMaintenance()
        assert(snapshotVersionsPresent(remoteDir) === Seq(3, 6, 18))
      }

      // pick up from the last snapshot and the next upload will be for version 21
      withDB(remoteDir, conf = conf, useColumnFamilies = colFamiliesEnabled,
          enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
          versionToUniqueId = versionToUniqueId) { db =>
        db.load(18, versionToUniqueId.get(18))
        db.commit()
        db.doMaintenance()
        assert(snapshotVersionsPresent(remoteDir) === Seq(3, 6, 18))

        for (version <- 19 to 20) {
          db.load(version, versionToUniqueId.get(version))
          db.commit()
        }
        db.doMaintenance()
        assert(snapshotVersionsPresent(remoteDir) === Seq(3, 6, 18, 21))
      }
  }

  testWithStateStoreCheckpointIdsAndColumnFamilies("SPARK-45419: Do not reuse SST files" +
    " in different RocksDB instances",
    TestWithChangelogCheckpointingEnabled) {
    case (enableStateStoreCheckpointIds, colFamiliesEnabled) =>
      val remoteDir = Utils.createTempDir().toString
      val conf = dbConf.copy(minDeltasForSnapshot = 0, compactOnCommit = false)
      new File(remoteDir).delete() // to make sure that the directory gets created
      val versionToUniqueId = new mutable.HashMap[Long, String]()
      withDB(remoteDir, conf = conf, useColumnFamilies = colFamiliesEnabled,
          enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
          versionToUniqueId = versionToUniqueId) { db =>
        for (version <- 0 to 2) {
          db.load(version, versionToUniqueId.get(version))
          db.put(version.toString, version.toString)
          db.commit()
        }
        // upload snapshot 3.zip
        db.doMaintenance()
        // Roll back to version 1 and start to process data.
        for (version <- 1 to 3) {
          db.load(version, versionToUniqueId.get(version))
          db.put(version.toString, version.toString)
          db.commit()
        }
        // Upload snapshot 4.zip, should not reuse the SST files in 3.zip
        db.doMaintenance()
      }

      withDB(remoteDir, conf = conf, useColumnFamilies = colFamiliesEnabled,
          enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
          versionToUniqueId = versionToUniqueId) { db =>
        // Open the db to verify that the state in 4.zip is no corrupted.
        db.load(4, versionToUniqueId.get(4))
      }
  }

  // A rocksdb instance with changelog checkpointing enabled should be able to load
  // an existing checkpoint without changelog.
  testWithStateStoreCheckpointIdsAndColumnFamilies(
    "RocksDB: changelog checkpointing backward compatibility",
    TestWithChangelogCheckpointingEnabled) { (enableStateStoreCheckpointIds, colFamiliesEnabled) =>
    val remoteDir = Utils.createTempDir().toString
    new File(remoteDir).delete() // to make sure that the directory gets created
    val disableChangelogCheckpointingConf =
      dbConf.copy(enableChangelogCheckpointing = false, minVersionsToRetain = 30)
    val versionToUniqueId = new mutable.HashMap[Long, String]()
    withDB(remoteDir, conf = disableChangelogCheckpointingConf,
      useColumnFamilies = colFamiliesEnabled,
      enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
      versionToUniqueId = versionToUniqueId) { db =>
      for (version <- 1 to 30) {
        db.load(version - 1, versionToUniqueId.get(version - 1))
        db.put(version.toString, version.toString)
        db.remove((version - 1).toString)
        db.commit()
      }
      assert(snapshotVersionsPresent(remoteDir) === (1 to 30))
    }

    // Now enable changelog checkpointing in a checkpoint created by a state store
    // that disable changelog checkpointing.
    val enableChangelogCheckpointingConf =
      dbConf.copy(enableChangelogCheckpointing = true, minVersionsToRetain = 30,
        minDeltasForSnapshot = 1)
    withDB(remoteDir, conf = enableChangelogCheckpointingConf,
      useColumnFamilies = colFamiliesEnabled,
      enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
      versionToUniqueId = versionToUniqueId) { db =>
      for (version <- 1 to 30) {
        db.load(version, versionToUniqueId.get(version))
        assert(db.iterator().map(toStr).toSet === Set((version.toString, version.toString)))
      }
      for (version <- 30 to 60) {
        db.load(version - 1, versionToUniqueId.get(version - 1))
        db.put(version.toString, version.toString)
        db.remove((version - 1).toString)
        db.commit()
      }
      assert(snapshotVersionsPresent(remoteDir) === (1 to 30))
      assert(changelogVersionsPresent(remoteDir) === (30 to 60))
      for (version <- 1 to 60) {
        db.load(version, versionToUniqueId.get(version), readOnly = true)
        assert(db.iterator().map(toStr).toSet === Set((version.toString, version.toString)))
      }

      // recommit 60 to ensure that acquireLock is released for maintenance
      for (version <- 60 to 60) {
        db.load(version - 1, versionToUniqueId.get(version - 1))
        db.put(version.toString, version.toString)
        db.remove((version - 1).toString)
        db.commit()
      }
      // Check that snapshots and changelogs get purged correctly.
      db.doMaintenance()
      assert(snapshotVersionsPresent(remoteDir) === Seq(30, 60))
      if (enableStateStoreCheckpointIds) {
        // recommit version 60 creates another changelog file with different unique id
        assert(changelogVersionsPresent(remoteDir) === (30 to 60) :+ 60)
      } else {
        assert(changelogVersionsPresent(remoteDir) === (30 to 60))
      }

      // Verify the content of retained versions.
      for (version <- 30 to 60) {
        db.load(version, versionToUniqueId.get(version), readOnly = true)
        assert(db.iterator().map(toStr).toSet === Set((version.toString, version.toString)))
      }
    }
  }

  testWithChangelogCheckpointingEnabled("RocksDB Fault Tolerance: correctly handle when there " +
    "are multiple snapshot files for the same version") {
    val enableStateStoreCheckpointIds = true
    val useColumnFamily = true
    val remoteDir = Utils.createTempDir().toString
    new File(remoteDir).delete() // to make sure that the directory gets created
    val enableChangelogCheckpointingConf =
      dbConf.copy(enableChangelogCheckpointing = true, minVersionsToRetain = 20,
        minDeltasForSnapshot = 3)

    // Simulate when there are multiple snapshot files for the same version
    // The first DB writes to version 0 with uniqueId
    val versionToUniqueId1 = new mutable.HashMap[Long, String]()
    withDB(remoteDir, conf = enableChangelogCheckpointingConf,
      useColumnFamilies = useColumnFamily,
      enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
      versionToUniqueId = versionToUniqueId1) { db =>
      db.load(0, versionToUniqueId1.get(0))
      db.put("a", "1") // write key a here
      db.commit()

      // Add some change log files after the snapshot
      for (version <- 2 to 5) {
        db.load(version - 1, versionToUniqueId1.get(version - 1))
        db.put(version.toString, version.toString) // update "1" -> "1", "2" -> "2", ...
        db.commit()
      }

      // doMaintenance uploads the snapshot
      db.doMaintenance()

      for (version <- 6 to 10) {
        db.load(version - 1, versionToUniqueId1.get(version - 1))
        db.put(version.toString, version.toString)
        db.commit()
      }
    }

    // versionToUniqueId1 should be non-empty, meaning the id is updated from rocksDB to the map
    assert(versionToUniqueId1.nonEmpty)

    // The second DB writes to version 0 with another uniqueId
    val versionToUniqueId2 = new mutable.HashMap[Long, String]()
    withDB(remoteDir, conf = enableChangelogCheckpointingConf,
      useColumnFamilies = useColumnFamily,
      enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
      versionToUniqueId = versionToUniqueId2) { db =>
      db.load(0, versionToUniqueId2.get(0))
      db.put("b", "2") // write key b here
      db.commit()
      // Add some change log files after the snapshot
      for (version <- 2 to 5) {
        db.load(version - 1, versionToUniqueId2.get(version - 1))
        db.put(version.toString, (version + 1).toString) // update "1" -> "2", "2" -> "3", ...
        db.commit()
      }

      // doMaintenance uploads the snapshot
      db.doMaintenance()

      for (version <- 6 to 10) {
        db.load(version - 1, versionToUniqueId2.get(version - 1))
        db.put(version.toString, (version + 1).toString)
        db.commit()
      }
    }

    // versionToUniqueId2 should be non-empty, meaning the id is updated from rocksDB to the map
    assert(versionToUniqueId2.nonEmpty)

    // During a load() with linage from the first rocksDB,
    // the DB should load with data in the first db
    withDB(remoteDir, conf = enableChangelogCheckpointingConf,
      useColumnFamilies = useColumnFamily,
      enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
      versionToUniqueId = versionToUniqueId1) { db =>
      db.load(10, versionToUniqueId1.get(10))
      assert(toStr(db.get("a")) === "1")
      for (version <- 2 to 10) {
        // first time we write version -> version
        // second time we write version -> version + 1
        // here since we are loading from the first db lineage, we should see version -> version
        assert(toStr(db.get(version.toString)) === version.toString)
      }
    }

    // During a load() with linage from the second rocksDB,
    // the DB should load with data in the second db
    withDB(remoteDir, conf = enableChangelogCheckpointingConf,
      useColumnFamilies = useColumnFamily,
      enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
      versionToUniqueId = versionToUniqueId2) { db =>
      db.load(10, versionToUniqueId2.get(10))
      assert(toStr(db.get("b")) === "2")
      for (version <- 2 to 10) {
        // first time we write version -> version
        // second time we write version -> version + 1
        // here since we are loading from the second db lineage,
        // we should see version -> version + 1
        assert(toStr(db.get(version.toString)) === (version + 1).toString)
      }
    }
  }

  // A rocksdb instance with changelog checkpointing disabled should be able to load
  // an existing checkpoint with changelog.
  testWithStateStoreCheckpointIdsAndColumnFamilies(
    "RocksDB: changelog checkpointing forward compatibility",
    TestWithChangelogCheckpointingEnabled) { (enableStateStoreCheckpointIds, colFamiliesEnabled) =>
    val remoteDir = Utils.createTempDir().toString
    new File(remoteDir).delete() // to make sure that the directory gets created
    val enableChangelogCheckpointingConf =
      dbConf.copy(enableChangelogCheckpointing = true, minVersionsToRetain = 20,
        minDeltasForSnapshot = 3)
    val versionToUniqueId = new mutable.HashMap[Long, String]()
    withDB(remoteDir, conf = enableChangelogCheckpointingConf,
      useColumnFamilies = colFamiliesEnabled,
      enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
      versionToUniqueId = versionToUniqueId) { db =>
      for (version <- 1 to 30) {
        db.load(version - 1, versionToUniqueId.get(version - 1))
        db.put(version.toString, version.toString)
        db.remove((version - 1).toString)
        db.commit()
      }
    }

    // Now disable changelog checkpointing in a checkpoint created by a state store
    // that enable changelog checkpointing.
    val disableChangelogCheckpointingConf =
      dbConf.copy(enableChangelogCheckpointing = false, minVersionsToRetain = 20,
        minDeltasForSnapshot = 1)
    withDB(remoteDir, conf = disableChangelogCheckpointingConf,
      useColumnFamilies = colFamiliesEnabled,
      enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
      versionToUniqueId = versionToUniqueId) { db =>
      for (version <- 1 to 30) {
        db.load(version, versionToUniqueId.get(version))
        assert(db.iterator().map(toStr).toSet === Set((version.toString, version.toString)))
      }
      for (version <- 31 to 60) {
        db.load(version - 1, versionToUniqueId.get(version - 1))
        db.put(version.toString, version.toString)
        db.remove((version - 1).toString)
        db.commit()
      }
      assert(changelogVersionsPresent(remoteDir) === (1 to 30))
      assert(snapshotVersionsPresent(remoteDir) === (31 to 60))
      for (version <- 1 to 60) {
        db.load(version, versionToUniqueId.get(version), readOnly = true)
        assert(db.iterator().map(toStr).toSet === Set((version.toString, version.toString)))
      }
      // Check that snapshots and changelogs get purged correctly.
      db.doMaintenance()
      assert(snapshotVersionsPresent(remoteDir) === (41 to 60))
      assert(changelogVersionsPresent(remoteDir) === Seq.empty)
      // Verify the content of retained versions.
      for (version <- 41 to 60) {
        db.load(version, versionToUniqueId.get(version), readOnly = true)
        assert(db.iterator().map(toStr).toSet === Set((version.toString, version.toString)))
      }
    }
  }

  testWithColumnFamilies(s"RocksDB: compression conf",
    TestWithBothChangelogCheckpointingEnabledAndDisabled) { colFamiliesEnabled =>
    val remoteDir = Utils.createTempDir().toString
    new File(remoteDir).delete() // to make sure that the directory gets created

    val conf = RocksDBConf().copy(compression = "zstd")
    withDB(remoteDir, conf = conf, useColumnFamilies = colFamiliesEnabled) { db =>
      assert(db.rocksDbOptions.compressionType() == CompressionType.ZSTD_COMPRESSION)
    }

    // Test the default is LZ4
    withDB(remoteDir, conf = RocksDBConf().copy(), useColumnFamilies = colFamiliesEnabled) { db =>
      assert(db.rocksDbOptions.compressionType() == CompressionType.LZ4_COMPRESSION)
    }
  }

  testWithStateStoreCheckpointIdsAndColumnFamilies(s"RocksDB: get, put, iterator, commit, load",
    TestWithBothChangelogCheckpointingEnabledAndDisabled) {
    case (enableStateStoreCheckpointIds, colFamiliesEnabled) =>
      def testOps(compactOnCommit: Boolean): Unit = {
        val remoteDir = Utils.createTempDir().toString
        new File(remoteDir).delete() // to make sure that the directory gets created

        val conf = RocksDBConf().copy(compactOnCommit = compactOnCommit)
        val versionToUniqueId = new mutable.HashMap[Long, String]()
        withDB(remoteDir, conf = conf, useColumnFamilies = colFamiliesEnabled,
          enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
          versionToUniqueId = versionToUniqueId) { db =>
          assert(db.get("a") === null)
          assert(iterator(db).isEmpty)

          db.put("a", "1")
          assert(toStr(db.get("a")) === "1")
          db.commit()
        }

        withDB(remoteDir, conf = conf, version = 0, useColumnFamilies = colFamiliesEnabled,
          enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
          versionToUniqueId = versionToUniqueId) { db =>
          // version 0 can be loaded again
          assert(toStr(db.get("a")) === null)
          assert(iterator(db).isEmpty)
        }

        withDB(remoteDir, conf = conf, version = 1, useColumnFamilies = colFamiliesEnabled,
          enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
          versionToUniqueId = versionToUniqueId) { db =>
          // version 1 data recovered correctly
          assert(toStr(db.get("a")) === "1")
          assert(db.iterator().map(toStr).toSet === Set(("a", "1")))

          // make changes but do not commit version 2
          db.put("b", "2")
          assert(toStr(db.get("b")) === "2")
          assert(db.iterator().map(toStr).toSet === Set(("a", "1"), ("b", "2")))
        }

        withDB(remoteDir, conf = conf, version = 1, useColumnFamilies = colFamiliesEnabled,
          enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
          versionToUniqueId = versionToUniqueId) { db =>
          // version 1 data not changed
          assert(toStr(db.get("a")) === "1")
          assert(db.get("b") === null)
          assert(db.iterator().map(toStr).toSet === Set(("a", "1")))

          // commit version 2
          db.put("b", "2")
          assert(toStr(db.get("b")) === "2")
          db.commit()
          assert(db.iterator().map(toStr).toSet === Set(("a", "1"), ("b", "2")))
        }

        withDB(remoteDir, conf = conf, version = 1, useColumnFamilies = colFamiliesEnabled,
          enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
          versionToUniqueId = versionToUniqueId) { db =>
          // version 1 data not changed
          assert(toStr(db.get("a")) === "1")
          assert(db.get("b") === null)
        }

        withDB(remoteDir, conf = conf, version = 2, useColumnFamilies = colFamiliesEnabled,
          enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
          versionToUniqueId = versionToUniqueId) { db =>
          // version 2 can be loaded again
          assert(toStr(db.get("b")) === "2")
          assert(db.iterator().map(toStr).toSet === Set(("a", "1"), ("b", "2")))

          db.load(1, versionToUniqueId.get(1))
          assert(toStr(db.get("b")) === null)
          assert(db.iterator().map(toStr).toSet === Set(("a", "1")))
        }
      }

      for (compactOnCommit <- Seq(false, true)) {
        withClue(s"compactOnCommit = $compactOnCommit") {
          testOps(compactOnCommit)
        }
      }
  }

  testWithStateStoreCheckpointIdsAndColumnFamilies(s"RocksDB: handle commit failures and aborts",
    TestWithBothChangelogCheckpointingEnabledAndDisabled) {
    case (enableStateStoreCheckpointIds, colFamiliesEnabled) =>
      val hadoopConf = new Configuration()
      hadoopConf.set(
        SQLConf.STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key,
        classOf[CreateAtomicTestManager].getName)
      val remoteDir = Utils.createTempDir().getAbsolutePath
      withDB(remoteDir, hadoopConf = hadoopConf, useColumnFamilies = colFamiliesEnabled,
        enableStateStoreCheckpointIds = enableStateStoreCheckpointIds) { db =>
        // Disable failure of output stream and generate versions
        CreateAtomicTestManager.shouldFailInCreateAtomic = false
        for (version <- 1 to 10) {
          db.load(version - 1)
          db.put(version.toString, version.toString) // update "1" -> "1", "2" -> "2", ...
          db.commit()
        }
        val version10Data = (1L to 10).map(_.toString).map(x => x -> x).toSet

        // Fail commit for next version and verify that reloading resets the files
        CreateAtomicTestManager.shouldFailInCreateAtomic = true
        db.load(10)
        db.put("11", "11")
        intercept[IOException] {
          quietly {
            db.commit()
          }
        }
        assert(db.load(10, readOnly = true).iterator().map(toStr).toSet === version10Data)
        CreateAtomicTestManager.shouldFailInCreateAtomic = false

        // Abort commit for next version and verify that reloading resets the files
        db.load(10)
        db.put("11", "11")
        db.rollback()
        assert(db.load(10, readOnly = true).iterator().map(toStr).toSet === version10Data)
      }
  }

  testWithStateStoreCheckpointIdsAndColumnFamilies("RocksDB close tests - " +
    "close before doMaintenance",
    TestWithBothChangelogCheckpointingEnabledAndDisabled) {
    case (enableStateStoreCheckpointIds, colFamiliesEnabled) =>
      val remoteDir = Utils.createTempDir().toString
      val conf = dbConf.copy(minDeltasForSnapshot = 1, compactOnCommit = false)
      new File(remoteDir).delete() // to make sure that the directory gets created
      withDB(remoteDir, conf = conf, useColumnFamilies = colFamiliesEnabled,
        enableStateStoreCheckpointIds = enableStateStoreCheckpointIds) { db =>
        db.load(0)
        db.put("foo", "bar")
        db.commit()
        // call close first and maintenance can be still be invoked in the context of the
        // maintenance task's thread pool
        db.close()
        db.doMaintenance()
      }
  }

  testWithStateStoreCheckpointIdsAndColumnFamilies("RocksDB close tests - " +
    "close after doMaintenance",
    TestWithBothChangelogCheckpointingEnabledAndDisabled) {
    case (enableStateStoreCheckpointIds, colFamiliesEnabled) =>
      val remoteDir = Utils.createTempDir().toString
      val conf = dbConf.copy(minDeltasForSnapshot = 1, compactOnCommit = false)
      new File(remoteDir).delete() // to make sure that the directory gets created
      withDB(remoteDir, conf = conf, useColumnFamilies = colFamiliesEnabled,
        enableStateStoreCheckpointIds = enableStateStoreCheckpointIds) { db =>
        db.load(0)
        db.put("foo", "bar")
        db.commit()
        // maintenance can be invoked in the context of the maintenance task's thread pool
        // and close is invoked after that
        db.doMaintenance()
        db.close()
      }
  }

  testWithChangelogCheckpointingEnabled("RocksDB: Unsupported Operations" +
    " with Changelog Checkpointing") {
    val dfsRootDir = new File(Utils.createTempDir().getAbsolutePath + "/state/1/1")
    val fileManager = new RocksDBFileManager(
      dfsRootDir.getAbsolutePath, Utils.createTempDir(), hadoopConf)
    val changelogWriter = fileManager.getChangeLogWriter(1)
    assert(changelogWriter.version === 1)

    val ex = intercept[UnsupportedOperationException] {
      changelogWriter.merge("a", "1")
    }

    assert(ex.getMessage.contains("state changelog writer v1"))
  }

  testWithChangelogCheckpointingEnabled("RocksDBFileManager: " +
    "background snapshot upload doesn't acquire RocksDB instance lock") {
    // Create a custom ExecutionContext
    implicit val ec: ExecutionContext = ExecutionContext
      .fromExecutor(Executors.newSingleThreadExecutor())

    val remoteDir = Utils.createTempDir().toString
    val conf = dbConf.copy(lockAcquireTimeoutMs = 10000, minDeltasForSnapshot = 0)
    new File(remoteDir).delete() // to make sure that the directory gets created

    withDB(remoteDir, conf = conf) { db =>
      db.load(0)
      db.put("0", "0")
      db.commit()

      // Acquire lock
      db.load(1)
      db.put("1", "1")

      // Run doMaintenance in another thread
      val maintenanceFuture = Future {
        db.doMaintenance()
      }

      val timeout = 5.seconds

      // Ensure that maintenance task runs without being blocked by task thread
      ThreadUtils.awaitResult(maintenanceFuture, timeout)
      assert(snapshotVersionsPresent(remoteDir) == Seq(1))

      // Release lock
      db.commit()
    }
  }

  testWithChangelogCheckpointingEnabled("RocksDBFileManager: read and write changelog") {
    val dfsRootDir = new File(Utils.createTempDir().getAbsolutePath + "/state/1/1")
    val fileManager = new RocksDBFileManager(
      dfsRootDir.getAbsolutePath, Utils.createTempDir(), hadoopConf)
    val changelogWriter = fileManager.getChangeLogWriter(1)
    assert(changelogWriter.version === 1)

    (1 to 5).foreach(i => changelogWriter.put(i.toString, i.toString))
    (2 to 4).foreach(j => changelogWriter.delete(j.toString))

    changelogWriter.commit()
    val changelogReader = fileManager.getChangelogReader(1)
    assert(changelogReader.version === 1)
    val entries = changelogReader.toSeq
    val expectedEntries = (1 to 5).map { i =>
      (RecordType.PUT_RECORD, i.toString.getBytes,
        i.toString.getBytes, StateStore.DEFAULT_COL_FAMILY_NAME)
    } ++ (2 to 4).map { j =>
      (RecordType.DELETE_RECORD, j.toString.getBytes,
        null, StateStore.DEFAULT_COL_FAMILY_NAME)
    }

    assert(entries.size == expectedEntries.size)
    entries.zip(expectedEntries).map{
      case (e1, e2) => assert(e1._1 === e2._1 && e1._2 === e2._2 && e1._3 === e2._3)
    }

    changelogReader.closeIfNeeded()
  }

  testWithChangelogCheckpointingEnabled("RocksDBFileManager: StateStoreChangelogReaderFactory " +
    "edge case") {
    val dfsRootDir = new File(Utils.createTempDir().getAbsolutePath + "/state/1/1")
    val fileManager = new RocksDBFileManager(
      dfsRootDir.getAbsolutePath, Utils.createTempDir(), hadoopConf)

    val checkpointUniqueId = Some(java.util.UUID.randomUUID.toString)
    val lineage: Array[LineageItem] = Array(
      LineageItem(1, java.util.UUID.randomUUID.toString),
      LineageItem(2, java.util.UUID.randomUUID.toString),
      LineageItem(3, java.util.UUID.randomUUID.toString)
    )

    // Create a v1 writer
    val changelogWriterV1 = fileManager.getChangeLogWriter(101)
    assert(changelogWriterV1.version === 1)
    changelogWriterV1.commit() // v1 with empty content

    val changelogReaderV1 = fileManager.getChangelogReader(101)
    assert(changelogReaderV1.version === 1) // getChangelogReader should return a v1 reader
    changelogReaderV1.closeIfNeeded()

    // Create a v2 writer
    val changelogWriterV2 = fileManager.getChangeLogWriter(102, useColumnFamilies = true)
    assert(changelogWriterV2.version === 2)
    changelogWriterV2.commit() // v2 with empty content

    val changelogReaderV2 = fileManager.getChangelogReader(102)
    assert(changelogReaderV2.version === 2) // getChangelogReader should return a v2 reader
    changelogReaderV2.closeIfNeeded()

    // Create a v3 writer
    val changelogWriterV3 = fileManager.getChangeLogWriter(
      103, useColumnFamilies = false, checkpointUniqueId, Some(lineage))
    assert(changelogWriterV3.version === 3)
    changelogWriterV3.commit() // v1 with empty content

    val changelogReaderV3 = fileManager.getChangelogReader(
      103, checkpointUniqueId = checkpointUniqueId)
    assert(changelogReaderV3.version === 3) // getChangelogReader should return a v3 reader
    assert(changelogReaderV3.lineage sameElements lineage)
    changelogReaderV3.closeIfNeeded()

    // Create a v4 writer
    val changelogWriterV4 = fileManager.getChangeLogWriter(
      104, useColumnFamilies = true, checkpointUniqueId, Some(lineage))
    assert(changelogWriterV4.version === 4)
    changelogWriterV4.commit() // v1 with empty content

    val changelogReaderV4 = fileManager.getChangelogReader(
      104, checkpointUniqueId = checkpointUniqueId)
    assert(changelogReaderV4.version === 4) // getChangelogReader should return a v4 reader
    assert(changelogReaderV4.lineage sameElements lineage)
    changelogReaderV4.closeIfNeeded()
  }

  testWithChangelogCheckpointingEnabled("RocksDBFileManager: changelog reader / writer " +
    "failure cases") {
    val dfsRootDir = new File(Utils.createTempDir().getAbsolutePath + "/state/1/1")
    val fileManager = new RocksDBFileManager(
      dfsRootDir.getAbsolutePath, Utils.createTempDir(), hadoopConf)
    // Failure case 1: reader writer version mismatch
    // Create a v1 writer
    val changelogWriterV1 = fileManager.getChangeLogWriter(101)
    assert(changelogWriterV1.version === 1)

    (1 to 5).foreach(i => changelogWriterV1.put(i.toString, i.toString))
    (2 to 4).foreach(j => changelogWriterV1.delete(j.toString))

    changelogWriterV1.commit()
    // Success case, when reading from the same file, a V1 reader should be constructed.
    val changelogReaderV1 = fileManager.getChangelogReader(101)
    assert(changelogReaderV1.version === 1)
    changelogReaderV1.closeIfNeeded()

    // Failure case, force creating a V3 reader.
    val dfsChangelogFile = PrivateMethod[Path](Symbol("dfsChangelogFile"))
    val codec = PrivateMethod[CompressionCodec](Symbol("codec"))
    var changelogFile = fileManager invokePrivate dfsChangelogFile(101L, None)
    val compressionCodec = fileManager invokePrivate codec()
    val fm = CheckpointFileManager.create(new Path(dfsRootDir.getAbsolutePath), new Configuration)
    val e = intercept[AssertionError] {
      new StateStoreChangelogReaderV3(fm, changelogFile, compressionCodec)
    }
    assert(e.getMessage.contains("Changelog version mismatch"))

    changelogFile = fileManager invokePrivate dfsChangelogFile(1L, None)
    // Failure case 2: readerFactory throw when reading from ckpt built in future Spark version
    // Create a v101 writer
    val changelogWriter = new TestStateStoreChangelogWriterV101(
      fm, changelogFile, compressionCodec)
    assert(changelogWriter.version === 101)

    changelogWriter.commit()

    // Failure case, force creating a V3 reader.
    val ex = intercept[SparkException] {
      fileManager.getChangelogReader(1)
    }
    checkError(
      ex,
      condition = "CANNOT_LOAD_STATE_STORE.INVALID_CHANGE_LOG_READER_VERSION",
      parameters = Map("version" -> 101.toString)
    )
    assert(ex.getMessage.contains("please upgrade your Spark"))
  }

  testWithChangelogCheckpointingEnabled("RocksDBFileManager: read and write changelog " +
      "with state checkpoint id enabled") {
    val dfsRootDir = new File(Utils.createTempDir().getAbsolutePath + "/state/1/1")
    val fileManager = new RocksDBFileManager(
      dfsRootDir.getAbsolutePath, Utils.createTempDir(), hadoopConf)
    val checkpointUniqueId = Some(java.util.UUID.randomUUID.toString)
    val lineage: Array[LineageItem] = Array(
      LineageItem(1, java.util.UUID.randomUUID.toString),
      LineageItem(2, java.util.UUID.randomUUID.toString),
      LineageItem(3, java.util.UUID.randomUUID.toString)
    )
    val changelogWriter = fileManager.getChangeLogWriter(
      3, useColumnFamilies = false, checkpointUniqueId, Some(lineage))
    assert(changelogWriter.version === 3)

    (1 to 5).foreach(i => changelogWriter.put(i.toString, i.toString))
    (2 to 4).foreach(j => changelogWriter.delete(j.toString))

    changelogWriter.commit()
    val changelogReader = fileManager.getChangelogReader(3, checkpointUniqueId)
    assert(changelogReader.version === 3)
    assert(changelogReader.lineage sameElements lineage)
    val entries = changelogReader.toSeq
    val expectedEntries = (1 to 5).map { i =>
      (RecordType.PUT_RECORD, i.toString.getBytes,
        i.toString.getBytes, StateStore.DEFAULT_COL_FAMILY_NAME)
    } ++ (2 to 4).map { j =>
      (RecordType.DELETE_RECORD, j.toString.getBytes,
        null, StateStore.DEFAULT_COL_FAMILY_NAME)
    }

    assert(entries.size == expectedEntries.size)
    entries.zip(expectedEntries).map{
      case (e1, e2) => assert(e1._1 === e2._1 && e1._2 === e2._2 && e1._3 === e2._3)
    }

    changelogReader.closeIfNeeded()
  }

  testWithChangelogCheckpointingEnabled(
    "RocksDBFileManager: read and write v2 changelog with default col family") {
    val dfsRootDir = new File(Utils.createTempDir().getAbsolutePath + "/state/1/1")
    val fileManager = new RocksDBFileManager(
      dfsRootDir.getAbsolutePath, Utils.createTempDir(), hadoopConf)
    val changelogWriter = fileManager.getChangeLogWriter(1, useColumnFamilies = true)
    assert(changelogWriter.version === 2)
    (1 to 5).foreach { i =>
      changelogWriter.put(i.toString, i.toString)
    }
    (1 to 5).foreach { i =>
      changelogWriter.merge(i.toString, i.toString)
    }

    (2 to 4).foreach { j =>
      changelogWriter.delete(j.toString)
    }

    changelogWriter.commit()
    val changelogReader = fileManager.getChangelogReader(1)
    assert(changelogReader.version === 2)
    val entries = changelogReader.toSeq
    val expectedEntries = (1 to 5).map { i =>
      (RecordType.PUT_RECORD, i.toString.getBytes, i.toString.getBytes)
    } ++ (1 to 5).map { i =>
      (RecordType.MERGE_RECORD, i.toString.getBytes, i.toString.getBytes)
    } ++ (2 to 4).map { j =>
      (RecordType.DELETE_RECORD, j.toString.getBytes, null)
    }

    assert(entries.size == expectedEntries.size)
    entries.zip(expectedEntries).map{
      case (e1, e2) => assert(e1._1 === e2._1 && e1._2 === e2._2 && e1._3 === e2._3)
    }

    changelogReader.closeIfNeeded()
  }

  testWithChangelogCheckpointingEnabled("RocksDBFileManager: read and write v2 changelog with " +
      "default col family and state checkpoint id enabled") {
    val dfsRootDir = new File(Utils.createTempDir().getAbsolutePath + "/state/1/1")
    val fileManager = new RocksDBFileManager(
      dfsRootDir.getAbsolutePath, Utils.createTempDir(), hadoopConf)
    val checkpointUniqueId = Some(java.util.UUID.randomUUID.toString)
    val lineage: Array[LineageItem] = Array(
      LineageItem(1, java.util.UUID.randomUUID.toString),
      LineageItem(2, java.util.UUID.randomUUID.toString),
      LineageItem(3, java.util.UUID.randomUUID.toString)
    )
    val changelogWriter = fileManager.getChangeLogWriter(
      1, useColumnFamilies = true, checkpointUniqueId, Some(lineage))
    assert(changelogWriter.version === 4)
    (1 to 5).foreach { i =>
      changelogWriter.put(i.toString, i.toString)
    }
    (1 to 5).foreach { i =>
      changelogWriter.merge(i.toString, i.toString)
    }

    (2 to 4).foreach { j =>
      changelogWriter.delete(j.toString)
    }

    changelogWriter.commit()
    val changelogReader = fileManager.getChangelogReader(1, checkpointUniqueId)
    assert(changelogReader.version === 4)
    assert(changelogReader.lineage sameElements lineage)
    val entries = changelogReader.toSeq
    val expectedEntries = (1 to 5).map { i =>
      (RecordType.PUT_RECORD, i.toString.getBytes, i.toString.getBytes)
    } ++ (1 to 5).map { i =>
      (RecordType.MERGE_RECORD, i.toString.getBytes, i.toString.getBytes)
    } ++ (2 to 4).map { j =>
      (RecordType.DELETE_RECORD, j.toString.getBytes, null)
    }

    assert(entries.size == expectedEntries.size)
    entries.zip(expectedEntries).map{
      case (e1, e2) => assert(e1._1 === e2._1 && e1._2 === e2._2 && e1._3 === e2._3)
    }

    changelogReader.closeIfNeeded()
  }

  testWithColumnFamilies("RocksDBFileManager: create init dfs directory with " +
    s"unknown number of keys",
    TestWithBothChangelogCheckpointingEnabledAndDisabled) { colFamiliesEnabled =>
    val dfsRootDir = new File(Utils.createTempDir().getAbsolutePath + "/state/1/1")
    try {
      val verificationDir = Utils.createTempDir().getAbsolutePath
      val fileManager = new RocksDBFileManager(
        dfsRootDir.getAbsolutePath, Utils.createTempDir(), hadoopConf)
      // Save a version of empty checkpoint files
      val cpFiles = Seq()
      generateFiles(verificationDir, cpFiles)
      assert(!dfsRootDir.exists())
      val fileMapping = new RocksDBFileMapping
      saveCheckpointFiles(fileManager, cpFiles, version = 1,
        numKeys = -1, fileMapping)
      // The dfs root dir is created even with unknown number of keys
      assert(dfsRootDir.exists())
      loadAndVerifyCheckpointFiles(fileManager, verificationDir, version = 1, Nil, -1, fileMapping)
    } finally {
      Utils.deleteRecursively(dfsRootDir)
    }
  }

  testWithChangelogCheckpointingEnabled("RocksDB: ensure that changelog files are written " +
    "and snapshots uploaded optionally with changelog format v2") {
    withTempDir { dir =>
      val remoteDir = Utils.createTempDir().toString
      val conf = dbConf.copy(minDeltasForSnapshot = 5, compactOnCommit = false)
      new File(remoteDir).delete() // to make sure that the directory gets created
      withDB(remoteDir, conf = conf, useColumnFamilies = true) { db =>
        db.createColFamilyIfAbsent("test")
        db.load(0)
        db.put("a", "1")
        db.put("b", "2")
        db.commit()
        assert(changelogVersionsPresent(remoteDir) == Seq(1))
        assert(snapshotVersionsPresent(remoteDir) == Seq(1))

        db.load(1)
        db.put("a", "3")
        db.put("c", "4")
        db.commit()

        assert(changelogVersionsPresent(remoteDir) == Seq(1, 2))
        assert(snapshotVersionsPresent(remoteDir) == Seq(1))

        db.removeColFamilyIfExists("test")
        db.load(2)
        db.remove("a")
        db.put("d", "5")
        db.commit()
        assert(changelogVersionsPresent(remoteDir) == Seq(1, 2, 3))
        assert(snapshotVersionsPresent(remoteDir) == Seq(1, 3))

        db.load(3)
        db.put("e", "6")
        db.remove("b")
        db.commit()
        assert(changelogVersionsPresent(remoteDir) == Seq(1, 2, 3, 4))
        assert(snapshotVersionsPresent(remoteDir) == Seq(1, 3))
      }
    }
  }

  test("RocksDB: ensure merge operation correctness") {
    withTempDir { dir =>
      val remoteDir = Utils.createTempDir().toString
      // minDeltasForSnapshot being 5 ensures that only changelog files are created
      // for the 3 commits below
      val conf = dbConf.copy(minDeltasForSnapshot = 5, compactOnCommit = false)
      new File(remoteDir).delete() // to make sure that the directory gets created
      withDB(remoteDir, conf = conf, useColumnFamilies = true) { db =>
        db.load(0)
        db.put("a", "1")
        db.merge("a", "2")
        db.commit()

        db.load(1)
        db.merge("a", "3")
        db.commit()

        db.load(2)
        db.remove("a")
        db.commit()

        db.load(1)
        assert(new String(db.get("a")) === "1,2")
        assert(db.iterator().map(toStr).toSet === Set(("a", "1,2")))

        db.load(2)
        assert(new String(db.get("a")) === "1,2,3")
        assert(db.iterator().map(toStr).toSet === Set(("a", "1,2,3")))

        db.load(3)
        assert(db.get("a") === null)
        assert(db.iterator().isEmpty)
      }
    }
  }

  testWithStateStoreCheckpointIdsAndColumnFamilies("RocksDBFileManager: delete orphan files",
    TestWithBothChangelogCheckpointingEnabledAndDisabled) {
    case (enableStateStoreCheckpointIds, colFamiliesEnabled) =>
    withTempDir { dir =>
      val dfsRootDir = dir.getAbsolutePath
      // Use 2 file managers here to emulate concurrent execution
      // that checkpoint the same version of state
      val fileManager = new RocksDBFileManager(
        dfsRootDir, Utils.createTempDir(), hadoopConf)
      val rocksDBFileMapping = new RocksDBFileMapping()
      val fileManager_ = new RocksDBFileManager(
        dfsRootDir, Utils.createTempDir(), hadoopConf)
      val sstDir = s"$dfsRootDir/SSTs"
      def numRemoteSSTFiles: Int = listFiles(sstDir).length
      val logDir = s"$dfsRootDir/logs"
      def numRemoteLogFiles: Int = listFiles(logDir).length

      // Save a version of checkpoint files
      val cpFiles1 = Seq(
        "001.sst" -> 10,
        "002.sst" -> 20,
        "other-file1" -> 100,
        "other-file2" -> 200,
        "archive/00001.log" -> 1000,
        "archive/00002.log" -> 2000
      )
      val uuid = enableStateStoreCheckpointIds match {
        case false => None
        case true => Some(UUID.randomUUID().toString)
      }
      saveCheckpointFiles(fileManager, cpFiles1, version = 1,
        numKeys = 101, rocksDBFileMapping, uuid)
      assert(fileManager.getLatestVersion() === 1)
      assert(numRemoteSSTFiles == 2) // 2 sst files copied
      assert(numRemoteLogFiles == 2)

      // Overwrite version 1, previous sst and log files will become orphan
      val cpFiles1_ = Seq(
        "001.sst" -> 10,
        "002.sst" -> 20,
        "other-file1" -> 100,
        "other-file2" -> 200,
        "archive/00002.log" -> 1000,
        "archive/00003.log" -> 2000
      )
      saveCheckpointFiles(fileManager_, cpFiles1_, version = 1,
        numKeys = 101, new RocksDBFileMapping(), uuid)
      assert(fileManager_.getLatestVersion() === 1)
      assert(numRemoteSSTFiles == 4)
      assert(numRemoteLogFiles == 4)

      // For orphan files cleanup test, add a sleep between 2 checkpoints.
      // We use file modification timestamp to find orphan files older than
      // any tracked files. Some file systems has timestamps in second precision.
      // Sleeping for 1.5s makes sure files from different versions has different timestamps.
      Thread.sleep(1500)
      // Save a version of checkpoint files
      val cpFiles2 = Seq(
        "003.sst" -> 10,
        "004.sst" -> 20,
        "other-file1" -> 100,
        "other-file2" -> 200,
        "archive/00004.log" -> 1000,
        "archive/00005.log" -> 2000
      )
      saveCheckpointFiles(fileManager_, cpFiles2,
        version = 2, numKeys = 121, new RocksDBFileMapping(), uuid)
      fileManager_.deleteOldVersions(1)
      assert(numRemoteSSTFiles <= 4) // delete files recorded in 1.zip
      assert(numRemoteLogFiles <= 5) // delete files recorded in 1.zip and orphan 00001.log

      Thread.sleep(1500)
      // Save a version of checkpoint files
      val cpFiles3 = Seq(
        "005.sst" -> 10,
        "other-file1" -> 100,
        "other-file2" -> 200,
        "archive/00006.log" -> 1000,
        "archive/00007.log" -> 2000
      )
      saveCheckpointFiles(fileManager_, cpFiles3,
        version = 3, numKeys = 131, new RocksDBFileMapping(), uuid)
      assert(fileManager_.getLatestVersion() === 3)
      fileManager_.deleteOldVersions(1)
      assert(numRemoteSSTFiles == 1)
      assert(numRemoteLogFiles == 2)
    }
  }

  testWithStateStoreCheckpointIdsAndColumnFamilies("RocksDBFileManager: don't delete " +
    s"orphan files when there is only 1 version",
    TestWithBothChangelogCheckpointingEnabledAndDisabled) {
    case (enableStateStoreCheckpointIds, colFamiliesEnabled) =>
    withTempDir { dir =>
      val dfsRootDir = dir.getAbsolutePath
      val fileManager = new RocksDBFileManager(
        dfsRootDir, Utils.createTempDir(), hadoopConf)
      (new File(dfsRootDir, "SSTs")).mkdir()
      (new File(dfsRootDir, "logs")).mkdir()

      val sstDir = s"$dfsRootDir/SSTs"
      def numRemoteSSTFiles: Int = listFiles(sstDir).length

      val logDir = s"$dfsRootDir/logs"
      def numRemoteLogFiles: Int = listFiles(logDir).length

      new File(sstDir, "orphan.sst").createNewFile()
      new File(logDir, "orphan.log").createNewFile()

      Thread.sleep(1500)
      // Save a version of checkpoint files
      val cpFiles1 = Seq(
        "001.sst" -> 10,
        "002.sst" -> 20,
        "other-file1" -> 100,
        "other-file2" -> 200,
        "archive/00001.log" -> 1000,
        "archive/00002.log" -> 2000
      )
      val rocksDBFileMapping = new RocksDBFileMapping()
      val uuid = if (enableStateStoreCheckpointIds) {
        Some(UUID.randomUUID().toString)
      } else {
        None
      }

      saveCheckpointFiles(
        fileManager, cpFiles1, version = 1, numKeys = 101, rocksDBFileMapping, uuid)
      fileManager.deleteOldVersions(1)
      // Should not delete orphan files even when they are older than all existing files
      // when there is only 1 version.
      assert(numRemoteSSTFiles == 3)
      assert(numRemoteLogFiles == 3)

      Thread.sleep(1500)
      // Save a version of checkpoint files
      val cpFiles2 = Seq(
        "003.sst" -> 10,
        "004.sst" -> 20,
        "other-file1" -> 100,
        "other-file2" -> 200,
        "archive/00003.log" -> 1000,
        "archive/00004.log" -> 2000
      )
      saveCheckpointFiles(
        fileManager, cpFiles2, version = 2, numKeys = 101, rocksDBFileMapping, uuid)
      assert(numRemoteSSTFiles == 5)
      assert(numRemoteLogFiles == 5)
      fileManager.deleteOldVersions(1)
      // Orphan files should be deleted now.
      assert(numRemoteSSTFiles == 2)
      assert(numRemoteLogFiles == 2)
    }
  }

  testWithStateStoreCheckpointIdsAndColumnFamilies("RocksDBFileManager: upload only " +
    "new immutable files",
    TestWithBothChangelogCheckpointingEnabledAndDisabled) {
    case (enableStateStoreCheckpointIds, colFamiliesEnabled) =>
      withTempDir { dir =>
        val dfsRootDir = dir.getAbsolutePath
        val verificationDir = Utils.createTempDir().getAbsolutePath // local dir to load checkpoints
        val fileManager = new RocksDBFileManager(
          dfsRootDir, Utils.createTempDir(), hadoopConf)
        val sstDir = s"$dfsRootDir/SSTs"
        def numRemoteSSTFiles: Int = listFiles(sstDir).length
        val logDir = s"$dfsRootDir/logs"
        def numRemoteLogFiles: Int = listFiles(logDir).length
        val fileMapping = new RocksDBFileMapping

        // Verify behavior before any saved checkpoints
        assert(fileManager.getLatestVersion() === 0)

        // Try to load incorrect versions
        intercept[FileNotFoundException] {
          fileManager.loadCheckpointFromDfs(1, Utils.createTempDir(), fileMapping)
        }

        // Save a version of checkpoint files
        val cpFiles1 = Seq(
          "sst-file1.sst" -> 10,
          "sst-file2.sst" -> 20,
          "other-file1" -> 100,
          "other-file2" -> 200,
          "archive/00001.log" -> 1000,
          "archive/00002.log" -> 2000
        )

        val uuid = if (enableStateStoreCheckpointIds) {
          Some(UUID.randomUUID().toString)
        } else {
          None
        }

        saveCheckpointFiles(
          fileManager, cpFiles1, version = 1, numKeys = 101, fileMapping, uuid)
        assert(fileManager.getLatestVersion() === 1)
        assert(numRemoteSSTFiles == 2) // 2 sst files copied
        assert(numRemoteLogFiles == 2) // 2 log files copied

        // Load back the checkpoint files into another local dir with existing files and verify
        generateFiles(verificationDir, Seq(
          "sst-file1.sst" -> 11, // files with same name but different sizes, should get overwritten
          "other-file1" -> 101,
          "archive/00001.log" -> 1001,
          "random-sst-file.sst" -> 100, // unnecessary files, should get deleted
          "random-other-file" -> 9,
          "00005.log" -> 101,
          "archive/00007.log" -> 101
        ))

        // as we are loading version 1 again, the previously committed 1.zip and
        // SST files would not be reused.
        loadAndVerifyCheckpointFiles(
          fileManager, verificationDir, version = 1, cpFiles1, 101, fileMapping, uuid)

        // Save SAME version again with different checkpoint files and load back again to verify
        // whether files were overwritten.
        val cpFiles1_ = Seq(
          "sst-file1.sst" -> 10, // same SST file as before, but will be uploaded again
          "sst-file2.sst" -> 25, // new SST file with same name as before, but different length
          "sst-file3.sst" -> 30, // new SST file
          "other-file1" -> 100, // same non-SST file as before, should not get copied
          "other-file2" -> 210, // new non-SST file with same name as before, but different length
          "other-file3" -> 300, // new non-SST file
          "archive/00001.log" -> 1000, // same log file as before, this should get reused
          "archive/00002.log" -> 2500, // new log file with same name but different length
          "archive/00003.log" -> 3000 // new log file
        )

        // upload version 1 again, new checkpoint will be created and SST files from
        // previously committed version 1 will not be reused.
        saveCheckpointFiles(fileManager, cpFiles1_,
          version = 1, numKeys = 1001, fileMapping, uuid)
        assert(numRemoteSSTFiles === 5, "shouldn't reuse old version 1 SST files" +
          " while uploading version 1 again") // 2 old + 3 new SST files
        assert(numRemoteLogFiles === 5, "shouldn't reuse old version 1 log files" +
          " while uploading version 1 again") // 2 old + 3 new log files

        // verify checkpoint state is correct
        loadAndVerifyCheckpointFiles(fileManager, verificationDir,
          version = 1, cpFiles1_, 1001, fileMapping, uuid)

        // Save another version and verify
        val cpFiles2 = Seq(
          "sst-file1.sst" -> 10, // same SST file as version 1, should be reused
          "sst-file2.sst" -> 25, // same SST file as version 1, should be reused
          "sst-file3.sst" -> 30, // same SST file as version 1, should be reused
          "sst-file4.sst" -> 40, // new sst file, should be uploaded
          "other-file4" -> 400,
          "archive/00004.log" -> 4000
        )
        saveCheckpointFiles(fileManager, cpFiles2,
          version = 2, numKeys = 1501, fileMapping, uuid)
        assert(numRemoteSSTFiles === 6) // 1 new file over earlier 5 files
        assert(numRemoteLogFiles === 6) // 1 new file over earlier 6 files
        loadAndVerifyCheckpointFiles(fileManager, verificationDir,
          version = 2, cpFiles2, 1501, fileMapping, uuid)

        // Loading an older version should work
        loadAndVerifyCheckpointFiles(
          fileManager, verificationDir, version = 1, cpFiles1_, 1001, fileMapping, uuid)

        // Loading incorrect version should fail
        intercept[FileNotFoundException] {
          loadAndVerifyCheckpointFiles(
            fileManager, verificationDir, version = 3, Nil, 1001, fileMapping, uuid)
        }

        // Loading 0 should delete all files
        require(verificationDir.list().length > 0)
        loadAndVerifyCheckpointFiles(
          fileManager, verificationDir, version = 0, Nil, 0, fileMapping, uuid)
      }
  }

  testWithStateStoreCheckpointIdsAndColumnFamilies("RocksDBFileManager: error writing " +
    s"[version].zip cancels the output stream",
    TestWithBothChangelogCheckpointingEnabledAndDisabled) {
    case (enableStateStoreCheckpointIds, colFamiliesEnabled) =>
    quietly {
      val hadoopConf = new Configuration()
      hadoopConf.set(
        SQLConf.STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key,
        classOf[CreateAtomicTestManager].getName)
      val dfsRootDir = Utils.createTempDir().getAbsolutePath
      val fileManager = new RocksDBFileManager(dfsRootDir, Utils.createTempDir(), hadoopConf)
      val cpFiles = Seq("sst-file1.sst" -> 10, "sst-file2.sst" -> 20, "other-file1" -> 100)
      CreateAtomicTestManager.shouldFailInCreateAtomic = true
      val uuid = if (enableStateStoreCheckpointIds) {
        Some(UUID.randomUUID().toString)
      } else {
        None
      }
      intercept[IOException] {
        saveCheckpointFiles(
          fileManager, cpFiles, version = 1, numKeys = 101, new RocksDBFileMapping(), uuid)
      }
      assert(CreateAtomicTestManager.cancelCalledInCreateAtomic)
    }
  }

  testWithStateStoreCheckpointIdsAndColumnFamilies("disallow concurrent updates to the same " +
    "RocksDB instance",
    TestWithBothChangelogCheckpointingEnabledAndDisabled) {
    case (enableStateStoreCheckpointIds, colFamiliesEnabled) =>
    quietly {
      val versionToUniqueId = new mutable.HashMap[Long, String]()
      withDB(
        Utils.createTempDir().toString,
        conf = dbConf.copy(lockAcquireTimeoutMs = 20),
        useColumnFamilies = colFamiliesEnabled,
        enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
        versionToUniqueId = versionToUniqueId) { db =>
        // DB has been loaded so current thread has already
        // acquired the lock on the RocksDB instance

        db.load(0, versionToUniqueId.get(0)) // Current thread should be able to load again

        // Another thread should not be able to load while current thread is using it
        var ex = intercept[SparkException] {
          ThreadUtils.runInNewThread("concurrent-test-thread-1") {
            db.load(0, versionToUniqueId.get(0))
          }
        }
        checkError(
          ex,
          condition = "CANNOT_LOAD_STATE_STORE.UNRELEASED_THREAD_ERROR",
          parameters = Map(
            "loggingId" -> "\\[Thread-\\d+\\]",
            "operationType" -> "load_store",
            "newAcquiredThreadInfo" -> "\\[ThreadId: Some\\(\\d+\\)\\]",
            "acquiredThreadInfo" -> "\\[ThreadId: Some\\(\\d+\\)\\]",
            "timeWaitedMs" -> "\\d+",
            "stackTraceOutput" -> "(?s).*"
          ),
          matchPVals = true
        )

        // Commit should release the instance allowing other threads to load new version
        db.commit()
        ThreadUtils.runInNewThread("concurrent-test-thread-2") {
          db.load(1, versionToUniqueId.get(1))
          db.commit()
        }

        // Another thread should not be able to load while current thread is using it
        db.load(2, versionToUniqueId.get(2))
        ex = intercept[SparkException] {
          ThreadUtils.runInNewThread("concurrent-test-thread-2") {
            db.load(2, versionToUniqueId.get(2))
          }
        }
        checkError(
          ex,
          condition = "CANNOT_LOAD_STATE_STORE.UNRELEASED_THREAD_ERROR",
          parameters = Map(
            "loggingId" -> "\\[Thread-\\d+\\]",
            "operationType" -> "load_store",
            "newAcquiredThreadInfo" -> "\\[ThreadId: Some\\(\\d+\\)\\]",
            "acquiredThreadInfo" -> "\\[ThreadId: Some\\(\\d+\\)\\]",
            "timeWaitedMs" -> "\\d+",
            "stackTraceOutput" -> "(?s).*"
          ),
          matchPVals = true
        )

        // Rollback should release the instance allowing other threads to load new version
        db.rollback()
        ThreadUtils.runInNewThread("concurrent-test-thread-3") {
          db.load(1, versionToUniqueId.get(1))
          db.commit()
        }
      }
    }
  }

  testWithColumnFamilies("ensure concurrent access lock is released after Spark task completes",
    TestWithBothChangelogCheckpointingEnabledAndDisabled) { colFamiliesEnabled =>
    RocksDBSuite.withSingletonDB {
      // Load a RocksDB instance, that is, get a lock inside a task and then fail
      quietly {
        intercept[Exception] {
          sparkContext.makeRDD[Int](1 to 1, 1).map { i =>
            RocksDBSuite.singleton.load(0)
            throw new Exception("fail this task to test lock release")
          }.count()
        }
      }

      // Test whether you can load again, that is, will it successfully lock again
      RocksDBSuite.singleton.load(0)
    }
  }

  testWithColumnFamilies("checkpoint metadata serde roundtrip",
    TestWithBothChangelogCheckpointingEnabledAndDisabled) { colFamiliesEnabled =>
    // expect read metadata error when metadata uses unsupported version
    withTempDir { dir =>
      val file2 = new File(dir, "json")
      val json2 = """{"sstFiles":[],"numKeys":0}"""
      FileUtils.write(file2, s"v2\n$json2", Charset.defaultCharset)
      val e = intercept[SparkException] {
        RocksDBCheckpointMetadata.readFromFile(file2)
      }
      checkError(
        e,
        condition = "CANNOT_LOAD_STATE_STORE.CANNOT_READ_CHECKPOINT",
        parameters = Map(
          "expectedVersion" -> "v1",
          "actualVersion" -> "v2"
        )
      )
    }

    def checkJsonRoundtrip(metadata: RocksDBCheckpointMetadata, json: String): Unit = {
      assert(metadata.json == json)
      withTempDir { dir =>
        val file = new File(dir, "json")
        FileUtils.write(file, s"v1\n$json", Charset.defaultCharset)
        assert(metadata == RocksDBCheckpointMetadata.readFromFile(file))
      }
    }
    val sstFiles = Seq(RocksDBSstFile("00001.sst", "00001-uuid.sst", 12345678901234L))
    val logFiles = Seq(RocksDBLogFile("00001.log", "00001-uuid.log", 12345678901234L))

    // scalastyle:off line.size.limit
    // should always include sstFiles and numKeys
    checkJsonRoundtrip(
      RocksDBCheckpointMetadata(Seq.empty, 0L),
      """{"sstFiles":[],"numKeys":0}"""
    )
    // shouldn't include the "logFiles" field in json when it's empty
    checkJsonRoundtrip(
      RocksDBCheckpointMetadata(sstFiles, 12345678901234L),
      """{"sstFiles":[{"localFileName":"00001.sst","dfsSstFileName":"00001-uuid.sst","sizeBytes":12345678901234}],"numKeys":12345678901234}"""
    )
    checkJsonRoundtrip(
      RocksDBCheckpointMetadata(sstFiles, logFiles, 12345678901234L),
      """{"sstFiles":[{"localFileName":"00001.sst","dfsSstFileName":"00001-uuid.sst","sizeBytes":12345678901234}],"logFiles":[{"localFileName":"00001.log","dfsLogFileName":"00001-uuid.log","sizeBytes":12345678901234}],"numKeys":12345678901234}""")
    // scalastyle:on line.size.limit
  }

  testWithColumnFamilies("SPARK-36236: reset RocksDB metrics whenever a new version is loaded",
    TestWithBothChangelogCheckpointingEnabledAndDisabled) { colFamiliesEnabled =>
    def verifyMetrics(putCount: Long, getCount: Long, iterCountPositive: Boolean = false,
                      metrics: RocksDBMetrics): Unit = {
      assert(metrics.nativeOpsHistograms("put").count === putCount, "invalid put count")
      assert(metrics.nativeOpsHistograms("get").count === getCount, "invalid get count")
      if (iterCountPositive) {
        assert(metrics.nativeOpsMetrics("totalBytesReadThroughIterator") > 0)
      } else {
        assert(metrics.nativeOpsMetrics("totalBytesReadThroughIterator") === 0)
      }

      // most of the time get reads from WriteBatch which is not counted in this metric
      assert(metrics.nativeOpsMetrics("totalBytesRead") >= 0)
      assert(metrics.nativeOpsMetrics("totalBytesWritten") >= putCount * 1)

      assert(metrics.nativeOpsHistograms("compaction") != null)
      assert(metrics.nativeOpsMetrics("readBlockCacheMissCount") >= 0)
      assert(metrics.nativeOpsMetrics("readBlockCacheHitCount") >= 0)

      assert(metrics.nativeOpsMetrics("writerStallDuration") >= 0)
      assert(metrics.nativeOpsMetrics("totalBytesReadByCompaction") >= 0)
      assert(metrics.nativeOpsMetrics("totalBytesWrittenByCompaction") >=0)

      assert(metrics.nativeOpsMetrics("totalBytesWrittenByFlush") >= 0)
    }

    withTempDir { dir =>
      val remoteDir = dir.getCanonicalPath
      withDB(remoteDir, useColumnFamilies = colFamiliesEnabled) { db =>
        db.load(0)
        db.put("a", "1") // put also triggers a db get
        db.get("a") // this is found in-memory writebatch - no get triggered in db
        db.get("b") // key doesn't exists - triggers db get
        db.commit()
        verifyMetrics(putCount = 1, getCount = 3, metrics = db.metricsOpt.get)

        db.load(1)
        db.put("b", "2") // put also triggers a db get
        db.get("a") // not found in-memory writebatch, so triggers a db get
        db.get("c") // key doesn't exists - triggers db get
        assert(iterator(db).toSet === Set(("a", "1"), ("b", "2")))
        db.commit()
        verifyMetrics(putCount = 1, getCount = 3, iterCountPositive = true, db.metricsOpt.get)
      }
    }

    // disable resetting stats
    withTempDir { dir =>
      val remoteDir = dir.getCanonicalPath
      withDB(remoteDir,
        conf = dbConf.copy(resetStatsOnLoad = false),
        useColumnFamilies = colFamiliesEnabled) { db =>
        db.load(0)
        db.put("a", "1") // put also triggers a db get
        db.commit()
        // put and get counts are cumulative
        verifyMetrics(putCount = 1, getCount = 1, metrics = db.metricsOpt.get)

        db.load(1)
        db.put("b", "2") // put also triggers a db get
        db.get("a")
        db.commit()
        // put and get counts are cumulative: existing get=1, put=1: new get=2, put=1
        verifyMetrics(putCount = 2, getCount = 3, metrics = db.metricsOpt.get)
      }
    }

    // force compaction and check the compaction metrics
    withTempDir { dir =>
      val remoteDir = dir.getCanonicalPath
      withDB(remoteDir,
        conf = RocksDBConf().copy(compactOnCommit = true),
        useColumnFamilies = colFamiliesEnabled) { db =>
        db.load(0)
        db.put("a", "5")
        db.put("b", "5")
        db.commit()

        db.load(1)
        db.put("a", "10")
        db.put("b", "25")
        db.commit()

        val metrics = db.metricsOpt.get
        assert(metrics.nativeOpsHistograms("compaction").count > 0)
        assert(metrics.nativeOpsMetrics("totalBytesReadByCompaction") > 0)
        assert(metrics.nativeOpsMetrics("totalBytesWrittenByCompaction") > 0)
        assert(metrics.pinnedBlocksMemUsage >= 0)
      }
    }
  }

  // Add tests to check valid and invalid values for max_open_files passed to the underlying
  // RocksDB instance.
  Seq("-1", "100", "1000").foreach { maxOpenFiles =>
    testWithColumnFamilies(s"SPARK-39781: adding valid max_open_files=$maxOpenFiles " +
      "config property for RocksDB state store instance should succeed",
      TestWithBothChangelogCheckpointingEnabledAndDisabled) { colFamiliesEnabled =>
      withTempDir { dir =>
        val sqlConf = SQLConf.get.clone()
        sqlConf.setConfString("spark.sql.streaming.stateStore.rocksdb.maxOpenFiles", maxOpenFiles)
        val dbConf = RocksDBConf(StateStoreConf(sqlConf))
        assert(dbConf.maxOpenFiles === maxOpenFiles.toInt)

        val remoteDir = dir.getCanonicalPath
        withDB(remoteDir, conf = dbConf,
          useColumnFamilies = colFamiliesEnabled) { db =>
          // Do some DB ops
          db.load(0)
          db.put("a", "1")
          db.commit()
          assert(toStr(db.get("a")) === "1")
        }
      }
    }
  }

  Seq("test", "true").foreach { maxOpenFiles =>
    testWithColumnFamilies(s"SPARK-39781: adding invalid max_open_files=$maxOpenFiles config " +
      "property for RocksDB state store instance should fail",
      TestWithBothChangelogCheckpointingEnabledAndDisabled) { colFamiliesEnabled =>
      withTempDir { dir =>
        val ex = intercept[IllegalArgumentException] {
          val sqlConf = SQLConf.get.clone()
          sqlConf.setConfString("spark.sql.streaming.stateStore.rocksdb.maxOpenFiles",
            maxOpenFiles)
          val dbConf = RocksDBConf(StateStoreConf(sqlConf))
          assert(dbConf.maxOpenFiles === maxOpenFiles.toInt)

          val remoteDir = dir.getCanonicalPath
          withDB(remoteDir, conf = dbConf, useColumnFamilies = colFamiliesEnabled) { db =>
            // Do some DB ops
            db.load(0)
            db.put("a", "1")
            db.commit()
            assert(toStr(db.get("a")) === "1")
          }
        }
        assert(ex.getMessage.contains("Invalid value for"))
        assert(ex.getMessage.contains("must be an integer"))
      }
    }
  }

  Seq("1", "2", "3").foreach { maxWriteBufferNumber =>
    Seq("16", "32", "64").foreach {writeBufferSizeMB =>
      testWithColumnFamilies(s"SPARK-42819: configure memtable memory usage with " +
        s"maxWriteBufferNumber=$maxWriteBufferNumber and writeBufferSize=$writeBufferSizeMB",
        TestWithBothChangelogCheckpointingEnabledAndDisabled) { colFamiliesEnabled =>
        withTempDir { dir =>
          val sqlConf = new SQLConf
          sqlConf.setConfString("spark.sql.streaming.stateStore.rocksdb.maxWriteBufferNumber",
            maxWriteBufferNumber)
          sqlConf.setConfString("spark.sql.streaming.stateStore.rocksdb.writeBufferSizeMB",
            writeBufferSizeMB)
          val dbConf = RocksDBConf(StateStoreConf(sqlConf))
          assert(dbConf.maxWriteBufferNumber === maxWriteBufferNumber.toInt)
          assert(dbConf.writeBufferSizeMB === writeBufferSizeMB.toInt)

          val remoteDir = dir.getCanonicalPath
          withDB(remoteDir, conf = dbConf, useColumnFamilies = colFamiliesEnabled) { db =>
            // Do some DB ops
            db.load(0)
            db.put("a", "1")
            db.commit()
            assert(toStr(db.get("a")) === "1")
          }
        }
      }
    }
  }

  testWithColumnFamilies("Verify that fallocate is allowed by default",
    TestWithBothChangelogCheckpointingEnabledAndDisabled) { colFamiliesEnabled =>
     val sqlConf = new SQLConf
     val dbConf = RocksDBConf(StateStoreConf(sqlConf))
     assert(dbConf.allowFAllocate == true)
  }

 /** RocksDB memory management tests for bounded memory usage */
  testWithColumnFamilies("Memory mgmt - invalid config",
    TestWithBothChangelogCheckpointingEnabledAndDisabled) { colFamiliesEnabled =>
    withTempDir { dir =>
      try {
        RocksDBMemoryManager.resetWriteBufferManagerAndCache
        val sqlConf = new SQLConf
        sqlConf.setConfString(RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + "."
          + RocksDBConf.BOUNDED_MEMORY_USAGE_CONF_KEY, "true")
        sqlConf.setConfString(RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + "."
          + RocksDBConf.MAX_MEMORY_USAGE_MB_CONF_KEY, "100")
        sqlConf.setConfString(RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + "."
          + RocksDBConf.WRITE_BUFFER_CACHE_RATIO_CONF_KEY, "0.7")
        sqlConf.setConfString(RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + "."
          + RocksDBConf.HIGH_PRIORITY_POOL_RATIO_CONF_KEY, "0.6")

        val dbConf = RocksDBConf(StateStoreConf(sqlConf))
        assert(dbConf.boundedMemoryUsage === true)
        assert(dbConf.totalMemoryUsageMB === 100)
        assert(dbConf.writeBufferCacheRatio === 0.7)
        assert(dbConf.highPriorityPoolRatio === 0.6)

        val ex = intercept[Exception] {
          val remoteDir = dir.getCanonicalPath
          withDB(remoteDir, conf = dbConf, useColumnFamilies = colFamiliesEnabled) { db =>
            db.load(0)
            db.put("a", "1")
            db.commit()
          }
        }
        assert(ex.isInstanceOf[IllegalArgumentException])
        assert(ex.getMessage.contains("should be less than 1.0"))
      } finally {
        RocksDBMemoryManager.resetWriteBufferManagerAndCache
      }
    }
  }

  Seq("true", "false").foreach { boundedMemoryUsage =>
    testWithColumnFamilies(s"Memory mgmt - Cache reuse for RocksDB " +
      s"with boundedMemoryUsage=$boundedMemoryUsage",
      TestWithBothChangelogCheckpointingEnabledAndDisabled) { colFamiliesEnabled =>
      withTempDir { dir1 =>
        withTempDir { dir2 =>
          try {
            val sqlConf = new SQLConf
            sqlConf.setConfString(RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + "."
              + RocksDBConf.BOUNDED_MEMORY_USAGE_CONF_KEY, boundedMemoryUsage)

            val dbConf = RocksDBConf(StateStoreConf(sqlConf))
            assert(dbConf.boundedMemoryUsage === boundedMemoryUsage.toBoolean)

            val remoteDir1 = dir1.getCanonicalPath
            val (writeManager1, cache1) = withDB(remoteDir1,
              conf = dbConf,
              useColumnFamilies = colFamiliesEnabled) { db =>
              db.load(0)
              db.put("a", "1")
              db.commit()
              if (boundedMemoryUsage == "true") {
                assert(db.metricsOpt.get.totalMemUsageBytes === 0)
              } else {
                assert(db.metricsOpt.get.totalMemUsageBytes > 0)
              }
              db.getWriteBufferManagerAndCache()
            }

            val remoteDir2 = dir2.getCanonicalPath
            val (writeManager2, cache2) = withDB(remoteDir2,
              conf = dbConf,
              useColumnFamilies = colFamiliesEnabled) { db =>
              db.load(0)
              db.put("a", "1")
              db.commit()
              if (boundedMemoryUsage == "true") {
                assert(db.metricsOpt.get.totalMemUsageBytes === 0)
              } else {
                assert(db.metricsOpt.get.totalMemUsageBytes > 0)
              }
              db.getWriteBufferManagerAndCache()
            }

            if (boundedMemoryUsage == "true") {
              assert(writeManager1 === writeManager2)
              assert(cache1 === cache2)
            } else {
              assert(writeManager1 === null)
              assert(writeManager2 === null)
              assert(cache1 != cache2)
            }
          } finally {
            RocksDBMemoryManager.resetWriteBufferManagerAndCache
          }
        }
      }
    }
  }

  Seq("100", "1000", "100000").foreach { totalMemorySizeMB =>
    testWithColumnFamilies(s"Memory mgmt - valid config " +
      s"with totalMemorySizeMB=$totalMemorySizeMB",
      TestWithBothChangelogCheckpointingEnabledAndDisabled) { colFamiliesEnabled =>
      withTempDir { dir =>
        try {
          val sqlConf = new SQLConf
          sqlConf.setConfString(RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + "."
            + RocksDBConf.BOUNDED_MEMORY_USAGE_CONF_KEY, "true")
          sqlConf.setConfString(RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + "."
            + RocksDBConf.MAX_MEMORY_USAGE_MB_CONF_KEY, totalMemorySizeMB)
          sqlConf.setConfString(RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + "."
            + RocksDBConf.WRITE_BUFFER_CACHE_RATIO_CONF_KEY, "0.4")
          sqlConf.setConfString(RocksDBConf.ROCKSDB_SQL_CONF_NAME_PREFIX + "."
            + RocksDBConf.HIGH_PRIORITY_POOL_RATIO_CONF_KEY, "0.1")

          val dbConf = RocksDBConf(StateStoreConf(sqlConf))
          assert(dbConf.boundedMemoryUsage === true)
          assert(dbConf.totalMemoryUsageMB === totalMemorySizeMB.toLong)
          assert(dbConf.writeBufferCacheRatio === 0.4)
          assert(dbConf.highPriorityPoolRatio === 0.1)

          val remoteDir = dir.getCanonicalPath
          withDB(remoteDir, conf = dbConf, useColumnFamilies = colFamiliesEnabled) { db =>
            db.load(0)
            db.put("a", "1")
            db.put("b", "2")
            db.remove("a")
            db.put("c", "3")
            db.commit()
            assert(db.metricsOpt.get.totalMemUsageBytes === 0)
          }
        } finally {
          RocksDBMemoryManager.resetWriteBufferManagerAndCache
        }
      }
    }
  }

  testWithColumnFamilies("SPARK-37224: flipping option 'trackTotalNumberOfRows' during restart",
    TestWithBothChangelogCheckpointingEnabledAndDisabled) { colFamiliesEnabled =>
    withTempDir { dir =>
      val remoteDir = dir.getCanonicalPath

      var curVersion: Long = 0
      // starting with the config "trackTotalNumberOfRows = true"
      // this should track the number of rows correctly
      withDB(remoteDir,
        conf = dbConf.copy(trackTotalNumberOfRows = true),
        useColumnFamilies = colFamiliesEnabled) { db =>
        db.load(curVersion)
        db.put("a", "5")
        db.put("b", "5")

        curVersion = db.commit()

        assert(db.metricsOpt.get.numUncommittedKeys === 2)
        assert(db.metricsOpt.get.numCommittedKeys === 2)
      }

      // restart with config "trackTotalNumberOfRows = false"
      // this should reset the number of keys as -1, and keep the number as -1
      withDB(remoteDir,
        conf = dbConf.copy(trackTotalNumberOfRows = false),
        useColumnFamilies = colFamiliesEnabled) { db =>
        db.load(curVersion)

        db.put("b", "7")
        db.put("c", "7")

        curVersion = db.commit()

        assert(db.metricsOpt.get.numUncommittedKeys === -1)
        assert(db.metricsOpt.get.numCommittedKeys === -1)
      }

      // restart with config "trackTotalNumberOfRows = true" again
      // this should count the number of keys at the load phase, and continue tracking the number
      withDB(remoteDir,
        conf = dbConf.copy(trackTotalNumberOfRows = true),
        useColumnFamilies = colFamiliesEnabled) { db =>
        db.load(curVersion)

        db.put("c", "8")
        db.put("d", "8")

        curVersion = db.commit()

        assert(db.metricsOpt.get.numUncommittedKeys === 4)
        assert(db.metricsOpt.get.numCommittedKeys === 4)
      }
    }
  }

  testWithStateStoreCheckpointIds("time travel - " +
    "validate successful RocksDB load") { enableStateStoreCheckpointIds =>
    val remoteDir = Utils.createTempDir().toString
    val conf = dbConf.copy(minDeltasForSnapshot = 1, compactOnCommit = false)
    new File(remoteDir).delete() // to make sure that the directory gets created
    val versionToUniqueId = new mutable.HashMap[Long, String]()
    withDB(remoteDir, conf = conf, enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
      versionToUniqueId = versionToUniqueId) { db =>
      for (version <- 0 to 1) {
        db.load(version, versionToUniqueId.get(version))
        db.put(version.toString, version.toString)
        db.commit()
      }
      // upload snapshot 2.zip
      db.doMaintenance()
      for (version <- Seq(2)) {
        db.load(version, versionToUniqueId.get(version))
        db.put(version.toString, version.toString)
        db.commit()
      }
      // upload snapshot 3.zip
      db.doMaintenance()
      // simulate db in another executor that override the zip file
      // In checkpoint V2, reusing the same versionToUniqueId to simulate when two executors
      // are scheduled with the same uniqueId in the same microbatch
      withDB(remoteDir, conf = conf, enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
        versionToUniqueId = versionToUniqueId) { db1 =>
        for (version <- 0 to 1) {
          db1.load(version)
          db1.put(version.toString, version.toString)
          db1.commit()
        }
        db1.doMaintenance()
      }
      db.load(2, versionToUniqueId.get(2))
      for (version <- Seq(2)) {
        db.load(version, versionToUniqueId.get(version))
        db.put(version.toString, version.toString)
        db.commit()
      }
      // upload snapshot 3.zip
      db.doMaintenance()
      // rollback to version 2
      db.load(2, versionToUniqueId.get(2))
    }
  }

  testWithStateStoreCheckpointIds("time travel 2 - " +
    "validate successful RocksDB load") { enableStateStoreCheckpointIds =>
    Seq(1, 2).map(minDeltasForSnapshot => {
      val remoteDir = Utils.createTempDir().toString
      val conf = dbConf.copy(minDeltasForSnapshot = minDeltasForSnapshot,
        compactOnCommit = false)
      new File(remoteDir).delete() // to make sure that the directory gets created
      val versionToUniqueId = new mutable.HashMap[Long, String]()
      withDB(remoteDir, conf = conf, enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
        versionToUniqueId = versionToUniqueId) { db =>
        for (version <- 0 to 1) {
          db.load(version, versionToUniqueId.get(version))
          db.put(version.toString, version.toString)
          db.commit()
        }
        // upload snapshot 2.zip
        db.doMaintenance()
        for (version <- 2 to 3) {
          db.load(version, versionToUniqueId.get(version))
          db.put(version.toString, version.toString)
          db.commit()
        }
        db.load(0, versionToUniqueId.get(0))
        // simulate db in another executor that override the zip file
        // In checkpoint V2, reusing the same versionToUniqueId to simulate when two executors
        // are scheduled with the same uniqueId in the same microbatch
        withDB(remoteDir, conf = conf,
          enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
          versionToUniqueId = versionToUniqueId) { db1 =>
          for (version <- 0 to 1) {
            db1.load(version)
            db1.put(version.toString, version.toString)
            db1.commit()
          }
          db1.doMaintenance()
        }
        for (version <- 2 to 3) {
          db.load(version, versionToUniqueId.get(version))
          db.put(version.toString, version.toString)
          db.commit()
        }
        // upload snapshot 4.zip
        db.doMaintenance()
      }
      withDB(remoteDir, version = 4, conf = conf) { db =>
      }
    })
  }

  testWithStateStoreCheckpointIds("time travel 3 - validate" +
    " successful RocksDB load") { enableStateStoreCheckpointIds =>
    val remoteDir = Utils.createTempDir().toString
    val conf = dbConf.copy(minDeltasForSnapshot = 0, compactOnCommit = false)
    new File(remoteDir).delete() // to make sure that the directory gets created
    val versionToUniqueId = new mutable.HashMap[Long, String]()
    withDB(remoteDir, conf = conf, enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
      versionToUniqueId = versionToUniqueId) { db =>
      for (version <- 0 to 2) {
        db.load(version, versionToUniqueId.get(version))
        db.put(version.toString, version.toString)
        db.commit()
      }
      // upload snapshot 2.zip
      db.doMaintenance()
      for (version <- 1 to 3) {
        db.load(version, versionToUniqueId.get(version))
        db.put(version.toString, version.toString)
        db.commit()
      }
      // upload snapshot 4.zip
      db.doMaintenance()
    }

    withDB(remoteDir, version = 4, conf = conf) { db =>
    }
  }

  testWithStateStoreCheckpointIdsAndChangelogEnabled("time travel 4 - validate successful" +
    " RocksDB load when metadata file is overwritten") { enableStateStoreCheckpointIds =>
    val remoteDir = Utils.createTempDir().toString
    val conf = dbConf.copy(minDeltasForSnapshot = 2, compactOnCommit = false)
    new File(remoteDir).delete() // to make sure that the directory gets created
    val versionToUniqueId = new mutable.HashMap[Long, String]()
    withDB(remoteDir, conf = conf, enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
      versionToUniqueId = versionToUniqueId) { db =>
      for (version <- 0 to 1) {
        db.load(version, versionToUniqueId.get(version))
        db.put(version.toString, version.toString)
        db.commit()
      }

      // load previous version, and recreate the snapshot
      db.load(1, versionToUniqueId.get(1))
      db.put("3", "3")

      // upload any latest snapshots so far
      db.doMaintenance()
      db.commit()
      // upload newly created snapshot 2.zip
      db.doMaintenance()
    }

    // reload version 2 - should succeed
    withDB(remoteDir, version = 2, conf = conf) { db =>
    }
  }

  testWithStateStoreCheckpointIdsAndChangelogEnabled("time travel 5 - validate successful " +
    "RocksDB load when metadata file is not overwritten") { enableStateStoreCheckpointIds =>
    val fmClass = "org.apache.spark.sql.execution.streaming.state." +
      "NoOverwriteFileSystemBasedCheckpointFileManager"
    Seq(Some(fmClass), None).foreach { fm =>
      withTempDir { dir =>
        val conf = dbConf.copy(minDeltasForSnapshot = 0) // create snapshot every commit
        val hadoopConf = new Configuration()
        fm.foreach(value =>
          hadoopConf.set(STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key, value))
        val remoteDir = dir.getCanonicalPath
        val versionToUniqueId = new mutable.HashMap[Long, String]()
        withDB(remoteDir, conf = conf, hadoopConf = hadoopConf,
          enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
          versionToUniqueId = versionToUniqueId) { db =>
          db.load(0, versionToUniqueId.get(0))
          db.put("a", "1")
          db.commit()

          // load previous version, will recreate snapshot on commit
          db.load(0, versionToUniqueId.get(0))
          db.put("a", "1")

          // upload version 1 snapshot created previously
          db.doMaintenance()
          assert(snapshotVersionsPresent(remoteDir) == Seq(1))

          db.commit() // create snapshot again

          // load version 1 - should succeed
          withDB(remoteDir, version = 1, conf = conf, hadoopConf = hadoopConf) { db =>
          }

          // upload recently created snapshot
          db.doMaintenance()
          assert(snapshotVersionsPresent(remoteDir) == Seq(1))

          // load version 1 again - should succeed
          withDB(remoteDir, version = 1, conf = conf, hadoopConf = hadoopConf) { db =>
          }
        }
      }
    }
  }

  testWithStateStoreCheckpointIdsAndChangelogEnabled("reloading the " +
    "same version") { enableStateStoreCheckpointIds =>
    // Keep executing the same batch for two or more times. Some queries with ForEachBatch
    // will cause this behavior.
    // The test was accidentally fixed by SPARK-48586 (https://github.com/apache/spark/pull/47130)
    val remoteDir = Utils.createTempDir().toString
    val conf = dbConf.copy(minDeltasForSnapshot = 2, compactOnCommit = false)
    new File(remoteDir).delete() // to make sure that the directory gets created
    val versionToUniqueId = new mutable.HashMap[Long, String]()
    withDB(remoteDir, conf = conf, enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
      versionToUniqueId = versionToUniqueId) { db =>
      // load the same version of pending snapshot uploading
      // This is possible because after committing version x, we can continue to x+1, and replay
      // x+1. The replay will load a checkpoint by version x. At this moment, the snapshot
      // uploading may not be finished.
      // Previously this generated a problem: new files generated by reloading are added to
      // local -> cloud file map and the information is used to skip some files uploading, which is
      // wrong because these files aren't a part of the RocksDB checkpoint.
      // This test was accidentally fixed by
      // SPARK-48931 (https://github.com/apache/spark/pull/47393)

      db.load(0, versionToUniqueId.get(0))
      db.put("foo", "bar")
      // Snapshot checkpoint not needed
      db.commit()

      // Continue using local DB
      db.load(1, versionToUniqueId.get(1))
      db.put("foo", "bar")
      // Should create a local RocksDB snapshot
      db.commit()
      // Upload the local RocksDB snapshot to the cloud with 2.zip
      db.doMaintenance()

      // This will reload Db from the cloud.
      db.load(1, versionToUniqueId.get(1))
      db.put("foo", "bar")
      // Should create another local snapshot
      db.commit()

      // Continue using local DB
      db.load(2, versionToUniqueId.get(2))
      db.put("foo", "bar")
      // Snapshot checkpoint not needed
      db.commit()

      // Reload DB from the cloud, loading from 2.zip
      db.load(2, versionToUniqueId.get(2))
      db.put("foo", "bar")
      // Snapshot checkpoint not needed
      db.commit()

      // Will upload local snapshot and overwrite 2.zip
      db.doMaintenance()

      // Reload new 2.zip just uploaded to validate it is not corrupted.
      db.load(2, versionToUniqueId.get(2))
      db.put("foo", "bar")
      db.commit()

      // Test the maintenance thread is delayed even after the next snapshot is created.
      // There will be two outstanding snapshots.
      for (batchVersion <- 3 to 6) {
        db.load(batchVersion, versionToUniqueId.get(batchVersion))
        db.put("foo", "bar")
        // In batchVersion 3 and 5, it will generate a local snapshot but won't be uploaded.
        db.commit()
      }
      db.doMaintenance()

      // Test the maintenance is called after each batch. This tests a common case where
      // maintenance tasks finish quickly.
      for (batchVersion <- 7 to 10) {
        for (j <- 0 to 1) {
          db.load(batchVersion, versionToUniqueId.get(batchVersion))
          db.put("foo", "bar")
          db.commit()
          db.doMaintenance()
        }
      }
    }
  }

  for (randomSeed <- 1 to 8) {
    for (ifTestSkipBatch <- 0 to 1) {
      testWithStateStoreCheckpointIdsAndChangelogEnabled("randomized snapshotting " +
        s"$randomSeed ifTestSkipBatch $ifTestSkipBatch") { enableStateStoreCheckpointIds =>
          // The unit test simulates the case where batches can be reloaded and maintenance tasks
        // can be delayed. After each batch, we randomly decide whether we would move onto the
        // next batch, and whether maintenance task is executed.
        val remoteDir = Utils.createTempDir().toString
        val conf = dbConf.copy(minDeltasForSnapshot = 3, compactOnCommit = false)
        new File(remoteDir).delete() // to make sure that the directory gets created
        val versionToUniqueId = new mutable.HashMap[Long, String]()
        withDB(remoteDir, conf = dbConf,
          enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
          versionToUniqueId = versionToUniqueId) { db =>
          // A second DB is opened to simulate another executor that runs some batches that
          // skipped in the current DB.
          withDB(remoteDir, conf = dbConf,
            enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
            versionToUniqueId = versionToUniqueId) { db2 =>
            val random = new Random(randomSeed)
            var curVer: Int = 0
            for (i <- 1 to 100) {
              db.load(curVer, versionToUniqueId.get(curVer))
              db.put("foo", "bar")
              db.commit()
              // For a one in five chance, maintenance task is executed. The chance is created to
              // simulate the case where snapshot isn't immediatelly uploaded, and even delayed
              // so that the next snapshot is ready. We create a snapshot in every 3 batches, so
              // with 1/5 chance, it is more likely to create longer maintenance delay.
              if (random.nextInt(5) == 0) {
                db.doMaintenance()
              }
              // For half the chance, we move to the next version, and half the chance we keep the
              // same version. When the same version is kept, the DB will be reloaded.
              if (random.nextInt(2) == 0) {
                val inc = if (ifTestSkipBatch == 1) {
                  random.nextInt(3)
                } else {
                  1
                }
                if (inc > 1) {
                  // Create changelog files in the gap
                  for (j <- 1 to inc - 1) {
                    db2.load(curVer + j)
                    db2.put("foo", "bar")
                    db2.commit()
                  }
                }
                curVer = curVer + inc
              }
            }
          }
        }
      }
    }
  }

  testWithStateStoreCheckpointIds("validate Rocks DB SST files do not have a VersionIdMismatch" +
    " when metadata file is not overwritten - scenario 1") { enableStateStoreCheckpointIds =>
    val fmClass = "org.apache.spark.sql.execution.streaming.state." +
      "NoOverwriteFileSystemBasedCheckpointFileManager"
    withTempDir { dir =>
      val dbConf = RocksDBConf(StateStoreConf(new SQLConf()))
      val hadoopConf = new Configuration()
      hadoopConf.set(STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key, fmClass)

      val remoteDir = dir.getCanonicalPath
      val versionToUniqueId = new mutable.HashMap[Long, String]()
      withDB(remoteDir, conf = dbConf, hadoopConf = hadoopConf,
        enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
        versionToUniqueId = versionToUniqueId) { db1 =>
        withDB(remoteDir, conf = dbConf, hadoopConf = hadoopConf,
          enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
          versionToUniqueId = versionToUniqueId) { db2 =>
          // commit version 1 via db1
          db1.load(0, versionToUniqueId.get(0))
          db1.put("a", "1")
          db1.put("b", "1")

          db1.commit()

          // commit version 1 via db2
          db2.load(0, versionToUniqueId.get(0))
          db2.put("a", "1")
          db2.put("b", "1")

          db2.commit()

          // commit version 2 via db2
          db2.load(1, versionToUniqueId.get(1))
          db2.put("a", "2")
          db2.put("b", "2")

          db2.commit()

          // reload version 1, this should succeed
          db2.load(1, versionToUniqueId.get(1))
          db1.load(1, versionToUniqueId.get(1))

          // reload version 2, this should succeed
          db2.load(2, versionToUniqueId.get(2))
          db1.load(2, versionToUniqueId.get(2))
        }
      }
    }
  }

  testWithStateStoreCheckpointIds("validate Rocks DB SST files do not have a VersionIdMismatch" +
    " when metadata file is overwritten - scenario 1") { enableStateStoreCheckpointIds =>
    withTempDir { dir =>
      val dbConf = RocksDBConf(StateStoreConf(new SQLConf()))
      val hadoopConf = new Configuration()
      val remoteDir = dir.getCanonicalPath
      val versionToUniqueId = new mutable.HashMap[Long, String]()
      withDB(remoteDir, conf = dbConf, hadoopConf = hadoopConf,
        enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
        versionToUniqueId = versionToUniqueId) { db1 =>
        withDB(remoteDir, conf = dbConf, hadoopConf = hadoopConf,
          enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
          versionToUniqueId = versionToUniqueId) { db2 =>
          // commit version 1 via db1
          db1.load(0, versionToUniqueId.get(0))
          db1.put("a", "1")
          db1.put("b", "1")

          db1.commit()

          // commit version 1 via db2
          db2.load(0, versionToUniqueId.get(0))
          db2.put("a", "1")
          db2.put("b", "1")

          db2.commit()

          // commit version 2 via db2
          db2.load(1, versionToUniqueId.get(1))
          db2.put("a", "2")
          db2.put("b", "2")

          db2.commit()

          // reload version 1, this should succeed
          db2.load(1, versionToUniqueId.get(1))
          db1.load(1, versionToUniqueId.get(1))

          // reload version 2, this should succeed
          db2.load(2, versionToUniqueId.get(2))
          db1.load(2, versionToUniqueId.get(2))
        }
      }
    }
  }

  testWithStateStoreCheckpointIds("validate Rocks DB SST files do not have a VersionIdMismatch" +
    " when metadata file is not overwritten - scenario 2") { enableStateStoreCheckpointIds =>
    val fmClass = "org.apache.spark.sql.execution.streaming.state." +
      "NoOverwriteFileSystemBasedCheckpointFileManager"
    withTempDir { dir =>
      val dbConf = RocksDBConf(StateStoreConf(new SQLConf()))
      val hadoopConf = new Configuration()
      hadoopConf.set(STREAMING_CHECKPOINT_FILE_MANAGER_CLASS.parent.key, fmClass)

      val remoteDir = dir.getCanonicalPath
      val versionToUniqueId = new mutable.HashMap[Long, String]()
      withDB(remoteDir, conf = dbConf, hadoopConf = hadoopConf,
        enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
        versionToUniqueId = versionToUniqueId) { db1 =>
        withDB(remoteDir, conf = dbConf, hadoopConf = hadoopConf,
          enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
          versionToUniqueId = versionToUniqueId) { db2 =>
          // commit version 1 via db2
          db2.load(0, versionToUniqueId.get(0))
          db2.put("a", "1")
          db2.put("b", "1")

          db2.commit()

          // commit version 1 via db1
          db1.load(0, versionToUniqueId.get(0))
          db1.put("a", "1")
          db1.put("b", "1")

          db1.commit()

          // commit version 2 via db2
          db2.load(1, versionToUniqueId.get(1))
          db2.put("a", "2")
          db2.put("b", "2")

          db2.commit()

          // reload version 1, this should succeed
          db2.load(1, versionToUniqueId.get(1))
          db1.load(1, versionToUniqueId.get(1))

          // reload version 2, this should succeed
          db2.load(2, versionToUniqueId.get(2))
          db1.load(2, versionToUniqueId.get(2))
        }
      }
    }
  }

  testWithStateStoreCheckpointIds("validate Rocks DB SST files do not have a VersionIdMismatch" +
    " when metadata file is overwritten - scenario 2") { enableStateStoreCheckpointIds =>
    withTempDir { dir =>
      val dbConf = RocksDBConf(StateStoreConf(new SQLConf()))
      val hadoopConf = new Configuration()
      val remoteDir = dir.getCanonicalPath
      val versionToUniqueId = new mutable.HashMap[Long, String]()
      withDB(remoteDir, conf = dbConf, hadoopConf = hadoopConf,
        enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
        versionToUniqueId = versionToUniqueId) { db1 =>
        withDB(remoteDir, conf = dbConf, hadoopConf = hadoopConf,
          enableStateStoreCheckpointIds = enableStateStoreCheckpointIds,
          versionToUniqueId = versionToUniqueId) { db2 =>
          // commit version 1 via db2
          db2.load(0, versionToUniqueId.get(0))
          db2.put("a", "1")
          db2.put("b", "1")

          db2.commit()

          // commit version 1 via db1
          db1.load(0, versionToUniqueId.get(0))
          db1.put("a", "1")
          db1.put("b", "1")

          db1.commit()

          // commit version 2 via db2
          db2.load(1, versionToUniqueId.get(1))
          db2.put("a", "2")
          db2.put("b", "2")

          db2.commit()

          // reload version 1, this should succeed
          db2.load(1, versionToUniqueId.get(1))
          db1.load(1, versionToUniqueId.get(1))

          // reload version 2, this should succeed
          db2.load(2, versionToUniqueId.get(2))
          db1.load(2, versionToUniqueId.get(2))
        }
      }
    }
  }

  test("ensure local files deleted on filesystem" +
    " are cleaned from dfs file mapping") {
    def getSSTFiles(dir: File): Set[File] = {
      val sstFiles = new mutable.HashSet[File]()
      dir.listFiles().foreach { f =>
        if (f.isDirectory) {
          sstFiles ++= getSSTFiles(f)
        } else {
          if (f.getName.endsWith(".sst")) {
            sstFiles.add(f)
          }
        }
      }
      sstFiles.toSet
    }

    def filterAndDeleteSSTFiles(dir: File, filesToKeep: Set[File]): Unit = {
      dir.listFiles().foreach { f =>
        if (f.isDirectory) {
          filterAndDeleteSSTFiles(f, filesToKeep)
        } else {
          if (!filesToKeep.contains(f) && f.getName.endsWith(".sst")) {
            logInfo(s"deleting ${f.getAbsolutePath} from local directory")
            f.delete()
          }
        }
      }
    }

    withTempDir { dir =>
      withTempDir { localDir =>
        val sqlConf = new SQLConf()
        val dbConf = RocksDBConf(StateStoreConf(sqlConf))
        logInfo(s"config set to ${dbConf.compactOnCommit}")
        val hadoopConf = new Configuration()
        val remoteDir = dir.getCanonicalPath
        withDB(remoteDir = remoteDir,
          conf = dbConf,
          hadoopConf = hadoopConf,
          localDir = localDir) { db =>
          db.load(0)
          db.put("a", "1")
          db.put("b", "1")
          db.commit()
          db.doMaintenance()

          // find all SST files written in version 1
          val sstFiles = getSSTFiles(localDir)

          // make more commits, this would generate more SST files and write
          // them to remoteDir
          for (version <- 1 to 10) {
            db.load(version)
            db.put("c", "1")
            db.put("d", "1")
            db.commit()
            db.doMaintenance()
          }

          // clean the SST files committed after version 1 from local
          // filesystem. This is similar to what a process like compaction
          // where multiple L0 SST files can be merged into a single L1 file
          filterAndDeleteSSTFiles(localDir, sstFiles)

          // reload 2, and overwrite commit for version 3, this should not
          // reuse any locally deleted files as they should be removed from the mapping
          db.load(2)
          db.put("e", "1")
          db.put("f", "1")
          db.commit()
          db.doMaintenance()

          // clean local state
          db.load(0)

          // reload version 3, should be successful
          db.load(3)
        }
      }
    }
  }

  test("Rocks DB task completion listener does not double unlock acquireThread") {
    // This test verifies that a thread that locks then unlocks the db and then
    // fires a completion listener (Thread 1) does not unlock the lock validly
    // acquired by another thread (Thread 2).
    //
    // Timeline of this test (* means thread is active):
    // STATE | MAIN             | THREAD 1         | THREAD 2         |
    // ------| ---------------- | ---------------- | ---------------- |
    // 0.    | wait for s3      | *load, commit    | wait for s1      |
    //       |                  | *signal s1       |                  |
    // ------| ---------------- | ---------------- | ---------------- |
    // 1.    |                  | wait for s2      | *load, signal s2 |
    // ------| ---------------- | ---------------- | ---------------- |
    // 2.    |                  | *task complete   | wait for s4      |
    //       |                  | *signal s3, END  |                  |
    // ------| ---------------- | ---------------- | ---------------- |
    // 3.    | *verify locked   |                  |                  |
    //       | *signal s4       |                  |                  |
    // ------| ---------------- | ---------------- | ---------------- |
    // 4.    | wait for s5      |                  | *commit          |
    //       |                  |                  | *signal s5, END  |
    // ------| ---------------- | ---------------- | ---------------- |
    // 5.    | *close db, END   |                  |                  |
    //
    // NOTE: state 4 and 5 are only for cleanup

    // Create a custom ExecutionContext with 3 threads
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(
      ThreadUtils.newDaemonFixedThreadPool(3, "pool-thread-executor"))
    val stateLock = new Object()
    var state = 0

    withTempDir { dir =>
      val remoteDir = dir.getCanonicalPath
      val db = new RocksDB(
        remoteDir,
        conf = dbConf,
        localRootDir = Utils.createTempDir(),
        hadoopConf = new Configuration(),
        loggingId = s"[Thread-${Thread.currentThread.getId}]",
        useColumnFamilies = false
      )
      try {
        Future { // THREAD 1
          // Set thread 1's task context so that it is not a clone
          // of the main thread's taskContext, which will end if the
          // task is marked as complete
          val taskContext = TaskContext.empty()
          TaskContext.setTaskContext(taskContext)

          stateLock.synchronized {
            // -------------------- STATE 0 --------------------
            // Simulate a task that loads and commits, db should be unlocked after
            db.load(0)
            db.put("a", "1")
            db.commit()
            // Signal that we have entered state 1
            state = 1
            stateLock.notifyAll()

            // -------------------- STATE 2 --------------------
            // Wait until we have entered state 2 (thread 2 has loaded db and acquired lock)
            while (state != 2) {
              stateLock.wait()
            }

            // thread 1's task context is marked as complete and signal
            // that we have entered state 3
            // At this point, thread 2 should still hold the DB lock.
            taskContext.markTaskCompleted(None)
            state = 3
            stateLock.notifyAll()
          }
        }

        Future { // THREAD 2
          // Set thread 2's task context so that it is not a clone of thread 1's
          // so it won't be marked as complete
          val taskContext = TaskContext.empty()
          TaskContext.setTaskContext(taskContext)

          stateLock.synchronized {
            // -------------------- STATE 1 --------------------
            // Wait until we have entered state 1 (thread 1 finished loading and committing)
            while (state != 1) {
              stateLock.wait()
            }

            // Load the db and signal that we have entered state 2
            db.load(1)
            assertAcquiredThreadIsCurrentThread(db)
            state = 2
            stateLock.notifyAll()

            // -------------------- STATE 4 --------------------
            // Wait until we have entered state 4 (thread 1 completed and
            // main thread confirmed that lock is held)
            while (state != 4) {
              stateLock.wait()
            }

            // Ensure we still have the lock
            assertAcquiredThreadIsCurrentThread(db)

            // commit and signal that we have entered state 5
            db.commit()
            state = 5
            stateLock.notifyAll()
          }
        }

        // MAIN THREAD
        stateLock.synchronized {
          // -------------------- STATE 3 --------------------
          // Wait until we have entered state 3 (thread 1 is complete)
          while (state != 3) {
            stateLock.wait()
          }

          // Verify that the lock is being held
          val threadInfo = db.getAcquiredThreadInfo()
          assert(threadInfo.nonEmpty, s"acquiredThreadInfo was None when it should be Some")

          // Signal that we have entered state 4 (thread 2 can now release lock)
          state = 4
          stateLock.notifyAll()

          // -------------------- STATE 5 --------------------
          // Wait until we have entered state 5 (thread 2 has released lock)
          // so that we can clean up
          while (state != 5) {
            stateLock.wait()
          }
        }
      } finally {
        db.close()
      }
    }
  }

  test("RocksDB task completion listener correctly releases for failed task") {
    // This test verifies that a thread that locks the DB and then fails
    // can rely on the completion listener to release the lock.

    // Create a custom ExecutionContext with 1 thread
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(
      ThreadUtils.newDaemonSingleThreadExecutor("single-thread-executor"))
    val timeout = 5.seconds

    withTempDir { dir =>
      val remoteDir = dir.getCanonicalPath
      withDB(remoteDir) { db =>
        // Release the lock acquired by withDB
        db.commit()

        // New task that will load and then complete with failure
        val fut = Future {
          val taskContext = TaskContext.empty()
          TaskContext.setTaskContext(taskContext)

          db.load(0)
          assertAcquiredThreadIsCurrentThread(db)

          // Task completion listener should unlock
          taskContext.markTaskCompleted(
            Some(new SparkException("Task failure injection")))
        }

        ThreadUtils.awaitResult(fut, timeout)

        // Assert that db is not locked
        val threadInfo = db.getAcquiredThreadInfo()
        assert(threadInfo.isEmpty, s"acquiredThreadInfo should be None but was $threadInfo")
      }
    }
  }

  private def assertAcquiredThreadIsCurrentThread(db: RocksDB): Unit = {
    val threadInfo = db.getAcquiredThreadInfo()
    assert(threadInfo != None,
      "acquired thread info should not be null after load")
    val threadId = threadInfo.get.threadRef.get.get.getId
    assert(
      threadId == Thread.currentThread().getId,
      s"acquired thread should be curent thread ${Thread.currentThread().getId} " +
        s"after load but was $threadId")
  }

  private def dbConf = RocksDBConf(StateStoreConf(SQLConf.get.clone()))

  class RocksDBCheckpointFormatV2(
      dfsRootDir: String,
      conf: RocksDBConf,
      localRootDir: File = Utils.createTempDir(),
      hadoopConf: Configuration = new Configuration,
      loggingId: String = "",
      useColumnFamilies: Boolean = false,
      val versionToUniqueId : mutable.Map[Long, String] = mutable.Map[Long, String]())
    extends RocksDB(dfsRootDir, conf, localRootDir, hadoopConf, loggingId,
      useColumnFamilies, enableStateStoreCheckpointIds = true) {

    override def load(
        version: Long,
        ckptId: Option[String] = None,
        readOnly: Boolean = false): RocksDB = {
      // When a ckptId is defined, it means the test is explicitly using v2 semantic
      // When it is not, it is possible that implicitly uses it.
      // So still do a versionToUniqueId.get
      ckptId match {
        case Some(_) => super.load(version, ckptId, readOnly)
        case None => super.load(version, versionToUniqueId.get(version), readOnly)
      }
    }

    override def commit(): Long = {
      val ret = super.commit()
      // update versionToUniqueId from lineageManager
      lineageManager.getLineageForCurrVersion().foreach {
        case LineageItem(version, id) => versionToUniqueId.getOrElseUpdate(version, id)
      }
      ret
    }
  }

  // withDB override with checkpoint format v2
  def withDB[T](
      remoteDir: String,
      version: Int = 0,
      conf: RocksDBConf = dbConf,
      hadoopConf: Configuration = hadoopConf,
      useColumnFamilies: Boolean = false,
      enableStateStoreCheckpointIds: Boolean = false,
      // versionToUniqueId is used in checkpoint format v2, it simulates the lineage
      // stored in the commit log. The lineage will be automatically updated in db.commit()
      // When testing V2, please create a versionToUniqueId map
      // and call versionToUniqueId.get(version) in the db.load() function.
      // In V1, versionToUniqueId is not used and versionToUniqueId.get(version) returns None.
      versionToUniqueId : mutable.Map[Long, String] = mutable.Map[Long, String](),
      localDir: File = Utils.createTempDir())(
      func: RocksDB => T): T = {
    var db: RocksDB = null
    try {
      db = if (enableStateStoreCheckpointIds) {
        new RocksDBCheckpointFormatV2(
          remoteDir,
          conf = conf,
          localRootDir = localDir,
          hadoopConf = hadoopConf,
          loggingId = s"[Thread-${Thread.currentThread.getId}]",
          useColumnFamilies = useColumnFamilies,
          versionToUniqueId = versionToUniqueId)
      } else {
        new RocksDB(
          remoteDir,
          conf = conf,
          localRootDir = localDir,
          hadoopConf = hadoopConf,
          loggingId = s"[Thread-${Thread.currentThread.getId}]",
          useColumnFamilies = useColumnFamilies)
      }
      db.load(version, versionToUniqueId.get(version))
      func(db)
    } finally {
      if (db != null) {
        db.close()
      }
    }
  }

  def generateFiles(dir: String, fileToLengths: Seq[(String, Int)]): Unit = {
    fileToLengths.foreach { case (fileName, length) =>
      val file = new File(dir, fileName)
      FileUtils.write(file, "a" * length, Charset.defaultCharset)
    }
  }

  def saveCheckpointFiles(
      fileManager: RocksDBFileManager,
      fileToLengths: Seq[(String, Int)],
      version: Int,
      numKeys: Int,
      fileMapping: RocksDBFileMapping,
      checkpointUniqueId: Option[String] = None): Unit = {
    val checkpointDir = Utils.createTempDir().getAbsolutePath // local dir to create checkpoints
    generateFiles(checkpointDir, fileToLengths)
    val (dfsFileSuffix, immutableFileMapping) = fileMapping.createSnapshotFileMapping(
      fileManager, checkpointDir, version)
    fileManager.saveCheckpointToDfs(
      checkpointDir,
      version,
      numKeys,
      immutableFileMapping,
      checkpointUniqueId = checkpointUniqueId)

    val snapshotInfo = RocksDBVersionSnapshotInfo(version, dfsFileSuffix)
    fileMapping.snapshotsPendingUpload.remove(snapshotInfo)
  }

  def loadAndVerifyCheckpointFiles(
      fileManager: RocksDBFileManager,
      verificationDir: String,
      version: Int,
      expectedFiles: Seq[(String, Int)],
      expectedNumKeys: Int,
      fileMapping: RocksDBFileMapping,
      checkpointUniqueId: Option[String] = None): Unit = {
    val metadata = fileManager.loadCheckpointFromDfs(
      version, verificationDir, fileMapping, checkpointUniqueId)
    val filesAndLengths =
      listFiles(verificationDir).map(f => f.getName -> f.length).toSet ++
      listFiles(verificationDir + "/archive").map(f => s"archive/${f.getName}" -> f.length()).toSet
    assert(filesAndLengths === expectedFiles.toSet)
    assert(metadata.numKeys === expectedNumKeys)
  }

  implicit def toFile(path: String): File = new File(path)

  implicit def toArray(str: String): Array[Byte] = if (str != null) str.getBytes else null

  implicit def toStr(bytes: Array[Byte]): String = if (bytes != null) new String(bytes) else null

  def toStr(kv: ByteArrayPair): (String, String) = (toStr(kv.key), toStr(kv.value))

  def iterator(db: RocksDB): Iterator[(String, String)] = db.iterator().map(toStr)

  def listFiles(file: File): Seq[File] = {
    if (!file.exists()) return Seq.empty
    file.listFiles.filter(file => !file.getName.endsWith("crc") && !file.isDirectory)
      .toImmutableArraySeq
  }

  def listFiles(file: String): Seq[File] = listFiles(new File(file))
}

object RocksDBSuite {
  @volatile var singleton: RocksDB = _

  def withSingletonDB[T](func: => T): T = {
    try {
      singleton = new RocksDB(
        dfsRootDir = Utils.createTempDir().getAbsolutePath,
        conf = RocksDBConf().copy(compactOnCommit = false, minVersionsToRetain = 100),
        hadoopConf = new Configuration(),
        loggingId = s"[Thread-${Thread.currentThread.getId}]")

      func
    } finally {
      if (singleton != null) {
        singleton.close()
        singleton = null
      }
    }
  }
}
