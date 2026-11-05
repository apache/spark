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

package org.apache.spark.scheduler

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.zip.CRC32

import org.mockito.ArgumentMatchers.anyInt
import org.mockito.Mockito.{doThrow, mock, spy, when}
import org.roaringbitmap.RoaringBitmap

import org.apache.spark.{FetchFailed, SparkConf, SparkEnv, SparkFunSuite}
import org.apache.spark.internal.config
import org.apache.spark.shuffle.MetadataFetchFailedException
import org.apache.spark.storage.BlockManagerId

class MapStatusChecksumSuite extends SparkFunSuite {

  private val loc = BlockManagerId("exec-1", "host-1", 7337)
  private val CRC32Alg = MapStatusChecksum.CRC32_ALG
  private val ADLER32Alg = MapStatusChecksum.ADLER32
  private val failureMarker = "MapStatus checksum verification failed"

  test("the compressed metadata is the ids, a length prefix, and the compressed sizes") {
    val sizes = Array[Long](0, 1, 0, 2, 3, 0, 0, 4)
    val baseline = checksumOf(sizes, CRC32Alg)
    val reference = {
      val crc = new CRC32()
      crc.update(longBytes(-1L)) // mapTaskId, as `checksumOf` builds the status
      crc.update(longBytes(0L)) // checksumVal
      crc.update(intBytes(sizes.length))
      crc.update(sizes.map(MapStatus.compressSize))
      crc.getValue.toInt
    }
    assert(baseline === reference)

    def variant(i: Int, v: Long): Array[Long] = { val a = sizes.clone(); a(i) = v; a }
    assert(checksumOf(variant(1, 99L), CRC32Alg) !== baseline, "a size changed bucket")
    assert(checksumOf(variant(0, 42L), CRC32Alg) !== baseline, "empty became non-empty")
    assert(checksumOf(variant(1, 0L), CRC32Alg) !== baseline, "non-empty became empty")
    assert(checksumOf(sizes, ADLER32Alg) !== baseline, "different algorithm")
    // The length prefix catches truncation even when every entry is identical.
    assert(checksumOf(Array.fill[Long](128)(0L), CRC32Alg) !==
      checksumOf(Array.fill[Long](127)(0L), CRC32Alg))
  }

  test("the compact metadata matches an independent CRC32 reference implementation") {
    val cases = Seq(
      Array.fill[Long](100000)(1024),
      Array.fill[Long](100000)(0),
      Array.tabulate[Long](100000)(i => if (Set(1, 50000, 99999).contains(i)) 0 else 1024),
      Array.tabulate[Long](100000)(i => if (i % 2 == 0) 0 else 1024),
      Array.tabulate[Long](100000)(i => if (i < 4) 1L << 30 else 1024L))
    cases.foreach { sizes =>
      val status = compactStatus(sizes)
      val bytes = payload(status)

      val reference = new CRC32()
      reference.update(bytes)
      assert(reference.getValue.toInt === status.metadataChecksum)

      // The cost of a verification: three longs, three length prefixes, the skew size byte, the
      // two bitmaps and five bytes per accurately-recorded block. The partition count does not
      // appear, which is the whole point -- 100,000 partitions cost a few dozen bytes unless a
      // bitmap is genuinely dense.
      assert(bytes.length === 37
        + getField[RoaringBitmap](status, "emptyBlocks").serializedSizeInBytes()
        + getField[RoaringBitmap](status, "skewedBlocks").serializedSizeInBytes()
        + 5 * getField[Array[Int]](status, "hugeBlockIds").length)
    }
  }

  test("huge block ids are encoded in increasing order") {
    val status = hugeStatus()
    val ids = getField[Array[Int]](status, "hugeBlockIds")
    assert(ids.length > 1)
    assert(ids.toSeq === ids.sorted.toSeq, "the production path must emit sorted ids")

    // Reversed ids are metadata writeForChecksum refuses to encode, rather than hash into a
    // value that would depend on the order a reducer happened to rebuild them in.
    setField(status, "hugeBlockIds", ids.reverse)
    val e = intercept[MetadataFetchFailedException] {
      verifyChecksum(restampChecksum(status), checksumConf())
    }
    assert(e.getMessage.contains("invalid metadata"))
  }

  test("newChecksum rejects unsupported algorithm names") {
    intercept[IllegalArgumentException] {
      MapStatusChecksum.newChecksum("SHA256")
    }
  }

  test("verification never walks the partition space") {
    // Recomputing from `getSizeForBlock` for every partition -- a math.pow each on the
    // compressed path -- and once per reduce task, cost minutes per task at 50,000 x 100,000.
    val originals = Seq[MapStatus](
      stamped(Array.tabulate[Long](2000)(i => if (i % 3 == 0) 1024L else 0L), mapTaskId = 1L),
      compactStatus(Array.fill[Long](100000)(1024)))
    originals.foreach { original =>
      val status = spy[MapStatus](original)
      doThrow(new AssertionError("must not walk partitions"))
        .when(status).getSizeForBlock(anyInt())
      verifyChecksum(status, checksumConf())
    }
  }

  test("MapStatus.apply stamps the status according to the conf") {
    val sizes = Array[Long](0, 100, 0, 200, 0, 300)

    val crc = withSparkEnv(checksumConf(CRC32Alg)) { MapStatus(loc, sizes, 5L) }
    assert(crc.isInstanceOf[CompressedMapStatus])
    assert(crc.metadataChecksum === MapStatusChecksum.compute(crc, CRC32Alg))

    val adler = withSparkEnv(checksumConf(ADLER32Alg)) { MapStatus(loc, sizes, 5L) }
    assert(adler.metadataChecksum === MapStatusChecksum.compute(adler, ADLER32Alg))
    assert(adler.metadataChecksum !== crc.metadataChecksum)

    val large = Array.tabulate[Long](3000)(i => if (i % 100 == 0 && i > 0) i.toLong else 0L)
    val compact = withSparkEnv(checksumConf()) { MapStatus(loc, large, 9L) }
    assert(compact.isInstanceOf[HighlyCompressedMapStatus])
    assert(compact.metadataChecksum === MapStatusChecksum.compute(compact, CRC32Alg))
    assert(compact.metadataChecksum !== 0)

    // An all-empty stage is stamped too: a zero has to mean "feature off", never "no output".
    val allEmpty = Array.fill[Long](64)(0L)
    assert(withSparkEnv(checksumConf()) { MapStatus(loc, allEmpty, 5L) }.metadataChecksum ===
      checksumOf(allEmpty, CRC32Alg, mapTaskId = 5L))

    assert(withSparkEnv(disabledConf) { MapStatus(loc, sizes, 5L) }.metadataChecksum === 0)
    assert(withoutSparkEnv { MapStatus(loc, sizes, 5L) }.metadataChecksum === 0)
  }

  test("the checksum crosses the wire unchanged") {
    for (algorithm <- Seq(CRC32Alg, ADLER32Alg)) {
      val statuses = Seq[MapStatus](
        stamped(Array[Long](0, 100, 0, 200, 0, 300), mapTaskId = 11L, algorithm),
        compactStatus(Array.tabulate[Long](100001)(i =>
          if (i >= 65535 && i <= 65537) 0 else 1024), algorithm = algorithm))
      statuses.foreach { status =>
        val expected = status.metadataChecksum
        var copy = status
        (0 until 3).foreach { _ =>
          copy = javaRoundTrip(copy)
          // The location is rewritten when an executor is lost, after the stamp.
          copy.updateLocation(BlockManagerId("moved", "host-2", 7338))
          assert(copy.metadataChecksum === expected)
          verifyChecksum(copy, checksumConf(algorithm))
        }
      }
    }
  }

  test("verify checks every status, skips unregistered ones, and fails the whole shuffle") {
    val sizes = Array[Long](0, 100, 0, 200)
    def good(mapId: Long): MapStatus = stamped(sizes, mapId)
    MapStatusChecksum.verify(0, Array[MapStatus](good(0L), null, good(1L)), checksumConf())
    MapStatusChecksum.verify(0, Array[MapStatus](null, null), checksumConf())

    val bad = corruptChecksum(good(7L), checksumOf(sizes, CRC32Alg, mapTaskId = 7L) + 1)
    val e = intercept[MetadataFetchFailedException] {
      MapStatusChecksum.verify(3, Array[MapStatus](good(0L), null, bad, good(2L)), checksumConf())
    }
    assert(e.getMessage.contains(failureMarker))
    assert(e.getMessage.contains("shuffleId=3"))
    assert(e.getMessage.contains("mapIndex=2"))
    assert(e.getMessage.contains("mapId=7"))
    // Partition -1, the convention MapOutputTracker already uses for an undecodable status.
    assert(e.toTaskFailedReason.asInstanceOf[FetchFailed].reduceId === -1)

    verifyChecksum(bad, disabledConf)
  }

  test("verify detects corruption of any field the checksum covers") {
    val mutations: Seq[(String, HighlyCompressedMapStatus => Unit)] = Seq(
      "avgSize" -> (setField(_, "avgSize", 2048L)),
      "avgSize zeroed" -> (setField(_, "avgSize", 0L)),
      "mapTaskId" -> (setField(_, "_mapTaskId", 8L)),
      "checksumVal" -> (setField(_, "_checksumVal", 99L)),
      "emptyBlocks" -> (setField(_, "emptyBlocks", RoaringBitmap.bitmapOf(2))),
      "hugeBlockIds" -> (st => setField(st, "hugeBlockIds",
        getField[Array[Int]](st, "hugeBlockIds").map(_ + 1000))),
      "hugeBlockSizes" -> (st => setField(st, "hugeBlockSizes",
        getField[Array[Byte]](st, "hugeBlockSizes").map(b => (b ^ 1).toByte))),
      "skewedBlocks" -> (st => {
        setField(st, "skewedBlocks", RoaringBitmap.bitmapOf(90000))
        setField(st, "skewedBlockSize", 42.toByte)
      }),
      "skewedBlockSize" -> (st => {
        setField(st, "skewedBlocks", RoaringBitmap.bitmapOf(90000))
        setField(st, "skewedBlockSize", 43.toByte)
      }))
    mutations.foreach { case (field, mutate) =>
      val status = hugeStatus()
      val expected = status.metadataChecksum
      mutate(status)
      val forwarded = javaRoundTrip(status)
      assert(forwarded.metadataChecksum === expected, "serialization must forward, not refresh")
      withClue(field) {
        intercept[MetadataFetchFailedException] { verifyChecksum(forwarded, checksumConf()) }
      }
    }
  }

  test("verify fails closed on an unstamped status or the wrong algorithm") {
    val sizes = Array[Long](0, 1, 0, 2, 3, 0, 0, 4)
    intercept[MetadataFetchFailedException] {
      verifyChecksum(new CompressedMapStatus(loc, sizes, 1L, 0L), checksumConf())
    }
    val unstamped = HighlyCompressedMapStatus(loc, Array.fill[Long](3000)(1024), 7L)
    intercept[MetadataFetchFailedException] {
      verifyChecksum(unstamped, checksumConf())
    }
    val adler = stamped(sizes, mapTaskId = 1L, ADLER32Alg)
    verifyChecksum(adler, checksumConf(ADLER32Alg)) // must not throw
    intercept[MetadataFetchFailedException] {
      verifyChecksum(adler, checksumConf(CRC32Alg))
    }
  }

  test("metadata too inconsistent to encode fails verification") {
    // writeForChecksum refuses these rather than hashing nonsense.
    val invalid: Seq[(String, HighlyCompressedMapStatus => Unit)] = Seq(
      "zero-sized huge block" -> (st =>
        setField(st, "hugeBlockSizes",
          getField[Array[Byte]](st, "hugeBlockSizes").updated(0, 0.toByte))),
      "negative huge index" -> (st =>
        setField(st, "hugeBlockIds",
          getField[Array[Int]](st, "hugeBlockIds").updated(0, -1))),
      "misaligned huge arrays" -> (st =>
        setField(st, "hugeBlockSizes",
          getField[Array[Byte]](st, "hugeBlockSizes").drop(1))),
      "huge block also marked empty" -> (st =>
        setField(st, "emptyBlocks",
          RoaringBitmap.bitmapOf(getField[Array[Int]](st, "hugeBlockIds").head))),
      "skewed blocks with zero size" -> (st => {
        setField(st, "skewedBlocks", RoaringBitmap.bitmapOf(90000))
        setField(st, "skewedBlockSize", 0.toByte)
      }))
    invalid.foreach { case (what, mutate) =>
      val status = hugeStatus()
      mutate(status)
      withClue(what) {
        val e = intercept[MetadataFetchFailedException] {
          verifyChecksum(status, checksumConf())
        }
        assert(e.getMessage.contains("invalid metadata"))
      }
    }
    // All-huge output with avgSize 0 is legitimate, and must still verify.
    val allHuge = hugeStatus()
    setField(allHuge, "avgSize", 0L)
    verifyChecksum(restampChecksum(allHuge), checksumConf())
  }

  private def withSparkEnv[T](conf: SparkConf)(body: => T): T = {
    val prev = SparkEnv.get
    val env = mock(classOf[SparkEnv])
    when(env.conf).thenReturn(conf)
    SparkEnv.set(env)
    try body finally SparkEnv.set(prev)
  }

  private def withoutSparkEnv[T](body: => T): T = {
    val prev = SparkEnv.get
    SparkEnv.set(null)
    try body finally SparkEnv.set(prev)
  }

  private def checksumConf(algorithm: String = CRC32Alg): SparkConf =
    new SparkConf()
      .set(config.SHUFFLE_MAP_STATUS_CHECKSUM_ENABLED, true)
      .set(config.SHUFFLE_MAP_STATUS_CHECKSUM_ALGORITHM, algorithm)

  /** `loadDefaults = false` so the assertion survives an ambient property. */
  private def disabledConf: SparkConf =
    new SparkConf(loadDefaults = false)
      .set(config.SHUFFLE_MAP_STATUS_CHECKSUM_ENABLED, false)

  private def verifyChecksum(status: MapStatus, conf: SparkConf, shuffleId: Int = 1): Unit = {
    MapStatusChecksum.verify(shuffleId, Array(status), conf)
  }

  /** A status built the way a mapper with the feature on would build it. */
  private def stamped(
      sizes: Array[Long],
      mapTaskId: Long,
      algorithm: String = CRC32Alg): CompressedMapStatus = {
    withSparkEnv(checksumConf(algorithm)) {
      MapStatus(loc, sizes, mapTaskId).asInstanceOf[CompressedMapStatus]
    }
  }

  /** A compact status that really carries accurately-recorded (huge) blocks. */
  private def hugeStatus(): HighlyCompressedMapStatus =
    compactStatus(Array.tabulate[Long](100000)(i => if (i < 4) 1L << 30 else 1024L))

  /** As [[stamped]], for sizes past `spark.shuffle.minNumPartitionsToHighlyCompress`. */
  private def compactStatus(
      sizes: Array[Long],
      mapTaskId: Long = 7L,
      algorithm: String = CRC32Alg): HighlyCompressedMapStatus = {
    withSparkEnv(checksumConf(algorithm)) {
      MapStatus(loc, sizes, mapTaskId).asInstanceOf[HighlyCompressedMapStatus]
    }
  }

  private def checksumOf(sizes: Array[Long], algorithm: String, mapTaskId: Long = -1L): Int = {
    MapStatusChecksum.compute(new CompressedMapStatus(loc, sizes, mapTaskId, 0L), algorithm)
  }

  private def payload(status: MapStatus): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    val out = new DataOutputStream(bytes)
    status.writeForChecksum(out)
    out.flush()
    bytes.toByteArray
  }

  private def getField[T](status: MapStatus, name: String): T = {
    val field = status.getClass.getDeclaredField(name)
    field.setAccessible(true)
    field.get(status).asInstanceOf[T]
  }

  private def intBytes(v: Int): Array[Byte] =
    Array((v >>> 24).toByte, (v >>> 16).toByte, (v >>> 8).toByte, v.toByte)

  private def longBytes(v: Long): Array[Byte] =
    intBytes((v >>> 32).toInt) ++ intBytes(v.toInt)

  private def setField(status: MapStatus, name: String, value: Any): Unit = {
    val field = status.getClass.getDeclaredField(name)
    field.setAccessible(true)
    field.set(status, value)
  }

  private def corruptChecksum[T <: MapStatus](status: T, value: Int): T = {
    setField(status, "_metadataChecksum", value)
    status
  }

  private def restampChecksum(status: HighlyCompressedMapStatus): HighlyCompressedMapStatus = {
    corruptChecksum(status, MapStatusChecksum.compute(status, CRC32Alg))
  }

  private def javaRoundTrip[T <: MapStatus](status: T): T = {
    val bytes = javaBytes(status)
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val out = ois.readObject().asInstanceOf[T]
    ois.close()
    out
  }

  private def javaBytes(o: Any): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(o)
    oos.close()
    baos.toByteArray
  }
}
