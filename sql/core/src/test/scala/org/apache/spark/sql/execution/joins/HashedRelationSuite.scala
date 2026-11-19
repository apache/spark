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

package org.apache.spark.sql.execution.joins

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, IOException, ObjectInputStream, ObjectOutputStream}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.SparkEnv
import org.apache.spark.SparkException
import org.apache.spark.internal.config.{BUFFER_PAGESIZE, MEMORY_OFFHEAP_ENABLED, MEMORY_OFFHEAP_SIZE}
import org.apache.spark.internal.config.Kryo._
import org.apache.spark.memory.{MemoryConsumer, SparkOutOfMemoryError, TaskMemoryManager, UnifiedMemoryManager}
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.map.BytesToBytesMap
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.collection.CompactBuffer

abstract class HashedRelationSuite extends SharedSparkSession {
  protected def useOffHeapMemoryMode: Boolean

  val umm = new UnifiedMemoryManager(
    new SparkConf()
      .set(MEMORY_OFFHEAP_ENABLED, useOffHeapMemoryMode)
      .set(MEMORY_OFFHEAP_SIZE, ByteUnit.GiB.toBytes(1L)),
    Runtime.getRuntime.maxMemory,
    Runtime.getRuntime.maxMemory / 2,
    1)

  val mm = new TaskMemoryManager(umm, 0)

  val rand = new Random(100)

  // key arrays used for building different relations under test
  val contiguousArray = (0 until 1000)
  val sparseArray = (0 until 1000 by 10)
  val randomArray = (0 until 1000).filter(_ => rand.nextBoolean())

  val singleKey = Seq(BoundReference(0, LongType, false))
  val projection = UnsafeProjection.create(singleKey)

  // build the corresponding rows for each array type
  val contiguousRows = contiguousArray.map(i => projection(InternalRow(i.toLong)).copy())
  val sparseRows = sparseArray.map(i => projection(InternalRow(i.toLong)).copy())
  val randomRows = randomArray.map(i => projection(InternalRow(i.toLong)).copy())

  protected override def afterEach(): Unit = {
    super.afterEach()
    assert(umm.executionMemoryUsed === 0)
  }

  test("UnsafeHashedRelation") {
    val schema = StructType(StructField("a", IntegerType, true) :: Nil)
    val data = Array(InternalRow(0), InternalRow(1), InternalRow(2), InternalRow(2))
    val toUnsafe = UnsafeProjection.create(schema)
    val unsafeData = data.map(toUnsafe(_).copy())


    val buildKey = Seq(BoundReference(0, IntegerType, false))
    val hashed = UnsafeHashedRelation(unsafeData.iterator, buildKey, 1, mm)
    assert(hashed.isInstanceOf[UnsafeHashedRelation])

    assert(hashed.get(unsafeData(0)).toArray === Array(unsafeData(0)))
    assert(hashed.get(unsafeData(1)).toArray === Array(unsafeData(1)))
    assert(hashed.get(toUnsafe(InternalRow(10))) === null)

    val data2 = CompactBuffer[InternalRow](unsafeData(2).copy())
    data2 += unsafeData(2).copy()
    assert(hashed.get(unsafeData(2)).toArray === data2.toArray)

    val os = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(os)
    hashed.asInstanceOf[UnsafeHashedRelation].writeExternal(out)
    out.flush()
    hashed.close()
    val in = new ObjectInputStream(new ByteArrayInputStream(os.toByteArray))
    val hashed2 = new UnsafeHashedRelation()
    hashed2.readExternal(in)
    assert(hashed2.get(unsafeData(0)).toArray === Array(unsafeData(0)))
    assert(hashed2.get(unsafeData(1)).toArray === Array(unsafeData(1)))
    assert(hashed2.get(toUnsafe(InternalRow(10))) === null)
    assert(hashed2.get(unsafeData(2)).toArray === data2)

    // SPARK-38542: UnsafeHashedRelation should serialize numKeys out
    assert(hashed2.keys().map(_.copy()).forall(_.numFields == 1))

    val os2 = new ByteArrayOutputStream()
    val out2 = new ObjectOutputStream(os2)
    hashed2.writeExternal(out2)
    out2.flush()
    // This depends on that the order of items in BytesToBytesMap.iterator() is exactly the same
    // as they are inserted
    assert(java.util.Arrays.equals(os2.toByteArray, os.toByteArray))
  }

  test("test serialization empty hash map") {
    val binaryMap = new BytesToBytesMap(mm, 1, 1)
    val os = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(os)
    val hashed = new UnsafeHashedRelation(1, 1, binaryMap)
    hashed.writeExternal(out)
    out.flush()
    hashed.close()
    val in = new ObjectInputStream(new ByteArrayInputStream(os.toByteArray))
    val hashed2 = new UnsafeHashedRelation()
    hashed2.readExternal(in)

    val schema = StructType(StructField("a", IntegerType, true) :: Nil)
    val toUnsafe = UnsafeProjection.create(schema)
    val row = toUnsafe(InternalRow(0))
    assert(hashed2.get(row) === null)

    val os2 = new ByteArrayOutputStream()
    val out2 = new ObjectOutputStream(os2)
    hashed2.writeExternal(out2)
    out2.flush()
    assert(java.util.Arrays.equals(os2.toByteArray, os.toByteArray))
  }

  test("SPARK-61082: many UnsafeHashedRelations fit in one task when each holds little data") {
    // UnsafeHashedRelation takes its page size from SparkEnv, not from this memory manager. With a
    // page larger than 16 MiB, 16 relations rounded up to a full page each would need more than
    // the 256 MiB available.
    assert(SparkEnv.get.memoryManager.pageSizeBytes > ByteUnit.MiB.toBytes(16))
    val smallUmm = newMemoryManager(ByteUnit.MiB.toBytes(64), ByteUnit.MiB.toBytes(256))
    val smallMm = new TaskMemoryManager(smallUmm, 0)
    val buildKey = Seq(BoundReference(0, StringType, false))
    val toUnsafeRow = UnsafeProjection.create(Array[DataType](StringType, IntegerType))
    val toUnsafeKey = UnsafeProjection.create(Array[DataType](StringType))
    val relations = ArrayBuffer.empty[HashedRelation]
    try {
      (0 until 16).foreach { i =>
        val rows = (0 until 10).map { k =>
          toUnsafeRow(InternalRow(UTF8String.fromString(s"key$k"), k + i)).copy()
        }
        val relation = HashedRelation(rows.iterator, buildKey, taskMemoryManager = smallMm)
        assert(relation.isInstanceOf[UnsafeHashedRelation])
        relations += relation
      }
      relations.zipWithIndex.foreach { case (relation, i) =>
        assert(relation.estimatedSize < 2 * UnsafeHashedRelation.INITIAL_PAGE_SIZE_BYTES)
        (0 until 10).foreach { k =>
          val key = toUnsafeKey(InternalRow(UTF8String.fromString(s"key$k")))
          assert(relation.getValue(key).getInt(1) === k + i)
        }
      }
    } catch {
      // SparkOutOfMemoryError is fatal to ScalaTest and would abort the whole suite.
      case e: SparkOutOfMemoryError =>
        fail("Could not build 16 small UnsafeHashedRelations in one task", e)
    } finally {
      relations.foreach(_.close())
    }
    assert(smallUmm.executionMemoryUsed === 0)
  }

  test("SPARK-61082: UnsafeHashedRelation does not round its page up to the page size") {
    // The single string column is also the key, so a row is its own key.
    val buildKey = Seq(BoundReference(0, StringType, false))
    val toUnsafeRow = UnsafeProjection.create(Array[DataType](StringType))
    val row = toUnsafeRow(InternalRow(UTF8String.fromString("a"))).copy()
    val relation = HashedRelation(Iterator(row), buildKey, taskMemoryManager = mm)
    val serializers = Seq(
      new JavaSerializer(new SparkConf()).newInstance(),
      new KryoSerializer(new SparkConf().set(KRYO_REFERENCE_TRACKING, false)).newInstance())
    // The map grows its pages up to the SparkEnv page size, not up to the initial page size.
    val pageSize = SparkEnv.get.memoryManager.pageSizeBytes
    try {
      assert(relation.estimatedSize < 2 * UnsafeHashedRelation.INITIAL_PAGE_SIZE_BYTES)
      assert(relation.asInstanceOf[UnsafeHashedRelation].mapPageSizeBytes === pageSize)
      serializers.foreach { ser =>
        val deserialized = ser.deserialize[UnsafeHashedRelation](ser.serialize(relation))
        try {
          assert(deserialized.estimatedSize < 2 * UnsafeHashedRelation.INITIAL_PAGE_SIZE_BYTES)
          assert(deserialized.mapPageSizeBytes === pageSize)
          assert(deserialized.getValue(row) === row)
        } finally {
          deserialized.close()
        }
      }
    } finally {
      relation.close()
    }
  }

  test("LongToUnsafeRowMap") {
    val unsafeProj = UnsafeProjection.create(
      Seq(BoundReference(0, LongType, false), BoundReference(1, IntegerType, true)))
    val rows = (0 until 100).map(i => unsafeProj(InternalRow(Int.int2long(i), i + 1)).copy())
    val key = Seq(BoundReference(0, LongType, false))
    val longRelation = LongHashedRelation(rows.iterator, key, 10, mm)
    assert(longRelation.keyIsUnique)
    (0 until 100).foreach { i =>
      val row = longRelation.getValue(i)
      assert(row.getLong(0) === i)
      assert(row.getInt(1) === i + 1)
    }
    longRelation.close()

    val longRelation2 = LongHashedRelation(rows.iterator ++ rows.iterator, key, 100, mm)
      .asInstanceOf[LongHashedRelation]
    assert(!longRelation2.keyIsUnique)
    (0 until 100).foreach { i =>
      val rows = longRelation2.get(i).toArray
      assert(rows.length === 2)
      assert(rows(0).getLong(0) === i)
      assert(rows(0).getInt(1) === i + 1)
      assert(rows(1).getLong(0) === i)
      assert(rows(1).getInt(1) === i + 1)
    }

    val os = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(os)
    longRelation2.writeExternal(out)
    out.flush()
    longRelation2.close()
    val in = new ObjectInputStream(new ByteArrayInputStream(os.toByteArray))
    val relation = new LongHashedRelation()
    relation.readExternal(in)
    assert(!relation.keyIsUnique)
    (0 until 100).foreach { i =>
      val rows = relation.get(i).toArray
      assert(rows.length === 2)
      assert(rows(0).getLong(0) === i)
      assert(rows(0).getInt(1) === i + 1)
      assert(rows(1).getLong(0) === i)
      assert(rows(1).getInt(1) === i + 1)
    }
  }

  test("LongToUnsafeRowMap with very wide range") {
    val unsafeProj = UnsafeProjection.create(Seq(BoundReference(0, LongType, false)))

    {
      // SPARK-16740
      val keys = Seq(0L, Long.MaxValue, Long.MaxValue)
      val map = new LongToUnsafeRowMap(mm, 1)
      keys.foreach { k =>
        map.append(k, unsafeProj(InternalRow(k)))
      }
      map.optimize()
      val row = unsafeProj(InternalRow(0L)).copy()
      keys.foreach { k =>
        assert(map.getValue(k, row) eq row)
        assert(row.getLong(0) === k)
      }
      map.free()
    }


    {
      // SPARK-16802
      val keys = Seq(Long.MaxValue, Long.MaxValue - 10)
      val map = new LongToUnsafeRowMap(mm, 1)
      keys.foreach { k =>
        map.append(k, unsafeProj(InternalRow(k)))
      }
      map.optimize()
      val row = unsafeProj(InternalRow(0L)).copy()
      keys.foreach { k =>
        assert(map.getValue(k, row) eq row)
        assert(row.getLong(0) === k)
      }
      assert(map.getValue(Long.MinValue, row) eq null)
      map.free()
    }
  }

  test("LongToUnsafeRowMap with random keys") {
    val unsafeProj = UnsafeProjection.create(Seq(BoundReference(0, LongType, false)))

    val N = 1000000
    val rand = new Random
    val keys = (0 to N).map(x => rand.nextLong()).toArray

    val map = new LongToUnsafeRowMap(mm, 10)
    keys.foreach { k =>
      map.append(k, unsafeProj(InternalRow(k)))
    }
    map.optimize()

    val os = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(os)
    map.writeExternal(out)
    out.flush()
    map.free()
    val in = new ObjectInputStream(new ByteArrayInputStream(os.toByteArray))
    val map2 = new LongToUnsafeRowMap(mm, 1)
    map2.readExternal(in)

    val row = unsafeProj(InternalRow(0L)).copy()
    keys.foreach { k =>
      val r = map2.get(k, row)
      assert(r.hasNext)
      var c = 0
      while (r.hasNext) {
        val rr = r.next()
        assert(rr.getLong(0) === k)
        c += 1
      }
    }
    var i = 0
    while (i < N * 10) {
      val k = rand.nextLong()
      val r = map2.get(k, row)
      if (r != null) {
        assert(r.hasNext)
        while (r.hasNext) {
          assert(r.next().getLong(0) === k)
        }
      }
      i += 1
    }
    map2.free()
  }

  test("SPARK-24257: insert big values into LongToUnsafeRowMap") {
    val unsafeProj = UnsafeProjection.create(Array[DataType](StringType))
    val map = new LongToUnsafeRowMap(mm, 1)

    val key = 0L
    // the page array is initialized with length 1 << 17 (1M bytes),
    // so here we need a value larger than 1 << 18 (2M bytes), to trigger the bug
    val bigStr = UTF8String.fromString("x".repeat(1 << 19))

    map.append(key, unsafeProj(InternalRow(bigStr)))
    map.optimize()

    val resultRow = new UnsafeRow(1)
    assert(map.getValue(key, resultRow).getUTF8String(0) === bigStr)
    map.free()
  }

  test("SPARK-24809: Serializing LongToUnsafeRowMap in executor may result in data error") {
    val unsafeProj = UnsafeProjection.create(Array[DataType](LongType))
    val originalMap = new LongToUnsafeRowMap(mm, 1)

    val key1 = 1L
    val value1 = 4852306286022334418L

    val key2 = 2L
    val value2 = 8813607448788216010L

    originalMap.append(key1, unsafeProj(InternalRow(value1)))
    originalMap.append(key2, unsafeProj(InternalRow(value2)))
    originalMap.optimize()

    val ser = sparkContext.env.serializer.newInstance()
    // Simulate serialize/deserialize twice on driver and executor
    val firstTimeSerialized = ser.deserialize[LongToUnsafeRowMap](ser.serialize(originalMap))
    val secondTimeSerialized =
      ser.deserialize[LongToUnsafeRowMap](ser.serialize(firstTimeSerialized))

    val resultRow = new UnsafeRow(1)
    assert(secondTimeSerialized.getValue(key1, resultRow).getLong(0) === value1)
    assert(secondTimeSerialized.getValue(key2, resultRow).getLong(0) === value2)

    originalMap.free()
    firstTimeSerialized.free()
    secondTimeSerialized.free()
  }

  /**
   * Creates a memory manager whose page size is set explicitly, so that the maps built on it do
   * not depend on the page size derived from the JVM heap and the number of cores.
   */
  private def newMemoryManager(pageSize: Long, memorySize: Long): UnifiedMemoryManager = {
    new UnifiedMemoryManager(
      new SparkConf()
        .set(MEMORY_OFFHEAP_ENABLED, useOffHeapMemoryMode)
        .set(MEMORY_OFFHEAP_SIZE, memorySize)
        .set(BUFFER_PAGESIZE, pageSize),
      memorySize,
      memorySize / 2,
      1)
  }

  test("SPARK-60998: LongToUnsafeRowMap does not round its page up to the page size") {
    // SPARK-61257: Like the default on-heap page size under G1 (SPARK-37593), a page size that
    // leaves room for the header of a long[] makes the page leave the same room.
    Seq(0L, Platform.LONG_ARRAY_OFFSET.toLong).foreach { header =>
      val largeUmm = newMemoryManager(ByteUnit.MiB.toBytes(64) - header, ByteUnit.GiB.toBytes(1))
      val largeMm = new TaskMemoryManager(largeUmm, 0)
      assert(largeMm.pageSizeBytes() === ByteUnit.MiB.toBytes(64) - header)

      // Capacity 16 gives an array of 32 longs, which does not grow for a single key.
      val arrayBytes = 32 * 8L
      val map = new LongToUnsafeRowMap(largeMm, 16)
      assert(map.getMode === largeMm.getTungstenMemoryMode)
      assert(map.getUsed === arrayBytes + LongToUnsafeRowMap.INITIAL_PAGE_SIZE_BYTES - header)

      // A value larger than the initial page doubles the page together with its header, to
      // 2 MiB, not 64 MiB.
      val unsafeProj = UnsafeProjection.create(Array[DataType](StringType))
      val bigStr = UTF8String.fromString("x".repeat(3 << 19))
      map.append(0L, unsafeProj(InternalRow(bigStr)))
      assert(map.getUsed === arrayBytes + 2 * LongToUnsafeRowMap.INITIAL_PAGE_SIZE_BYTES - header)

      val resultRow = new UnsafeRow(1)
      assert(map.getValue(0L, resultRow).getUTF8String(0) === bigStr)
      map.free()
      assert(largeUmm.executionMemoryUsed === 0)
    }
  }

  test("SPARK-60998: many LongToUnsafeRowMaps fit in one task when each holds little data") {
    // 16 maps rounded up to a 64 MiB page each would need 1 GiB, 4x the memory available.
    val smallUmm = newMemoryManager(ByteUnit.MiB.toBytes(64), ByteUnit.MiB.toBytes(256))
    val smallMm = new TaskMemoryManager(smallUmm, 0)
    val unsafeProj = UnsafeProjection.create(Seq(BoundReference(0, LongType, false)))
    val maps = ArrayBuffer.empty[LongToUnsafeRowMap]
    try {
      (0 until 16).foreach { i =>
        val map = new LongToUnsafeRowMap(smallMm, 16)
        maps += map
        (0 until 10).foreach(k => map.append(k, unsafeProj(InternalRow(k.toLong + i))))
        map.optimize()
      }
      maps.zipWithIndex.foreach { case (map, i) =>
        assert(map.getUsed < 2 * LongToUnsafeRowMap.INITIAL_PAGE_SIZE_BYTES)
        val resultRow = new UnsafeRow(1)
        (0 until 10).foreach(k => assert(map.getValue(k, resultRow).getLong(0) === k + i))
      }
    } catch {
      // SparkOutOfMemoryError is fatal to ScalaTest and would abort the whole suite.
      case e: SparkOutOfMemoryError =>
        fail("Could not build 16 small LongToUnsafeRowMaps in one task", e)
    } finally {
      maps.foreach(_.free())
    }
    assert(smallUmm.executionMemoryUsed === 0)
  }

  test("SPARK-60998: deserialized LongToUnsafeRowMap allocates exactly what it reads") {
    val largeUmm = newMemoryManager(ByteUnit.MiB.toBytes(64), ByteUnit.GiB.toBytes(1))
    val largeMm = new TaskMemoryManager(largeUmm, 0)
    val unsafeProj = UnsafeProjection.create(Array[DataType](LongType))
    val serializers = Seq(
      new JavaSerializer(new SparkConf()).newInstance(),
      new KryoSerializer(new SparkConf().set(KRYO_REFERENCE_TRACKING, false)).newInstance())

    // Both a non-empty map and an empty map, whose serialized page has length 0.
    for (ser <- serializers; numRows <- Seq(10, 0)) {
      val originalMap = new LongToUnsafeRowMap(largeMm, 16)
      (0 until numRows).foreach(k => originalMap.append(k, unsafeProj(InternalRow(k * 2L))))
      if (numRows > 0) {
        originalMap.optimize()
      }
      val deserializedMap = ser.deserialize[LongToUnsafeRowMap](ser.serialize(originalMap))

      // The deserialized map does not use `largeMm`: it is created by the no-arg constructor,
      // which builds its own memory manager. It allocates the array and the page it reads. After
      // `optimize()`, the keys 0 until 10 use a dense array of 10 longs, and each value takes a
      // 16-byte UnsafeRow plus an 8-byte pointer. The empty map keeps its sparse array of 32
      // longs, and its empty page is rounded up to `MIN_PAGE_SIZE_BYTES`.
      val expectedUsed = if (numRows > 0) {
        numRows * 8L + numRows * 24L
      } else {
        32 * 8L + LongToUnsafeRowMap.MIN_PAGE_SIZE_BYTES
      }
      assert(originalMap.getUsed < 2 * LongToUnsafeRowMap.INITIAL_PAGE_SIZE_BYTES)
      assert(deserializedMap.getUsed === expectedUsed)
      val resultRow = new UnsafeRow(1)
      (0 until numRows).foreach { k =>
        assert(deserializedMap.getValue(k, resultRow).getLong(0) === k * 2L)
      }
      assert(deserializedMap.getValue(numRows, resultRow) === null)
      originalMap.free()
      deserializedMap.free()
      assert(deserializedMap.getUsed === 0)
    }
    assert(largeUmm.executionMemoryUsed === 0)
  }

  test("Spark-14521") {
    val ser = new KryoSerializer(
      (new SparkConf).set(KRYO_REFERENCE_TRACKING, false)).newInstance()
    val key = Seq(BoundReference(0, LongType, false))

    // Testing Kryo serialization of HashedRelation
    val unsafeProj = UnsafeProjection.create(
      Seq(BoundReference(0, LongType, false), BoundReference(1, IntegerType, true)))
    val rows = (0 until 100).map(i => unsafeProj(InternalRow(Int.int2long(i), i + 1)).copy())
    val longRelation = LongHashedRelation(rows.iterator ++ rows.iterator, key, 100, mm)
    val longRelation2 = ser.deserialize[LongHashedRelation](ser.serialize(longRelation))
    longRelation.close()
    (0 until 100).foreach { i =>
      val rows = longRelation2.get(i).toArray
      assert(rows.length === 2)
      assert(rows(0).getLong(0) === i)
      assert(rows(0).getInt(1) === i + 1)
      assert(rows(1).getLong(0) === i)
      assert(rows(1).getInt(1) === i + 1)
    }

    // Testing Kryo serialization of UnsafeHashedRelation
    val unsafeHashed = UnsafeHashedRelation(rows.iterator, key, 1, mm)
    val os = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(os)
    unsafeHashed.asInstanceOf[UnsafeHashedRelation].writeExternal(out)
    out.flush()
    val unsafeHashed2 = ser.deserialize[UnsafeHashedRelation](ser.serialize(unsafeHashed))
    unsafeHashed.close()
    val os2 = new ByteArrayOutputStream()
    val out2 = new ObjectOutputStream(os2)
    unsafeHashed2.writeExternal(out2)
    out2.flush()
    assert(java.util.Arrays.equals(os.toByteArray, os2.toByteArray))
  }

  test("SPARK-61078: LongToUnsafeRowMap does not allocate memory before deserialization") {
    // JavaSerializer creates the map with the no-arg constructor, and KryoSerializer creates it
    // with `Kryo.newInstance`. Then `read()` allocates `array` and `page`.
    assert(new LongToUnsafeRowMap().getUsed === 0)
    val kryo = new KryoSerializer(new SparkConf).newKryo()
    assert(kryo.newInstance(classOf[LongToUnsafeRowMap]).getUsed === 0)
  }

  test("SPARK-31511: Make BytesToBytesMap iterators thread-safe") {
    val ser = sparkContext.env.serializer.newInstance()
    val key = Seq(BoundReference(0, LongType, false))

    val unsafeProj = UnsafeProjection.create(
      Seq(BoundReference(0, LongType, false), BoundReference(1, IntegerType, true)))
    val rows = (0 until 10000).map(i => unsafeProj(InternalRow(Int.int2long(i), i + 1)).copy())
    val unsafeHashed = UnsafeHashedRelation(rows.iterator, key, 1, mm)

    val os = new ByteArrayOutputStream()
    val thread1 = new Thread {
      override def run(): Unit = {
        val out = new ObjectOutputStream(os)
        unsafeHashed.asInstanceOf[UnsafeHashedRelation].writeExternal(out)
        out.flush()
      }
    }

    val thread2 = new Thread {
      override def run(): Unit = {
        val threadOut = new ObjectOutputStream(new ByteArrayOutputStream())
        unsafeHashed.asInstanceOf[UnsafeHashedRelation].writeExternal(threadOut)
        threadOut.flush()
      }
    }

    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()

    val unsafeHashed2 = ser.deserialize[UnsafeHashedRelation](ser.serialize(unsafeHashed))
    unsafeHashed.close()
    val os2 = new ByteArrayOutputStream()
    val out2 = new ObjectOutputStream(os2)
    unsafeHashed2.writeExternal(out2)
    out2.flush()
    assert(java.util.Arrays.equals(os.toByteArray, os2.toByteArray))
  }

  // This test require 4G heap to run, should run it manually
  ignore("build HashedRelation that is larger than 1G") {
    val unsafeProj = UnsafeProjection.create(
      Seq(BoundReference(0, IntegerType, false),
        BoundReference(1, StringType, true)))
    val unsafeRow = unsafeProj(InternalRow(0, UTF8String.fromString(" ".repeat(100))))
    val key = Seq(BoundReference(0, IntegerType, false))
    val rows = (0 until (1 << 24)).iterator.map { i =>
      unsafeRow.setInt(0, i % 1000000)
      unsafeRow.setInt(1, i)
      unsafeRow
    }

    val unsafeRelation = UnsafeHashedRelation(rows, key, 1000, mm)
    assert(unsafeRelation.estimatedSize > (2L << 30))
    unsafeRelation.close()

    val rows2 = (0 until (1 << 24)).iterator.map { i =>
      unsafeRow.setInt(0, i % 1000000)
      unsafeRow.setInt(1, i)
      unsafeRow
    }
    val longRelation = LongHashedRelation(rows2, key, 1000, mm)
    assert(longRelation.estimatedSize > (2L << 30))
    longRelation.close()
  }

  // This test require 4G heap to run, should run it manually
  ignore("build HashedRelation with more than 100 millions rows") {
    val unsafeProj = UnsafeProjection.create(
      Seq(BoundReference(0, IntegerType, false),
        BoundReference(1, StringType, true)))
    val unsafeRow = unsafeProj(InternalRow(0, UTF8String.fromString(" ".repeat(100))))
    val key = Seq(BoundReference(0, IntegerType, false))
    val rows = (0 until (1 << 10)).iterator.map { i =>
      unsafeRow.setInt(0, i % 1000000)
      unsafeRow.setInt(1, i)
      unsafeRow
    }
    val m = LongHashedRelation(rows, key, 100 << 20, mm)
    m.close()
  }

  test("UnsafeHashedRelation: key set iterator on a contiguous array of keys") {
    val hashedRelation = UnsafeHashedRelation(contiguousRows.iterator, singleKey, 1, mm)
    val keyIterator = hashedRelation.keys()
    assert(keyIterator.map(key => key.getLong(0)).toArray === contiguousArray)
    hashedRelation.close()
  }

  test("UnsafeHashedRelation: key set iterator on a sparse array of keys") {
    val hashedRelation = UnsafeHashedRelation(sparseRows.iterator, singleKey, 1, mm)
    val keyIterator = hashedRelation.keys()
    assert(keyIterator.map(key => key.getLong(0)).toArray === sparseArray)
    hashedRelation.close()
  }

  test("LongHashedRelation: key set iterator on a contiguous array of keys") {
    val longRelation = LongHashedRelation(contiguousRows.iterator, singleKey, 1, mm)
    val keyIterator = longRelation.keys()
    assert(keyIterator.map(key => key.getLong(0)).toArray === contiguousArray)
    longRelation.close()
  }

  test("LongToUnsafeRowMap: key set iterator on a contiguous array of keys") {
    val rowMap = new LongToUnsafeRowMap(mm, 1)
    contiguousArray.toArray.lazyZip(contiguousRows).map {
      (i, row) => rowMap.append(i, row)
    }
    var keyIterator = rowMap.keys()
    // in sparse mode the keys are unsorted
    assert(keyIterator.map(key => key.getLong(0)).toArray.sortWith(_ < _) === contiguousArray)
    // in dense mode the keys are already ordered
    rowMap.optimize()
    keyIterator = rowMap.keys()
    assert(keyIterator.map(key => key.getLong(0)).toArray === contiguousArray)
    rowMap.free()
  }

  test("LongToUnsafeRowMap: key set iterator on a sparse array with equidistant keys") {
    val rowMap = new LongToUnsafeRowMap(mm, 1)
    sparseArray.toArray.lazyZip(sparseRows).map {
      (i, row) => rowMap.append(i, row)
    }
    var keyIterator = rowMap.keys()
    assert(keyIterator.map(_.getLong(0)).toArray.sortWith(_ < _) === sparseArray)
    rowMap.optimize()
    keyIterator = rowMap.keys()
    assert(keyIterator.map(_.getLong(0)).toArray === sparseArray)
    rowMap.free()
  }

  test("LongToUnsafeRowMap: key set iterator on an array with a single key") {
    // build several maps each of which has a single valid key
    (0 to 1000).foreach { i =>
      val rowMap = new LongToUnsafeRowMap(mm, 1)
      rowMap.append(i, projection(InternalRow((2 * i + 1).toLong)))
      var keyIterator = rowMap.keys()
      assert(keyIterator.next().getLong(0) === i)
      rowMap.optimize()
      keyIterator = rowMap.keys()
      assert(keyIterator.next().getLong(0) === i)
      rowMap.free()
    }
  }

  test("LongToUnsafeRowMap: multiple hasNext calls before calling next() on the key iterator") {
    val rowMap = new LongToUnsafeRowMap(mm, 1)
    randomArray.toArray.lazyZip(randomRows).map {
      (i, row) => rowMap.append(i, row)
    }
    val buffer = new ArrayBuffer[Long]()
    // hasNext should not change the cursor unless the key was read by a next() call
    var keyIterator = rowMap.keys()
    while (keyIterator.hasNext) {
      keyIterator.hasNext
      keyIterator.hasNext
      buffer.append(keyIterator.next().getLong(0))
    }
    assert(buffer.sortWith(_ < _) === randomArray)
    buffer.clear()

    rowMap.optimize()
    keyIterator = rowMap.keys()
    while (keyIterator.hasNext) {
      keyIterator.hasNext
      keyIterator.hasNext
      buffer.append(keyIterator.next().getLong(0))
    }
    assert(buffer === randomArray)
    rowMap.free()
  }

  test("LongToUnsafeRowMap: no explicit hasNext calls on the key iterator") {
    val rowMap = new LongToUnsafeRowMap(mm, 1)
    randomArray.toArray.lazyZip(randomRows).map {
      (i, row) => rowMap.append(i, row)
    }
    val buffer = new ArrayBuffer[Long]()
    // call next() until the buffer is filled with all keys
    var keyIterator = rowMap.keys()
    while (buffer.size < randomArray.size) {
      buffer.append(keyIterator.next().getLong(0))
    }
    // attempt an illegal next() call
    checkError(
      exception = intercept[SparkException] {
        keyIterator.next()
      },
      condition = "INTERNAL_ERROR",
      parameters = Map("message" -> "End of the iterator.")
    )
    assert(buffer.sortWith(_ < _) === randomArray)
    buffer.clear()

    rowMap.optimize()
    keyIterator = rowMap.keys()
    while (buffer.size < randomArray.size) {
      buffer.append(keyIterator.next().getLong(0))
    }
    assert(buffer === randomArray)
    rowMap.free()
  }

  test("LongToUnsafeRowMap: call hasNext at the end of the iterator") {
    val rowMap = new LongToUnsafeRowMap(mm, 1)
    sparseArray.toArray.lazyZip(sparseRows).map {
      (i, row) => rowMap.append(i, row)
    }
    var keyIterator = rowMap.keys()
    assert(keyIterator.map(key => key.getLong(0)).toArray.sortWith(_ < _) === sparseArray)
    assert(keyIterator.hasNext == false)
    assert(keyIterator.hasNext == false)

    rowMap.optimize()
    keyIterator = rowMap.keys()
    assert(keyIterator.map(key => key.getLong(0)).toArray === sparseArray)
    assert(keyIterator.hasNext == false)
    assert(keyIterator.hasNext == false)
    rowMap.free()
  }

  test("LongToUnsafeRowMap: random sequence of hasNext and next() calls on the key iterator") {
    val rowMap = new LongToUnsafeRowMap(mm, 1)
    randomArray.toArray.lazyZip(randomRows).map {
      (i, row) => rowMap.append(i, row)
    }
    val buffer = new ArrayBuffer[Long]()
    // call hasNext or next() at random
    var keyIterator = rowMap.keys()
    while (buffer.size < randomArray.size) {
      if (rand.nextBoolean() && keyIterator.hasNext) {
        buffer.append(keyIterator.next().getLong(0))
      } else {
        keyIterator.hasNext
      }
    }
    assert(buffer.sortWith(_ < _) === randomArray)
    buffer.clear()

    rowMap.optimize()
    keyIterator = rowMap.keys()
    while (buffer.size < randomArray.size) {
      if (rand.nextBoolean() && keyIterator.hasNext) {
        buffer.append(keyIterator.next().getLong(0))
      } else {
        keyIterator.hasNext
      }
    }
    assert(buffer === randomArray)
    rowMap.free()
  }

  test("HashJoin: packing and unpacking with the same key type in a LongType") {
    val row = InternalRow(0.toShort, 1.toShort, 2.toShort, 3.toShort)
    val keys = Seq(BoundReference(0, ShortType, false),
      BoundReference(1, ShortType, false),
      BoundReference(2, ShortType, false),
      BoundReference(3, ShortType, false))
    val packed = HashJoin.rewriteKeyExpr(keys)
    val unsafeProj = UnsafeProjection.create(packed)
    val packedKeys = unsafeProj(row)

    (0 to 3).foreach { i =>
      val key = HashJoin.extractKeyExprAt(keys, i)
      val proj = UnsafeProjection.create(key)
      assert(proj(packedKeys).getShort(0) == i)
    }
  }

  test("HashJoin: packing and unpacking with various key types in a LongType") {
    val row = InternalRow((-1).toByte, (-2).toInt, (-3).toShort)
    val keys = Seq(BoundReference(0, ByteType, false),
      BoundReference(1, IntegerType, false),
      BoundReference(2, ShortType, false))
    // Rewrite and exacting key expressions should not cause exception when ANSI mode is on.
    Seq("false", "true").foreach { ansiEnabled =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled) {
        val packed = HashJoin.rewriteKeyExpr(keys)
        val unsafeProj = UnsafeProjection.create(packed)
        val packedKeys = unsafeProj(row)

        Seq((0, ByteType), (1, IntegerType), (2, ShortType)).foreach { case (i, dt) =>
          val key = HashJoin.extractKeyExprAt(keys, i)
          val proj = UnsafeProjection.create(key)
          assert(proj(packedKeys).get(0, dt) == -i - 1)
        }
      }
    }
  }

  test("EmptyHashedRelation override methods behavior test") {
    val buildKey = Seq(BoundReference(0, LongType, false))
    val hashed = HashedRelation(Seq.empty[InternalRow].iterator, buildKey, 1, mm)
    assert(hashed == EmptyHashedRelation)

    val key = InternalRow(1L)
    assert(hashed.get(0L) == null)
    assert(hashed.get(key) == null)
    assert(hashed.getValue(0L) == null)
    assert(hashed.getValue(key) == null)

    assert(hashed.keys().isEmpty)
    assert(hashed.keyIsUnique)
    assert(hashed.estimatedSize == 0)
    hashed.close()
  }

  test("SPARK-32399: test methods related to key index") {
    val schema = StructType(StructField("a", IntegerType, true) :: Nil)
    val toUnsafe = UnsafeProjection.create(schema)
    val key = Seq(BoundReference(0, IntegerType, true))
    val row = Seq(BoundReference(0, IntegerType, true), BoundReference(1, IntegerType, true))
    val unsafeProj = UnsafeProjection.create(row)
    var rows = (0 until 100).map(i => {
      val k = if (i % 10 == 0) null else i % 10
      unsafeProj(InternalRow(k, i)).copy()
    })
    rows = unsafeProj(InternalRow(-1, -1)).copy() +: rows
    val unsafeRelation = UnsafeHashedRelation(rows.iterator, key, 10, mm, allowsNullKey = true)
    val keyIndexToKeyMap = new mutable.HashMap[Int, String]
    val keyIndexToValueMap = new mutable.HashMap[Int, Seq[Int]]

    // test getWithKeyIndex()
    (0 until 10).foreach(i => {
      val key = if (i == 0) InternalRow(null) else InternalRow(i)
      val valuesWithKeyIndex = unsafeRelation.getWithKeyIndex(toUnsafe(key)).map(
        v => (v.getKeyIndex, v.getValue.getInt(1))).toArray
      val keyIndex = valuesWithKeyIndex.head._1
      val actualValues = valuesWithKeyIndex.map(_._2)
      val expectedValues = (0 until 10).map(j => j * 10 + i)
      if (i == 0) {
        keyIndexToKeyMap(keyIndex) = "null"
      } else {
        keyIndexToKeyMap(keyIndex) = i.toString
      }
      keyIndexToValueMap(keyIndex) = actualValues.toImmutableArraySeq
      // key index is non-negative
      assert(keyIndex >= 0)
      // values are expected
      assert(actualValues.sortWith(_ < _) === expectedValues)
    })
    // key index is unique per key
    val numUniqueKeyIndex = (0 until 10).flatMap(i => {
      val key = if (i == 0) InternalRow(null) else InternalRow(i)
      val keyIndex = unsafeRelation.getWithKeyIndex(toUnsafe(key)).map(_.getKeyIndex).toSeq
      keyIndex
    }).distinct.size
    assert(numUniqueKeyIndex == 10)
    // NULL for non-existing key
    assert(unsafeRelation.getWithKeyIndex(toUnsafe(InternalRow(100))) == null)

    // test getValueWithKeyIndex()
    val valuesWithKeyIndex = unsafeRelation.getValueWithKeyIndex(toUnsafe(InternalRow(-1)))
    val keyIndex = valuesWithKeyIndex.getKeyIndex
    keyIndexToKeyMap(keyIndex) = "-1"
    keyIndexToValueMap(keyIndex) = Seq(-1)
    // key index is non-negative
    assert(valuesWithKeyIndex.getKeyIndex >= 0)
    // value is expected
    assert(valuesWithKeyIndex.getValue.getInt(1) == -1)
    // NULL for non-existing key
    assert(unsafeRelation.getValueWithKeyIndex(toUnsafe(InternalRow(100))) == null)

    // test valuesWithKeyIndex()
    val keyIndexToRowMap = unsafeRelation.valuesWithKeyIndex().map(
      v => (v.getKeyIndex, v.getValue.copy())).toSeq.groupBy(_._1)
    assert(keyIndexToRowMap.size == 11)
    keyIndexToRowMap.foreach {
      case (keyIndex, row) =>
        val expectedKey = keyIndexToKeyMap(keyIndex)
        val expectedValues = keyIndexToValueMap(keyIndex)
        // key index returned from valuesWithKeyIndex()
        // should be the same as returned from getWithKeyIndex()
        if (expectedKey == "null") {
          assert(row.head._2.isNullAt(0))
        } else {
          assert(row.head._2.getInt(0).toString == expectedKey)
        }
        // values returned from valuesWithKeyIndex()
        // should have same value and order as returned from getWithKeyIndex()
        val actualValues = row.map(_._2.getInt(1))
        assert(actualValues === expectedValues)
    }
    unsafeRelation.close()
  }

  test("LongToUnsafeRowMap support ignoresDuplicatedKey") {
    val unsafeProj = UnsafeProjection.create(Seq(BoundReference(0, LongType, false)))
    val keys = Seq(1L, 1L, 1L)
    Seq(true, false).foreach { ignoresDuplicatedKey =>
      val map = new LongToUnsafeRowMap(mm, 1, ignoresDuplicatedKey)
      keys.foreach { k =>
        map.append(k, unsafeProj(InternalRow(k)))
      }
      map.optimize()
      val res = new UnsafeRow(1)
      val it = map.get(1L, res)
      assert(it.hasNext)
      assert(it.next().getLong(0) == 1)
      assert(it.hasNext != ignoresDuplicatedKey)
      map.free()
    }
  }

  test("SPARK-61051: LongToUnsafeRowMap stays sparse when the dense array does not fit") {
    // The sparse array (1 MiB) and the page (1 MiB) fit, but the dense array (about 0.5 MiB)
    // does not, because it is allocated while the sparse array is still alive.
    val memorySize = ByteUnit.MiB.toBytes(2) + ByteUnit.KiB.toBytes(256)
    val smallUmm = new UnifiedMemoryManager(
      new SparkConf()
        .set(MEMORY_OFFHEAP_ENABLED, useOffHeapMemoryMode)
        .set(MEMORY_OFFHEAP_SIZE, memorySize)
        .set(BUFFER_PAGESIZE, ByteUnit.MiB.toBytes(1)),
      memorySize,
      memorySize / 2,
      1)
    val smallMm = new TaskMemoryManager(smallUmm, 0)
    val unsafeProj = UnsafeProjection.create(Seq(BoundReference(0, LongType, false)))
    // Capacity 1 << 16 gives a sparse array of 1 << 17 longs, which 32 keys do not grow.
    val map = new LongToUnsafeRowMap(smallMm, 1 << 16)
    val keys = (0 until 32).map(_ * 2048L)
    keys.foreach(k => map.append(k, unsafeProj(InternalRow(k))))
    val used = map.getUsed
    assert(used === ByteUnit.MiB.toBytes(2))

    try {
      map.optimize()
    } catch {
      // SparkOutOfMemoryError is fatal to ScalaTest and would abort the whole suite.
      case e: SparkOutOfMemoryError =>
        map.free()
        fail("optimize() should keep the map sparse when the dense array does not fit", e)
    }
    // The map keeps its sparse array and the failed allocation does not leak memory.
    assert(map.getUsed === used)
    assert(smallUmm.executionMemoryUsed === used)
    val resultRow = new UnsafeRow(1)
    keys.foreach(k => assert(map.getValue(k, resultRow).getLong(0) === k))
    assert(map.getValue(1L, resultRow) === null)

    // A failed spill of another consumer is not a lack of memory, so optimize() does not hide it.
    val failingConsumer = new MemoryConsumer(smallMm, smallMm.getTungstenMemoryMode) {
      override def spill(size: Long, trigger: MemoryConsumer): Long = {
        throw new IOException("Injected spill failure")
      }
    }
    val remaining = memorySize - used
    assert(failingConsumer.acquireMemory(remaining) === remaining)
    val e = intercept[SparkOutOfMemoryError](map.optimize())
    assert(e.getCondition === "SPILL_OUT_OF_MEMORY")
    failingConsumer.freeMemory(remaining)
    map.free()
    assert(smallUmm.executionMemoryUsed === 0)
  }
}

class HashedRelationOnHeapSuite extends HashedRelationSuite {
  override protected def useOffHeapMemoryMode: Boolean = false
}

class HashedRelationOffHeapSuite extends HashedRelationSuite {
  override protected def useOffHeapMemoryMode: Boolean = true
}
