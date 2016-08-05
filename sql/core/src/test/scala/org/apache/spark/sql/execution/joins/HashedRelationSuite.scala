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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.memory.{StaticMemoryManager, TaskMemoryManager}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.unsafe.map.BytesToBytesMap
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.collection.CompactBuffer

class HashedRelationSuite extends SparkFunSuite with SharedSQLContext {

  val mm = new TaskMemoryManager(
    new StaticMemoryManager(
      new SparkConf().set("spark.memory.offHeap.enabled", "false"),
      Long.MaxValue,
      Long.MaxValue,
      1),
    0)

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
    val in = new ObjectInputStream(new ByteArrayInputStream(os.toByteArray))
    val hashed2 = new UnsafeHashedRelation()
    hashed2.readExternal(in)
    assert(hashed2.get(unsafeData(0)).toArray === Array(unsafeData(0)))
    assert(hashed2.get(unsafeData(1)).toArray === Array(unsafeData(1)))
    assert(hashed2.get(toUnsafe(InternalRow(10))) === null)
    assert(hashed2.get(unsafeData(2)).toArray === data2)

    val os2 = new ByteArrayOutputStream()
    val out2 = new ObjectOutputStream(os2)
    hashed2.asInstanceOf[UnsafeHashedRelation].writeExternal(out2)
    out2.flush()
    // This depends on that the order of items in BytesToBytesMap.iterator() is exactly the same
    // as they are inserted
    assert(java.util.Arrays.equals(os2.toByteArray, os.toByteArray))
  }

  test("test serialization empty hash map") {
    val taskMemoryManager = new TaskMemoryManager(
      new StaticMemoryManager(
        new SparkConf().set("spark.memory.offHeap.enabled", "false"),
        Long.MaxValue,
        Long.MaxValue,
        1),
      0)
    val binaryMap = new BytesToBytesMap(taskMemoryManager, 1, 1)
    val os = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(os)
    val hashed = new UnsafeHashedRelation(1, binaryMap)
    hashed.writeExternal(out)
    out.flush()
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

    val longRelation2 = LongHashedRelation(rows.iterator ++ rows.iterator, key, 100, mm)
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
    val taskMemoryManager = new TaskMemoryManager(
      new StaticMemoryManager(
        new SparkConf().set("spark.memory.offHeap.enabled", "false"),
        Long.MaxValue,
        Long.MaxValue,
        1),
      0)
    val unsafeProj = UnsafeProjection.create(Seq(BoundReference(0, LongType, false)))

    {
      // SPARK-16740
      val keys = Seq(0L, Long.MaxValue, Long.MaxValue)
      val map = new LongToUnsafeRowMap(taskMemoryManager, 1)
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
      val map = new LongToUnsafeRowMap(taskMemoryManager, 1)
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

  test("Spark-14521") {
    val ser = new KryoSerializer(
      (new SparkConf).set("spark.kryo.referenceTracking", "false")).newInstance()
    val key = Seq(BoundReference(0, LongType, false))

    // Testing Kryo serialization of HashedRelation
    val unsafeProj = UnsafeProjection.create(
      Seq(BoundReference(0, LongType, false), BoundReference(1, IntegerType, true)))
    val rows = (0 until 100).map(i => unsafeProj(InternalRow(Int.int2long(i), i + 1)).copy())
    val longRelation = LongHashedRelation(rows.iterator ++ rows.iterator, key, 100, mm)
    val longRelation2 = ser.deserialize[LongHashedRelation](ser.serialize(longRelation))
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
    val unsafeRow = unsafeProj(InternalRow(0, UTF8String.fromString(" " * 100)))
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
    val unsafeRow = unsafeProj(InternalRow(0, UTF8String.fromString(" " * 100)))
    val key = Seq(BoundReference(0, IntegerType, false))
    val rows = (0 until (1 << 10)).iterator.map { i =>
      unsafeRow.setInt(0, i % 1000000)
      unsafeRow.setInt(1, i)
      unsafeRow
    }
    val m = LongHashedRelation(rows, key, 100 << 20, mm)
    m.close()
  }
}
