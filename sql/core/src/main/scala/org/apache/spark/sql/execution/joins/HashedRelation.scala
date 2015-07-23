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

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.nio.ByteOrder
import java.util.{HashMap => JavaHashMap}

import scala.reflect.ClassTag

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{SparkPlan, SparkSqlSerializer}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.PlatformDependent
import org.apache.spark.unsafe.map.BytesToBytesMap
import org.apache.spark.unsafe.memory.{ExecutorMemoryManager, TaskMemoryManager, MemoryAllocator}
import org.apache.spark.util.collection.CompactBuffer


/**
 * Interface for a hashed relation by some key. Use [[HashedRelation.apply]] to create a concrete
 * object.
 */
private[joins] sealed trait HashedRelation {
  def get(key: InternalRow): CompactBuffer[InternalRow]

  // This is a helper method to implement Externalizable, and is used by
  // GeneralHashedRelation and UniqueKeyHashedRelation
  protected def writeBytes(out: ObjectOutput, serialized: Array[Byte]): Unit = {
    out.writeInt(serialized.length) // Write the length of serialized bytes first
    out.write(serialized)
  }

  // This is a helper method to implement Externalizable, and is used by
  // GeneralHashedRelation and UniqueKeyHashedRelation
  protected def readBytes(in: ObjectInput): Array[Byte] = {
    val serializedSize = in.readInt() // Read the length of serialized bytes first
    val bytes = new Array[Byte](serializedSize)
    in.readFully(bytes)
    bytes
  }
}


/**
 * A general [[HashedRelation]] backed by a hash map that maps the key into a sequence of values.
 */
private[joins] final class GeneralHashedRelation(
    private var hashTable: JavaHashMap[InternalRow, CompactBuffer[InternalRow]])
  extends HashedRelation with Externalizable {

  def this() = this(null) // Needed for serialization

  override def get(key: InternalRow): CompactBuffer[InternalRow] = hashTable.get(key)

  override def writeExternal(out: ObjectOutput): Unit = {
    writeBytes(out, SparkSqlSerializer.serialize(hashTable))
  }

  override def readExternal(in: ObjectInput): Unit = {
    hashTable = SparkSqlSerializer.deserialize(readBytes(in))
  }
}


/**
 * A specialized [[HashedRelation]] that maps key into a single value. This implementation
 * assumes the key is unique.
 */
private[joins]
final class UniqueKeyHashedRelation(private var hashTable: JavaHashMap[InternalRow, InternalRow])
  extends HashedRelation with Externalizable {

  def this() = this(null) // Needed for serialization

  override def get(key: InternalRow): CompactBuffer[InternalRow] = {
    val v = hashTable.get(key)
    if (v eq null) null else CompactBuffer(v)
  }

  def getValue(key: InternalRow): InternalRow = hashTable.get(key)

  override def writeExternal(out: ObjectOutput): Unit = {
    writeBytes(out, SparkSqlSerializer.serialize(hashTable))
  }

  override def readExternal(in: ObjectInput): Unit = {
    hashTable = SparkSqlSerializer.deserialize(readBytes(in))
  }
}

// TODO(rxin): a version of [[HashedRelation]] backed by arrays for consecutive integer keys.


private[joins] object HashedRelation {

  def apply(
      input: Iterator[InternalRow],
      keyGenerator: Projection,
      sizeEstimate: Int = 64): HashedRelation = {

    // TODO: Use Spark's HashMap implementation.
    val hashTable = new JavaHashMap[InternalRow, CompactBuffer[InternalRow]](sizeEstimate)
    var currentRow: InternalRow = null

    // Whether the join key is unique. If the key is unique, we can convert the underlying
    // hash map into one specialized for this.
    var keyIsUnique = true

    // Create a mapping of buildKeys -> rows
    while (input.hasNext) {
      currentRow = input.next()
      val rowKey = keyGenerator(currentRow)
      if (!rowKey.anyNull) {
        val existingMatchList = hashTable.get(rowKey)
        val matchList = if (existingMatchList == null) {
          val newMatchList = new CompactBuffer[InternalRow]()
          hashTable.put(rowKey.copy(), newMatchList)
          newMatchList
        } else {
          keyIsUnique = false
          existingMatchList
        }
        matchList += currentRow.copy()
      }
    }

    if (keyIsUnique) {
      val uniqHashTable = new JavaHashMap[InternalRow, InternalRow](hashTable.size)
      val iter = hashTable.entrySet().iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        uniqHashTable.put(entry.getKey, entry.getValue()(0))
      }
      new UniqueKeyHashedRelation(uniqHashTable)
    } else {
      new GeneralHashedRelation(hashTable)
    }
  }
}

/**
 * An extended CompactBuffer that could grow and update.
 */
class MutableCompactBuffer[T: ClassTag] extends CompactBuffer[T] {
  override def growToSize(newSize: Int): Unit = super.growToSize(newSize)
  override def update(i: Int, v: T): Unit = super.update(i, v)
}

/**
 * A HashedRelation for UnsafeRow, which is backed by BytesToBytesMap that maps the key into a
 * sequence of values.
 */
private[joins] final class UnsafeHashedRelation(
    private var hashTable: JavaHashMap[UnsafeRow, CompactBuffer[UnsafeRow]])
  extends HashedRelation with Externalizable {

  def this() = this(null)  // Needed for serialization

  // Use BytesToBytesMap in executor for better performance (it's created when deserialization)
  @transient private[this] var binaryMap: BytesToBytesMap = _

  // A pool of compact buffers to reduce memory garbage
  @transient private[this] val bufferPool = new ThreadLocal[MutableCompactBuffer[UnsafeRow]]

  override def get(key: InternalRow): CompactBuffer[InternalRow] = {
    val unsafeKey = key.asInstanceOf[UnsafeRow]

    if (binaryMap != null) {
      // Used in Broadcast join
      val loc = binaryMap.lookup(unsafeKey.getBaseObject, unsafeKey.getBaseOffset,
        unsafeKey.getSizeInBytes)
      if (loc.isDefined) {
        // thread-local buffer
        var buffer = bufferPool.get()
        if (buffer == null) {
          buffer = new MutableCompactBuffer[UnsafeRow]
          bufferPool.set(buffer)
        }

        val base = loc.getValueAddress.getBaseObject
        var offset = loc.getValueAddress.getBaseOffset
        val last = loc.getValueAddress.getBaseOffset + loc.getValueLength
        var i = 0
        while (offset < last) {
          val numFields = PlatformDependent.UNSAFE.getInt(base, offset)
          val sizeInBytes = PlatformDependent.UNSAFE.getInt(base, offset + 4)
          offset += 8

          // try to re-use the UnsafeRow in buffer, to reduce garbage
          buffer.growToSize(i + 1)
          if (buffer(i) == null) {
            buffer(i) = new UnsafeRow
          }
          buffer(i).pointTo(base, offset, numFields, sizeInBytes, null)
          i += 1
          offset += sizeInBytes
        }
        buffer.asInstanceOf[CompactBuffer[InternalRow]]
      } else {
        null
      }

    } else {
      // Use the JavaHashMap in Local mode or ShuffleHashJoin
      hashTable.get(unsafeKey).asInstanceOf[CompactBuffer[InternalRow]]
    }
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeInt(hashTable.size())

    val iter = hashTable.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val key = entry.getKey
      val values = entry.getValue

      // write all the values as single byte array
      var totalSize = 0L
      var i = 0
      while (i < values.size) {
        totalSize += values(i).getSizeInBytes + 4 + 4
        i += 1
      }
      assert(totalSize < Integer.MAX_VALUE, "values are too big")

      // [key size] [values size] [key bytes] [values bytes]
      out.writeInt(key.getSizeInBytes)
      out.writeInt(totalSize.toInt)
      out.write(key.getBytes)
      i = 0
      while (i < values.size) {
        // [num of fields] [num of bytes] [row bytes]
        // write the integer in native order, so they can be read by UNSAFE.getInt()
        if (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN) {
          out.writeInt(values(i).length())
          out.writeInt(values(i).getSizeInBytes)
        } else {
          out.writeInt(Integer.reverseBytes(values(i).length()))
          out.writeInt(Integer.reverseBytes(values(i).getSizeInBytes))
        }
        out.write(values(i).getBytes)
        i += 1
      }
    }
  }

  override def readExternal(in: ObjectInput): Unit = {
    val nKeys = in.readInt()
    // This is used in Broadcast, shared by multiple tasks, so we use on-heap memory
    val memoryManager = new TaskMemoryManager(new ExecutorMemoryManager(MemoryAllocator.HEAP))
    binaryMap = new BytesToBytesMap(memoryManager, nKeys * 2) // reduce hash collision

    var i = 0
    var keyBuffer = new Array[Byte](1024)
    var valuesBuffer = new Array[Byte](1024)
    while (i < nKeys) {
      val keySize = in.readInt()
      val valuesSize = in.readInt()
      if (keySize > keyBuffer.size) {
        keyBuffer = new Array[Byte](keySize)
      }
      in.readFully(keyBuffer, 0, keySize)
      if (valuesSize > valuesBuffer.size) {
        valuesBuffer = new Array[Byte](valuesSize)
      }
      in.readFully(valuesBuffer, 0, valuesSize)

      // put it into binary map
      val loc = binaryMap.lookup(keyBuffer, PlatformDependent.BYTE_ARRAY_OFFSET, keySize)
      assert(!loc.isDefined, "Duplicated key found!")
      loc.putNewKey(keyBuffer, PlatformDependent.BYTE_ARRAY_OFFSET, keySize,
        valuesBuffer, PlatformDependent.BYTE_ARRAY_OFFSET, valuesSize)
      i += 1
    }
  }
}

private[joins] object UnsafeHashedRelation {

  def apply(
      input: Iterator[InternalRow],
      buildKeys: Seq[Expression],
      buildPlan: SparkPlan,
      sizeEstimate: Int = 64): HashedRelation = {
    val boundedKeys = buildKeys.map(BindReferences.bindReference(_, buildPlan.output))
    apply(input, boundedKeys, buildPlan.schema, sizeEstimate)
  }

  // Used for tests
  def apply(
      input: Iterator[InternalRow],
      buildKeys: Seq[Expression],
      rowSchema: StructType,
      sizeEstimate: Int): HashedRelation = {

    val hashTable = new JavaHashMap[UnsafeRow, CompactBuffer[UnsafeRow]](sizeEstimate)
    val toUnsafe = UnsafeProjection.create(rowSchema)
    val keyGenerator = UnsafeProjection.create(buildKeys)

    // Create a mapping of buildKeys -> rows
    while (input.hasNext) {
      val currentRow = input.next()
      val unsafeRow = if (currentRow.isInstanceOf[UnsafeRow]) {
        currentRow.asInstanceOf[UnsafeRow]
      } else {
        toUnsafe(currentRow)
      }
      val rowKey = keyGenerator(unsafeRow)
      if (!rowKey.anyNull) {
        val existingMatchList = hashTable.get(rowKey)
        val matchList = if (existingMatchList == null) {
          val newMatchList = new CompactBuffer[UnsafeRow]()
          hashTable.put(rowKey.copy(), newMatchList)
          newMatchList
        } else {
          existingMatchList
        }
        matchList += unsafeRow.copy()
      }
    }

    new UnsafeHashedRelation(hashTable)
  }
}
