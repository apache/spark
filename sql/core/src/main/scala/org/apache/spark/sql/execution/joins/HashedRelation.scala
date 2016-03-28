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

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}
import java.util.{HashMap => JavaHashMap}

import org.apache.spark.{SparkConf, SparkEnv, SparkException, TaskContext}
import org.apache.spark.memory.{StaticMemoryManager, TaskMemoryManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution.SparkSqlSerializer
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.hash.Murmur3_x86_32
import org.apache.spark.unsafe.map.BytesToBytesMap
import org.apache.spark.util.{KnownSizeEstimation, Utils}
import org.apache.spark.util.collection.CompactBuffer

/**
 * Interface for a hashed relation by some key. Use [[HashedRelation.apply]] to create a concrete
 * object.
 */
private[execution] sealed trait HashedRelation {
  /**
    * Returns matched rows.
    */
  def get(key: InternalRow): Seq[InternalRow]

  /**
    * Returns matched rows for a key that has only one column with LongType.
    */
  def get(key: Long): Seq[InternalRow] = {
    throw new UnsupportedOperationException
  }

  /**
    * Returns the size of used memory.
    */
  def getMemorySize: Long = 1L  // to make the test happy

  /**
   * Release any used resources.
   */
  def close(): Unit = {}

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
  * Interface for a hashed relation that have only one row per key.
  *
  * We should call getValue() for better performance.
  */
private[execution] trait UniqueHashedRelation extends HashedRelation {

  /**
    * Returns the matched single row.
    */
  def getValue(key: InternalRow): InternalRow

  /**
    * Returns the matched single row with key that have only one column of LongType.
    */
  def getValue(key: Long): InternalRow = {
    throw new UnsupportedOperationException
  }

  override def get(key: InternalRow): Seq[InternalRow] = {
    val row = getValue(key)
    if (row != null) {
      CompactBuffer[InternalRow](row)
    } else {
      null
    }
  }

  override def get(key: Long): Seq[InternalRow] = {
    val row = getValue(key)
    if (row != null) {
      CompactBuffer[InternalRow](row)
    } else {
      null
    }
  }
}

private[execution] object HashedRelation {

  /**
   * Create a HashedRelation from an Iterator of InternalRow.
   *
   * Note: The caller should make sure that these InternalRow are different objects.
   */
  def apply(
      canJoinKeyFitWithinLong: Boolean,
      input: Iterator[InternalRow],
      keyGenerator: Projection,
      sizeEstimate: Int = 64): HashedRelation = {

    if (canJoinKeyFitWithinLong) {
      LongHashedRelation(input, keyGenerator, sizeEstimate)
    } else {
      UnsafeHashedRelation(
        input, keyGenerator.asInstanceOf[UnsafeProjection], sizeEstimate)
    }
  }
}

/**
 * A HashedRelation for UnsafeRow, which is backed BytesToBytesMap.
 *
 * It's serialized in the following format:
 *  [number of keys]
 *  [size of key] [size of value] [key bytes] [bytes for value]
 */
private[joins] class UnsafeHashedRelation(
    private var numFields: Int,
    private var binaryMap: BytesToBytesMap)
  extends HashedRelation with KnownSizeEstimation with Externalizable {

  private[joins] def this() = this(0, null)  // Needed for serialization

  override def getMemorySize: Long = {
    binaryMap.getTotalMemoryConsumption
  }

  override def estimatedSize: Long = {
    binaryMap.getTotalMemoryConsumption
  }

  override def get(key: InternalRow): Seq[InternalRow] = {
    val unsafeKey = key.asInstanceOf[UnsafeRow]
    val map = binaryMap  // avoid the compiler error
    val loc = new map.Location  // this could be allocated in stack
    binaryMap.safeLookup(unsafeKey.getBaseObject, unsafeKey.getBaseOffset,
      unsafeKey.getSizeInBytes, loc, unsafeKey.hashCode())
    if (loc.isDefined) {
      val buffer = CompactBuffer[UnsafeRow]()
      val row = new UnsafeRow(numFields)
      row.pointTo(loc.getValueBase, loc.getValueOffset, loc.getValueLength)
      buffer += row
      while (loc.nextValue()) {
        val row = new UnsafeRow(numFields)
        row.pointTo(loc.getValueBase, loc.getValueOffset, loc.getValueLength)
        buffer += row
      }
      buffer
    } else {
      null
    }
  }

  override def close(): Unit = {
    binaryMap.free()
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    out.writeInt(numFields)
    // TODO: move these into BytesToBytesMap
    out.writeInt(binaryMap.numKeys())
    out.writeInt(binaryMap.numValues())

    var buffer = new Array[Byte](64)
    def write(base: Object, offset: Long, length: Int): Unit = {
      if (buffer.length < length) {
        buffer = new Array[Byte](length)
      }
      Platform.copyMemory(base, offset, buffer, Platform.BYTE_ARRAY_OFFSET, length)
      out.write(buffer, 0, length)
    }

    val iter = binaryMap.iterator()
    while (iter.hasNext) {
      val loc = iter.next()
      // [key size] [values size] [key bytes] [value bytes]
      out.writeInt(loc.getKeyLength)
      out.writeInt(loc.getValueLength)
      write(loc.getKeyBase, loc.getKeyOffset, loc.getKeyLength)
      write(loc.getValueBase, loc.getValueOffset, loc.getValueLength)
    }
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    numFields = in.readInt()
    val nKeys = in.readInt()
    val nValues = in.readInt()
    // This is used in Broadcast, shared by multiple tasks, so we use on-heap memory
    // TODO(josh): This needs to be revisited before we merge this patch; making this change now
    // so that tests compile:
    val taskMemoryManager = new TaskMemoryManager(
      new StaticMemoryManager(
        new SparkConf().set("spark.memory.offHeap.enabled", "false"),
        Long.MaxValue,
        Long.MaxValue,
        1),
      0)

    val pageSizeBytes = Option(SparkEnv.get).map(_.memoryManager.pageSizeBytes)
      .getOrElse(new SparkConf().getSizeAsBytes("spark.buffer.pageSize", "16m"))

    // TODO(josh): We won't need this dummy memory manager after future refactorings; revisit
    // during code review

    binaryMap = new BytesToBytesMap(
      taskMemoryManager,
      (nKeys * 1.5 + 1).toInt, // reduce hash collision
      pageSizeBytes)

    var i = 0
    var keyBuffer = new Array[Byte](1024)
    var valuesBuffer = new Array[Byte](1024)
    while (i < nValues) {
      val keySize = in.readInt()
      val valuesSize = in.readInt()
      if (keySize > keyBuffer.length) {
        keyBuffer = new Array[Byte](keySize)
      }
      in.readFully(keyBuffer, 0, keySize)
      if (valuesSize > valuesBuffer.length) {
        valuesBuffer = new Array[Byte](valuesSize)
      }
      in.readFully(valuesBuffer, 0, valuesSize)

      val loc = binaryMap.lookup(keyBuffer, Platform.BYTE_ARRAY_OFFSET, keySize)
      val putSuceeded = loc.append(keyBuffer, Platform.BYTE_ARRAY_OFFSET, keySize,
        valuesBuffer, Platform.BYTE_ARRAY_OFFSET, valuesSize)
      if (!putSuceeded) {
        binaryMap.free()
        throw new IOException("Could not allocate memory to grow BytesToBytesMap")
      }
      i += 1
    }
  }
}

/**
 * A HashedRelation for UnsafeRow with unique keys.
 */
private[joins] final class UniqueUnsafeHashedRelation(
    private var numFields: Int,
    private var binaryMap: BytesToBytesMap)
  extends UnsafeHashedRelation(numFields, binaryMap) with UniqueHashedRelation {
  def getValue(key: InternalRow): InternalRow = {
    val unsafeKey = key.asInstanceOf[UnsafeRow]
    val map = binaryMap  // avoid the compiler error
    val loc = new map.Location  // this could be allocated in stack
    binaryMap.safeLookup(unsafeKey.getBaseObject, unsafeKey.getBaseOffset,
      unsafeKey.getSizeInBytes, loc, unsafeKey.hashCode())
    if (loc.isDefined) {
      val row = new UnsafeRow(numFields)
      row.pointTo(loc.getValueBase, loc.getValueOffset, loc.getValueLength)
      row
    } else {
      null
    }
  }
}

private[joins] object UnsafeHashedRelation {

  def apply(
      input: Iterator[InternalRow],
      keyGenerator: UnsafeProjection,
      sizeEstimate: Int): HashedRelation = {

    val taskMemoryManager = if (TaskContext.get() != null) {
      TaskContext.get().taskMemoryManager()
    } else {
      new TaskMemoryManager(
        new StaticMemoryManager(
          new SparkConf().set("spark.memory.offHeap.enabled", "false"),
          Long.MaxValue,
          Long.MaxValue,
          1),
        0)
    }
    val pageSizeBytes = Option(SparkEnv.get).map(_.memoryManager.pageSizeBytes)
      .getOrElse(new SparkConf().getSizeAsBytes("spark.buffer.pageSize", "16m"))

    val binaryMap = new BytesToBytesMap(
      taskMemoryManager,
      // Only 70% of the slots can be used before growing, more capacity help to reduce collision
      (sizeEstimate * 1.5 + 1).toInt,
      pageSizeBytes)

    // Create a mapping of buildKeys -> rows
    var numFields = 0
    // Whether all the keys are unique or not
    var allUnique: Boolean = true
    while (input.hasNext) {
      val row = input.next().asInstanceOf[UnsafeRow]
      numFields = row.numFields()
      val key = keyGenerator(row)
      if (!key.anyNull) {
        val loc = binaryMap.lookup(key.getBaseObject, key.getBaseOffset, key.getSizeInBytes)
        if (loc.isDefined) {
          allUnique = false
        }
        val success = loc.append(
          key.getBaseObject, key.getBaseOffset, key.getSizeInBytes,
          row.getBaseObject, row.getBaseOffset, row.getSizeInBytes)
        if (!success) {
          binaryMap.free()
          throw new SparkException("There is no enough memory to build hash map")
        }
      }
    }

    if (allUnique) {
      new UniqueUnsafeHashedRelation(numFields, binaryMap)
    } else {
      new UnsafeHashedRelation(numFields, binaryMap)
    }
  }
}

/**
  * An interface for a hashed relation that the key is a Long.
  */
private[joins] trait LongHashedRelation extends HashedRelation {
  override def get(key: InternalRow): Seq[InternalRow] = {
    get(key.getLong(0))
  }
}

private[joins] final class GeneralLongHashedRelation(
  private var hashTable: JavaHashMap[Long, CompactBuffer[UnsafeRow]])
  extends LongHashedRelation with Externalizable {

  // Needed for serialization (it is public to make Java serialization work)
  def this() = this(null)

  override def get(key: Long): Seq[InternalRow] = hashTable.get(key)

  override def writeExternal(out: ObjectOutput): Unit = {
    writeBytes(out, SparkSqlSerializer.serialize(hashTable))
  }

  override def readExternal(in: ObjectInput): Unit = {
    hashTable = SparkSqlSerializer.deserialize(readBytes(in))
  }
}

private[joins] final class UniqueLongHashedRelation(
  private var hashTable: JavaHashMap[Long, UnsafeRow])
  extends UniqueHashedRelation with LongHashedRelation with Externalizable {

  // Needed for serialization (it is public to make Java serialization work)
  def this() = this(null)

  override def getValue(key: InternalRow): InternalRow = {
    getValue(key.getLong(0))
  }

  override def getValue(key: Long): InternalRow = {
    hashTable.get(key)
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    writeBytes(out, SparkSqlSerializer.serialize(hashTable))
  }

  override def readExternal(in: ObjectInput): Unit = {
    hashTable = SparkSqlSerializer.deserialize(readBytes(in))
  }
}

/**
  * A relation that pack all the rows into a byte array, together with offsets and sizes.
  *
  * All the bytes of UnsafeRow are packed together as `bytes`:
  *
  *  [  Row0  ][  Row1  ][] ... [  RowN  ]
  *
  * With keys:
  *
  *   start    start+1   ...       start+N
  *
  * `offsets` are offsets of UnsafeRows in the `bytes`
  * `sizes` are the numbers of bytes of UnsafeRows, 0 means no row for this key.
  *
  *  For example, two UnsafeRows (24 bytes and 32 bytes), with keys as 3 and 5 will stored as:
  *
  *  start   = 3
  *  offsets = [0, 0, 24]
  *  sizes   = [24, 0, 32]
  *  bytes   = [0 - 24][][24 - 56]
  */
private[joins] final class LongArrayRelation(
    private var numFields: Int,
    private var start: Long,
    private var offsets: Array[Int],
    private var sizes: Array[Int],
    private var bytes: Array[Byte]
  ) extends UniqueHashedRelation with LongHashedRelation with Externalizable {

  // Needed for serialization (it is public to make Java serialization work)
  def this() = this(0, 0L, null, null, null)

  override def getValue(key: InternalRow): InternalRow = {
    getValue(key.getLong(0))
  }

  override def getMemorySize: Long = {
    offsets.length * 4 + sizes.length * 4 + bytes.length
  }

  override def getValue(key: Long): InternalRow = {
    val idx = (key - start).toInt
    if (idx >= 0 && idx < sizes.length && sizes(idx) > 0) {
      val result = new UnsafeRow(numFields)
      result.pointTo(bytes, Platform.BYTE_ARRAY_OFFSET + offsets(idx), sizes(idx))
      result
    } else {
      null
    }
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeInt(numFields)
    out.writeLong(start)
    out.writeInt(sizes.length)
    var i = 0
    while (i < sizes.length) {
      out.writeInt(sizes(i))
      i += 1
    }
    out.writeInt(bytes.length)
    out.write(bytes)
  }

  override def readExternal(in: ObjectInput): Unit = {
    numFields = in.readInt()
    start = in.readLong()
    val length = in.readInt()
    // read sizes of rows
    sizes = new Array[Int](length)
    offsets = new Array[Int](length)
    var i = 0
    var offset = 0
    while (i < length) {
      offsets(i) = offset
      sizes(i) = in.readInt()
      offset += sizes(i)
      i += 1
    }
    // read all the bytes
    val total = in.readInt()
    assert(total == offset)
    bytes = new Array[Byte](total)
    in.readFully(bytes)
  }
}

/**
  * Create hashed relation with key that is long.
  */
private[joins] object LongHashedRelation {

  val DENSE_FACTOR = 0.2

  def apply(
    input: Iterator[InternalRow],
    keyGenerator: Projection,
    sizeEstimate: Int): HashedRelation = {

    // TODO: use LongToBytesMap for better memory efficiency
    val hashTable = new JavaHashMap[Long, CompactBuffer[UnsafeRow]](sizeEstimate)

    // Create a mapping of key -> rows
    var numFields = 0
    var keyIsUnique = true
    var minKey = Long.MaxValue
    var maxKey = Long.MinValue
    while (input.hasNext) {
      val unsafeRow = input.next().asInstanceOf[UnsafeRow]
      numFields = unsafeRow.numFields()
      val rowKey = keyGenerator(unsafeRow)
      if (!rowKey.anyNull) {
        val key = rowKey.getLong(0)
        minKey = math.min(minKey, key)
        maxKey = math.max(maxKey, key)
        val existingMatchList = hashTable.get(key)
        val matchList = if (existingMatchList == null) {
          val newMatchList = new CompactBuffer[UnsafeRow]()
          hashTable.put(key, newMatchList)
          newMatchList
        } else {
          keyIsUnique = false
          existingMatchList
        }
        matchList += unsafeRow
      }
    }

    if (keyIsUnique) {
      if (hashTable.size() > (maxKey - minKey) * DENSE_FACTOR) {
        // The keys are dense enough, so use LongArrayRelation
        val length = (maxKey - minKey).toInt + 1
        val sizes = new Array[Int](length)
        val offsets = new Array[Int](length)
        var offset = 0
        var i = 0
        while (i < length) {
          val rows = hashTable.get(i + minKey)
          if (rows != null) {
            offsets(i) = offset
            sizes(i) = rows(0).getSizeInBytes
            offset += sizes(i)
          }
          i += 1
        }
        val bytes = new Array[Byte](offset)
        i = 0
        while (i < length) {
          val rows = hashTable.get(i + minKey)
          if (rows != null) {
            rows(0).writeToMemory(bytes, Platform.BYTE_ARRAY_OFFSET + offsets(i))
          }
          i += 1
        }
        new LongArrayRelation(numFields, minKey, offsets, sizes, bytes)

      } else {
        // all the keys are unique, one row per key.
        val uniqHashTable = new JavaHashMap[Long, UnsafeRow](hashTable.size)
        val iter = hashTable.entrySet().iterator()
        while (iter.hasNext) {
          val entry = iter.next()
          uniqHashTable.put(entry.getKey, entry.getValue()(0))
        }
        new UniqueLongHashedRelation(uniqHashTable)
      }
    } else {
      new GeneralLongHashedRelation(hashTable)
    }
  }
}

/** The HashedRelationBroadcastMode requires that rows are broadcasted as a HashedRelation. */
private[execution] case class HashedRelationBroadcastMode(
    canJoinKeyFitWithinLong: Boolean,
    keys: Seq[Expression],
    attributes: Seq[Attribute]) extends BroadcastMode {

  override def transform(rows: Array[InternalRow]): HashedRelation = {
    val generator = UnsafeProjection.create(keys, attributes)
    HashedRelation(canJoinKeyFitWithinLong, rows.iterator, generator, rows.length)
  }

  private lazy val canonicalizedKeys: Seq[Expression] = {
    keys.map { e =>
      BindReferences.bindReference(e.canonicalized, attributes)
    }
  }

  override def compatibleWith(other: BroadcastMode): Boolean = other match {
    case m: HashedRelationBroadcastMode =>
      canJoinKeyFitWithinLong == m.canJoinKeyFitWithinLong &&
        canonicalizedKeys == m.canonicalizedKeys
    case _ => false
  }
}
