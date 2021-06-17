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

import java.io._

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}

import org.apache.spark.{SparkConf, SparkEnv, SparkException}
import org.apache.spark.internal.config.{BUFFER_PAGESIZE, MEMORY_OFFHEAP_ENABLED}
import org.apache.spark.memory._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.types.LongType
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.map.BytesToBytesMap
import org.apache.spark.util.{KnownSizeEstimation, Utils}

/**
 * Interface for a hashed relation by some key. Use [[HashedRelation.apply]] to create a concrete
 * object.
 */
private[execution] sealed trait HashedRelation extends KnownSizeEstimation {
  /**
   * Returns matched rows.
   *
   * Returns null if there is no matched rows.
   */
  def get(key: InternalRow): Iterator[InternalRow]

  /**
   * Returns matched rows for a key that has only one column with LongType.
   *
   * Returns null if there is no matched rows.
   */
  def get(key: Long): Iterator[InternalRow] = {
    throw new UnsupportedOperationException
  }

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

  /**
   * Returns an iterator for key index and matched rows.
   *
   * Returns null if there is no matched rows.
   */
  def getWithKeyIndex(key: InternalRow): Iterator[ValueRowWithKeyIndex] = {
    throw new UnsupportedOperationException
  }

  /**
   * Returns key index and matched single row.
   * This is for unique key case.
   *
   * Returns null if there is no matched rows.
   */
  def getValueWithKeyIndex(key: InternalRow): ValueRowWithKeyIndex = {
    throw new UnsupportedOperationException
  }

  /**
   * Returns an iterator for keys index and rows of InternalRow type.
   */
  def valuesWithKeyIndex(): Iterator[ValueRowWithKeyIndex] = {
    throw new UnsupportedOperationException
  }

  /**
   * Returns the maximum number of allowed keys index.
   */
  def maxNumKeysIndex: Int = {
    throw new UnsupportedOperationException
  }

  /**
   * Returns true iff all the keys are unique.
   */
  def keyIsUnique: Boolean

  /**
   * Returns an iterator for keys of InternalRow type.
   */
  def keys(): Iterator[InternalRow]

  /**
   * Returns a read-only copy of this, to be safely used in current thread.
   */
  def asReadOnlyCopy(): HashedRelation

  /**
   * Release any used resources.
   */
  def close(): Unit
}

private[execution] object HashedRelation {

  /**
   * Create a HashedRelation from an Iterator of InternalRow.
   *
   * @param allowsNullKey Allow NULL keys in HashedRelation.
   *                      This is used for full outer join in `ShuffledHashJoinExec` only.
   */
  def apply(
      input: Iterator[InternalRow],
      key: Seq[Expression],
      sizeEstimate: Int = 64,
      taskMemoryManager: TaskMemoryManager = null,
      isNullAware: Boolean = false,
      allowsNullKey: Boolean = false): HashedRelation = {
    val mm = Option(taskMemoryManager).getOrElse {
      new TaskMemoryManager(
        new UnifiedMemoryManager(
          new SparkConf().set(MEMORY_OFFHEAP_ENABLED.key, "false"),
          Long.MaxValue,
          Long.MaxValue / 2,
          1),
        0)
    }

    if (!input.hasNext && !allowsNullKey) {
      EmptyHashedRelation
    } else if (key.length == 1 && key.head.dataType == LongType && !allowsNullKey) {
      // NOTE: LongHashedRelation does not support NULL keys.
      LongHashedRelation(input, key, sizeEstimate, mm, isNullAware)
    } else {
      UnsafeHashedRelation(input, key, sizeEstimate, mm, isNullAware, allowsNullKey)
    }
  }
}

/**
 * A wrapper for key index and value in InternalRow type.
 * Designed to be instantiated once per thread and reused.
 */
private[execution] class ValueRowWithKeyIndex {
  private var keyIndex: Int = _
  private var value: InternalRow = _

  /** Updates this ValueRowWithKeyIndex by updating its key index.  Returns itself. */
  def withNewKeyIndex(newKeyIndex: Int): ValueRowWithKeyIndex = {
    keyIndex = newKeyIndex
    this
  }

  /** Updates this ValueRowWithKeyIndex by updating its value.  Returns itself. */
  def withNewValue(newValue: InternalRow): ValueRowWithKeyIndex = {
    value = newValue
    this
  }

  /** Updates this ValueRowWithKeyIndex.  Returns itself. */
  def update(newKeyIndex: Int, newValue: InternalRow): ValueRowWithKeyIndex = {
    keyIndex = newKeyIndex
    value = newValue
    this
  }

  def getKeyIndex: Int = {
    keyIndex
  }

  def getValue: InternalRow = {
    value
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
    private var numKeys: Int,
    private var numFields: Int,
    private var binaryMap: BytesToBytesMap)
  extends HashedRelation with Externalizable with KryoSerializable {

  private[joins] def this() = this(0, 0, null)  // Needed for serialization

  override def keyIsUnique: Boolean = binaryMap.numKeys() == binaryMap.numValues()

  override def asReadOnlyCopy(): UnsafeHashedRelation = {
    new UnsafeHashedRelation(numKeys, numFields, binaryMap)
  }

  override def estimatedSize: Long = binaryMap.getTotalMemoryConsumption

  // re-used in get()/getValue()/getWithKeyIndex()/getValueWithKeyIndex()/valuesWithKeyIndex()
  var resultRow = new UnsafeRow(numFields)

  // re-used in getWithKeyIndex()/getValueWithKeyIndex()/valuesWithKeyIndex()
  var valueRowWithKeyIndex = new ValueRowWithKeyIndex

  override def get(key: InternalRow): Iterator[InternalRow] = {
    val unsafeKey = key.asInstanceOf[UnsafeRow]
    val map = binaryMap  // avoid the compiler error
    val loc = new map.Location  // this could be allocated in stack
    binaryMap.safeLookup(unsafeKey.getBaseObject, unsafeKey.getBaseOffset,
      unsafeKey.getSizeInBytes, loc, unsafeKey.hashCode())
    if (loc.isDefined) {
      new Iterator[UnsafeRow] {
        private var _hasNext = true
        override def hasNext: Boolean = _hasNext
        override def next(): UnsafeRow = {
          resultRow.pointTo(loc.getValueBase, loc.getValueOffset, loc.getValueLength)
          _hasNext = loc.nextValue()
          resultRow
        }
      }
    } else {
      null
    }
  }

  def getValue(key: InternalRow): InternalRow = {
    val unsafeKey = key.asInstanceOf[UnsafeRow]
    val map = binaryMap  // avoid the compiler error
    val loc = new map.Location  // this could be allocated in stack
    binaryMap.safeLookup(unsafeKey.getBaseObject, unsafeKey.getBaseOffset,
      unsafeKey.getSizeInBytes, loc, unsafeKey.hashCode())
    if (loc.isDefined) {
      resultRow.pointTo(loc.getValueBase, loc.getValueOffset, loc.getValueLength)
      resultRow
    } else {
      null
    }
  }

  override def getWithKeyIndex(key: InternalRow): Iterator[ValueRowWithKeyIndex] = {
    val unsafeKey = key.asInstanceOf[UnsafeRow]
    val map = binaryMap  // avoid the compiler error
    val loc = new map.Location  // this could be allocated in stack
    binaryMap.safeLookup(unsafeKey.getBaseObject, unsafeKey.getBaseOffset,
      unsafeKey.getSizeInBytes, loc, unsafeKey.hashCode())
    if (loc.isDefined) {
      valueRowWithKeyIndex.withNewKeyIndex(loc.getKeyIndex)
      new Iterator[ValueRowWithKeyIndex] {
        private var _hasNext = true
        override def hasNext: Boolean = _hasNext
        override def next(): ValueRowWithKeyIndex = {
          resultRow.pointTo(loc.getValueBase, loc.getValueOffset, loc.getValueLength)
          _hasNext = loc.nextValue()
          valueRowWithKeyIndex.withNewValue(resultRow)
        }
      }
    } else {
      null
    }
  }

  override def getValueWithKeyIndex(key: InternalRow): ValueRowWithKeyIndex = {
    val unsafeKey = key.asInstanceOf[UnsafeRow]
    val map = binaryMap  // avoid the compiler error
    val loc = new map.Location  // this could be allocated in stack
    binaryMap.safeLookup(unsafeKey.getBaseObject, unsafeKey.getBaseOffset,
      unsafeKey.getSizeInBytes, loc, unsafeKey.hashCode())
    if (loc.isDefined) {
      resultRow.pointTo(loc.getValueBase, loc.getValueOffset, loc.getValueLength)
      valueRowWithKeyIndex.update(loc.getKeyIndex, resultRow)
    } else {
      null
    }
  }

  override def valuesWithKeyIndex(): Iterator[ValueRowWithKeyIndex] = {
    val iter = binaryMap.iteratorWithKeyIndex()

    new Iterator[ValueRowWithKeyIndex] {
      override def hasNext: Boolean = iter.hasNext

      override def next(): ValueRowWithKeyIndex = {
        if (!hasNext) {
          throw new NoSuchElementException("End of the iterator")
        }
        val loc = iter.next()
        resultRow.pointTo(loc.getValueBase, loc.getValueOffset, loc.getValueLength)
        valueRowWithKeyIndex.update(loc.getKeyIndex, resultRow)
      }
    }
  }

  override def maxNumKeysIndex: Int = {
    binaryMap.maxNumKeysIndex
  }

  override def keys(): Iterator[InternalRow] = {
    val iter = binaryMap.iterator()

    new Iterator[InternalRow] {
      val unsafeRow = new UnsafeRow(numKeys)

      override def hasNext: Boolean = {
        iter.hasNext
      }

      override def next(): InternalRow = {
        if (!hasNext) {
          throw new NoSuchElementException("End of the iterator")
        } else {
          val loc = iter.next()
          unsafeRow.pointTo(loc.getKeyBase, loc.getKeyOffset, loc.getKeyLength)
          unsafeRow
        }
      }
    }
  }

  override def close(): Unit = {
    binaryMap.free()
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    write(out.writeInt, out.writeLong, out.write)
  }

  override def write(kryo: Kryo, out: Output): Unit = Utils.tryOrIOException {
    write(out.writeInt, out.writeLong, out.write)
  }

  private def write(
      writeInt: (Int) => Unit,
      writeLong: (Long) => Unit,
      writeBuffer: (Array[Byte], Int, Int) => Unit) : Unit = {
    writeInt(numFields)
    // TODO: move these into BytesToBytesMap
    writeLong(binaryMap.numKeys())
    writeLong(binaryMap.numValues())

    var buffer = new Array[Byte](64)
    def write(base: Object, offset: Long, length: Int): Unit = {
      if (buffer.length < length) {
        buffer = new Array[Byte](length)
      }
      Platform.copyMemory(base, offset, buffer, Platform.BYTE_ARRAY_OFFSET, length)
      writeBuffer(buffer, 0, length)
    }

    val iter = binaryMap.iterator()
    while (iter.hasNext) {
      val loc = iter.next()
      // [key size] [values size] [key bytes] [value bytes]
      writeInt(loc.getKeyLength)
      writeInt(loc.getValueLength)
      write(loc.getKeyBase, loc.getKeyOffset, loc.getKeyLength)
      write(loc.getValueBase, loc.getValueOffset, loc.getValueLength)
    }
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    read(() => in.readInt(), () => in.readLong(), in.readFully)
  }

  private def read(
      readInt: () => Int,
      readLong: () => Long,
      readBuffer: (Array[Byte], Int, Int) => Unit): Unit = {
    numFields = readInt()
    resultRow = new UnsafeRow(numFields)
    val nKeys = readLong()
    val nValues = readLong()
    // This is used in Broadcast, shared by multiple tasks, so we use on-heap memory
    // TODO(josh): This needs to be revisited before we merge this patch; making this change now
    // so that tests compile:
    val taskMemoryManager = new TaskMemoryManager(
      new UnifiedMemoryManager(
        new SparkConf().set(MEMORY_OFFHEAP_ENABLED.key, "false"),
        Long.MaxValue,
        Long.MaxValue / 2,
        1),
      0)

    val pageSizeBytes = Option(SparkEnv.get).map(_.memoryManager.pageSizeBytes)
      .getOrElse(new SparkConf().get(BUFFER_PAGESIZE).getOrElse(16L * 1024 * 1024))

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
      val keySize = readInt()
      val valuesSize = readInt()
      if (keySize > keyBuffer.length) {
        keyBuffer = new Array[Byte](keySize)
      }
      readBuffer(keyBuffer, 0, keySize)
      if (valuesSize > valuesBuffer.length) {
        valuesBuffer = new Array[Byte](valuesSize)
      }
      readBuffer(valuesBuffer, 0, valuesSize)

      val loc = binaryMap.lookup(keyBuffer, Platform.BYTE_ARRAY_OFFSET, keySize)
      val putSucceeded = loc.append(keyBuffer, Platform.BYTE_ARRAY_OFFSET, keySize,
        valuesBuffer, Platform.BYTE_ARRAY_OFFSET, valuesSize)
      if (!putSucceeded) {
        binaryMap.free()
        throw new IOException("Could not allocate memory to grow BytesToBytesMap")
      }
      i += 1
    }
  }

  override def read(kryo: Kryo, in: Input): Unit = Utils.tryOrIOException {
    read(() => in.readInt(), () => in.readLong(), in.readBytes)
  }
}

private[joins] object UnsafeHashedRelation {

  def apply(
      input: Iterator[InternalRow],
      key: Seq[Expression],
      sizeEstimate: Int,
      taskMemoryManager: TaskMemoryManager,
      isNullAware: Boolean = false,
      allowsNullKey: Boolean = false): HashedRelation = {
    require(!(isNullAware && allowsNullKey),
      "isNullAware and allowsNullKey cannot be enabled at same time")

    val pageSizeBytes = Option(SparkEnv.get).map(_.memoryManager.pageSizeBytes)
      .getOrElse(new SparkConf().get(BUFFER_PAGESIZE).getOrElse(16L * 1024 * 1024))
    val binaryMap = new BytesToBytesMap(
      taskMemoryManager,
      // Only 70% of the slots can be used before growing, more capacity help to reduce collision
      (sizeEstimate * 1.5 + 1).toInt,
      pageSizeBytes)

    // Create a mapping of buildKeys -> rows
    val keyGenerator = UnsafeProjection.create(key)
    var numFields = 0
    while (input.hasNext) {
      val row = input.next().asInstanceOf[UnsafeRow]
      numFields = row.numFields()
      val key = keyGenerator(row)
      if (!key.anyNull || allowsNullKey) {
        val loc = binaryMap.lookup(key.getBaseObject, key.getBaseOffset, key.getSizeInBytes)
        val success = loc.append(
          key.getBaseObject, key.getBaseOffset, key.getSizeInBytes,
          row.getBaseObject, row.getBaseOffset, row.getSizeInBytes)
        if (!success) {
          binaryMap.free()
          // scalastyle:off throwerror
          throw new SparkOutOfMemoryError("There is not enough memory to build hash map")
          // scalastyle:on throwerror
        }
      } else if (isNullAware) {
        binaryMap.free()
        return HashedRelationWithAllNullKeys
      }
    }

    new UnsafeHashedRelation(key.size, numFields, binaryMap)
  }
}

/**
 * An append-only hash map mapping from key of Long to UnsafeRow.
 *
 * The underlying bytes of all values (UnsafeRows) are packed together as a single byte array
 * (`page`) in this format:
 *
 *  [bytes of row1][address1][bytes of row2][address1] ...
 *
 *  address1 (8 bytes) is the offset and size of next value for the same key as row1, any key
 *  could have multiple values. the address at the end of last value for every key is 0.
 *
 * The keys and addresses of their values could be stored in two modes:
 *
 * 1) sparse mode: the keys and addresses are stored in `array` as:
 *
 *  [key1][address1][key2][address2]...[]
 *
 *  address1 (Long) is the offset (in `page`) and size of the value for key1. The position of key1
 *  is determined by `key1 % cap`. Quadratic probing with triangular numbers is used to address
 *  hash collision.
 *
 * 2) dense mode: all the addresses are packed into a single array of long, as:
 *
 *  [address1] [address2] ...
 *
 *  address1 (Long) is the offset (in `page`) and size of the value for key1, the position is
 *  determined by `key1 - minKey`.
 *
 * The map is created as sparse mode, then key-value could be appended into it. Once finish
 * appending, caller could call optimize() to try to turn the map into dense mode, which is faster
 * to probe.
 *
 * see http://java-performance.info/implementing-world-fastest-java-int-to-int-hash-map/
 */
private[execution] final class LongToUnsafeRowMap(val mm: TaskMemoryManager, capacity: Int)
  extends MemoryConsumer(mm) with Externalizable with KryoSerializable {

  // Whether the keys are stored in dense mode or not.
  private var isDense = false

  // The minimum key
  private var minKey = Long.MaxValue

  // The maximum key
  private var maxKey = Long.MinValue

  // The array to store the key and offset of UnsafeRow in the page.
  //
  // Sparse mode: [key1] [offset1 | size1] [key2] [offset | size2] ...
  // Dense mode: [offset1 | size1] [offset2 | size2]
  private var array: Array[Long] = null
  private var mask: Int = 0

  // The page to store all bytes of UnsafeRow and the pointer to next rows.
  // [row1][pointer1] [row2][pointer2]
  private var page: Array[Long] = null

  // Current write cursor in the page.
  private var cursor: Long = Platform.LONG_ARRAY_OFFSET

  // The number of bits for size in address
  private val SIZE_BITS = 28
  private val SIZE_MASK = 0xfffffff

  // The total number of values of all keys.
  private var numValues = 0L

  // The number of unique keys.
  private var numKeys = 0L

  // needed by serializer
  def this() = {
    this(
      new TaskMemoryManager(
        new UnifiedMemoryManager(
          new SparkConf().set(MEMORY_OFFHEAP_ENABLED.key, "false"),
          Long.MaxValue,
          Long.MaxValue / 2,
          1),
        0),
      0)
  }

  private def ensureAcquireMemory(size: Long): Unit = {
    // do not support spilling
    val got = acquireMemory(size)
    if (got < size) {
      freeMemory(got)
      throw new SparkException(s"Can't acquire $size bytes memory to build hash relation, " +
        s"got $got bytes")
    }
  }

  private def init(): Unit = {
    if (mm != null) {
      require(capacity < 512000000, "Cannot broadcast 512 million or more rows")
      var n = 1
      while (n < capacity) n *= 2
      ensureAcquireMemory(n * 2L * 8 + (1 << 20))
      array = new Array[Long](n * 2)
      mask = n * 2 - 2
      page = new Array[Long](1 << 17)  // 1M bytes
    }
  }

  init()

  def spill(size: Long, trigger: MemoryConsumer): Long = 0L

  /**
   * Returns whether all the keys are unique.
   */
  def keyIsUnique: Boolean = numKeys == numValues

  /**
   * Returns total memory consumption.
   */
  def getTotalMemoryConsumption: Long = array.length * 8L + page.length * 8L

  /**
   * Returns the first slot of array that store the keys (sparse mode).
   */
  private def firstSlot(key: Long): Int = {
    val h = key * 0x9E3779B9L
    (h ^ (h >> 32)).toInt & mask
  }

  /**
   * Returns the next probe in the array.
   */
  private def nextSlot(pos: Int): Int = (pos + 2) & mask

  private[this] def toAddress(offset: Long, size: Int): Long = {
    ((offset - Platform.LONG_ARRAY_OFFSET) << SIZE_BITS) | size
  }

  private[this] def toOffset(address: Long): Long = {
    (address >>> SIZE_BITS) + Platform.LONG_ARRAY_OFFSET
  }

  private[this] def toSize(address: Long): Int = {
    (address & SIZE_MASK).toInt
  }

  private def getRow(address: Long, resultRow: UnsafeRow): UnsafeRow = {
    resultRow.pointTo(page, toOffset(address), toSize(address))
    resultRow
  }

  /**
   * Returns the single UnsafeRow for given key, or null if not found.
   */
  def getValue(key: Long, resultRow: UnsafeRow): UnsafeRow = {
    if (isDense) {
      if (key >= minKey && key <= maxKey) {
        val value = array((key - minKey).toInt)
        if (value > 0) {
          return getRow(value, resultRow)
        }
      }
    } else {
      var pos = firstSlot(key)
      while (array(pos + 1) != 0) {
        if (array(pos) == key) {
          return getRow(array(pos + 1), resultRow)
        }
        pos = nextSlot(pos)
      }
    }
    null
  }

  /**
   * Returns an iterator of UnsafeRow for multiple linked values.
   */
  private def valueIter(address: Long, resultRow: UnsafeRow): Iterator[UnsafeRow] = {
    new Iterator[UnsafeRow] {
      var addr = address
      override def hasNext: Boolean = addr != 0
      override def next(): UnsafeRow = {
        val offset = toOffset(addr)
        val size = toSize(addr)
        resultRow.pointTo(page, offset, size)
        addr = Platform.getLong(page, offset + size)
        resultRow
      }
    }
  }

  /**
   * Returns an iterator for all the values for the given key, or null if no value found.
   */
  def get(key: Long, resultRow: UnsafeRow): Iterator[UnsafeRow] = {
    if (isDense) {
      if (key >= minKey && key <= maxKey) {
        val value = array((key - minKey).toInt)
        if (value > 0) {
          return valueIter(value, resultRow)
        }
      }
    } else {
      var pos = firstSlot(key)
      while (array(pos + 1) != 0) {
        if (array(pos) == key) {
          return valueIter(array(pos + 1), resultRow)
        }
        pos = nextSlot(pos)
      }
    }
    null
  }

  /**
   * Builds an iterator on a sparse array.
   */
  def keys(): Iterator[InternalRow] = {
    val row = new GenericInternalRow(1)
    // a) in dense mode the array stores the address
    //  => (k, v) = (minKey + index, array(index))
    // b) in sparse mode the array stores both the key and the address
    //  => (k, v) = (array(index), array(index+1))
    new Iterator[InternalRow] {
      // cursor that indicates the position of the next key which was not read by a next() call
      var pos = 0
      // when we iterate in dense mode we need to jump two positions at a time
      val step = if (isDense) 0 else 1

      override def hasNext: Boolean = {
        // go to the next key if the current key slot is empty
        while (pos + step < array.length) {
          if (array(pos + step) > 0) {
            return true
          }
          pos += step + 1
        }
        false
      }

      override def next(): InternalRow = {
        if (!hasNext) {
          throw new NoSuchElementException("End of the iterator")
        } else {
          // the key is retrieved based on the map mode
          val ret = if (isDense) minKey + pos else array(pos)
          // advance the cursor to the next index
          pos += step + 1
          row.setLong(0, ret)
          row
        }
      }
    }
  }

  /**
   * Appends the key and row into this map.
   */
  def append(key: Long, row: UnsafeRow): Unit = {
    val sizeInBytes = row.getSizeInBytes
    if (sizeInBytes >= (1 << SIZE_BITS)) {
      throw new UnsupportedOperationException("Does not support row that is larger than 256M")
    }

    if (key < minKey) {
      minKey = key
    }
    if (key > maxKey) {
      maxKey = key
    }

    grow(row.getSizeInBytes)

    // copy the bytes of UnsafeRow
    val offset = cursor
    Platform.copyMemory(row.getBaseObject, row.getBaseOffset, page, cursor, row.getSizeInBytes)
    cursor += row.getSizeInBytes
    Platform.putLong(page, cursor, 0)
    cursor += 8
    numValues += 1
    updateIndex(key, toAddress(offset, row.getSizeInBytes))
  }

  /**
   * Update the address in array for given key.
   */
  private def updateIndex(key: Long, address: Long): Unit = {
    var pos = firstSlot(key)
    assert(numKeys < array.length / 2)
    while (array(pos) != key && array(pos + 1) != 0) {
      pos = nextSlot(pos)
    }
    if (array(pos + 1) == 0) {
      // this is the first value for this key, put the address in array.
      array(pos) = key
      array(pos + 1) = address
      numKeys += 1
      if (numKeys * 4 > array.length) {
        // reach half of the capacity
        if (array.length < (1 << 30)) {
          // Cannot allocate an array with 2G elements
          growArray()
        } else if (numKeys > array.length / 2 * 0.75) {
          // The fill ratio should be less than 0.75
          throw new UnsupportedOperationException(
            "Cannot build HashedRelation with more than 1/3 billions unique keys")
        }
      }
    } else {
      // there are some values for this key, put the address in the front of them.
      val pointer = toOffset(address) + toSize(address)
      Platform.putLong(page, pointer, array(pos + 1))
      array(pos + 1) = address
    }
  }

  private def grow(inputRowSize: Int): Unit = {
    // There is 8 bytes for the pointer to next value
    val neededNumWords = (cursor - Platform.LONG_ARRAY_OFFSET + 8 + inputRowSize + 7) / 8
    if (neededNumWords > page.length) {
      if (neededNumWords > (1 << 30)) {
        throw new UnsupportedOperationException(
          "Can not build a HashedRelation that is larger than 8G")
      }
      val newNumWords = math.max(neededNumWords, math.min(page.length * 2, 1 << 30))
      ensureAcquireMemory(newNumWords * 8L)
      val newPage = new Array[Long](newNumWords.toInt)
      Platform.copyMemory(page, Platform.LONG_ARRAY_OFFSET, newPage, Platform.LONG_ARRAY_OFFSET,
        cursor - Platform.LONG_ARRAY_OFFSET)
      val used = page.length
      page = newPage
      freeMemory(used * 8L)
    }
  }

  private def growArray(): Unit = {
    var old_array = array
    val n = array.length
    numKeys = 0
    ensureAcquireMemory(n * 2 * 8L)
    array = new Array[Long](n * 2)
    mask = n * 2 - 2
    var i = 0
    while (i < old_array.length) {
      if (old_array(i + 1) > 0) {
        updateIndex(old_array(i), old_array(i + 1))
      }
      i += 2
    }
    old_array = null  // release the reference to old array
    freeMemory(n * 8L)
  }

  /**
   * Try to turn the map into dense mode, which is faster to probe.
   */
  def optimize(): Unit = {
    val range = maxKey - minKey
    // Convert to dense mode if it does not require more memory or could fit within L1 cache
    // SPARK-16740: Make sure range doesn't overflow if minKey has a large negative value
    if (range >= 0 && (range < array.length || range < 1024)) {
      try {
        ensureAcquireMemory((range + 1) * 8L)
      } catch {
        case e: SparkException =>
          // there is no enough memory to convert
          return
      }
      val denseArray = new Array[Long]((range + 1).toInt)
      var i = 0
      while (i < array.length) {
        if (array(i + 1) > 0) {
          val idx = (array(i) - minKey).toInt
          denseArray(idx) = array(i + 1)
        }
        i += 2
      }
      val old_length = array.length
      array = denseArray
      isDense = true
      freeMemory(old_length * 8L)
    }
  }

  /**
   * Free all the memory acquired by this map.
   */
  def free(): Unit = {
    if (page != null) {
      freeMemory(page.length * 8L)
      page = null
    }
    if (array != null) {
      freeMemory(array.length * 8L)
      array = null
    }
  }

  private def writeLongArray(
      writeBuffer: (Array[Byte], Int, Int) => Unit,
      arr: Array[Long],
      len: Int): Unit = {
    val buffer = new Array[Byte](4 << 10)
    var offset: Long = Platform.LONG_ARRAY_OFFSET
    val end = len * 8L + Platform.LONG_ARRAY_OFFSET
    while (offset < end) {
      val size = Math.min(buffer.length, end - offset)
      Platform.copyMemory(arr, offset, buffer, Platform.BYTE_ARRAY_OFFSET, size)
      writeBuffer(buffer, 0, size.toInt)
      offset += size
    }
  }

  private def write(
      writeBoolean: (Boolean) => Unit,
      writeLong: (Long) => Unit,
      writeBuffer: (Array[Byte], Int, Int) => Unit): Unit = {
    writeBoolean(isDense)
    writeLong(minKey)
    writeLong(maxKey)
    writeLong(numKeys)
    writeLong(numValues)

    writeLong(array.length)
    writeLongArray(writeBuffer, array, array.length)
    val used = ((cursor - Platform.LONG_ARRAY_OFFSET) / 8).toInt
    writeLong(used)
    writeLongArray(writeBuffer, page, used)
  }

  override def writeExternal(output: ObjectOutput): Unit = {
    write(output.writeBoolean, output.writeLong, output.write)
  }

  override def write(kryo: Kryo, out: Output): Unit = {
    write(out.writeBoolean, out.writeLong, out.write)
  }

  private def readLongArray(
      readBuffer: (Array[Byte], Int, Int) => Unit,
      length: Int): Array[Long] = {
    val array = new Array[Long](length)
    val buffer = new Array[Byte](4 << 10)
    var offset: Long = Platform.LONG_ARRAY_OFFSET
    val end = length * 8L + Platform.LONG_ARRAY_OFFSET
    while (offset < end) {
      val size = Math.min(buffer.length, end - offset)
      readBuffer(buffer, 0, size.toInt)
      Platform.copyMemory(buffer, Platform.BYTE_ARRAY_OFFSET, array, offset, size)
      offset += size
    }
    array
  }

  private def read(
      readBoolean: () => Boolean,
      readLong: () => Long,
      readBuffer: (Array[Byte], Int, Int) => Unit): Unit = {
    isDense = readBoolean()
    minKey = readLong()
    maxKey = readLong()
    numKeys = readLong()
    numValues = readLong()

    val length = readLong().toInt
    mask = length - 2
    array = readLongArray(readBuffer, length)
    val pageLength = readLong().toInt
    page = readLongArray(readBuffer, pageLength)
    // Restore cursor variable to make this map able to be serialized again on executors.
    cursor = pageLength * 8 + Platform.LONG_ARRAY_OFFSET
  }

  override def readExternal(in: ObjectInput): Unit = {
    read(() => in.readBoolean(), () => in.readLong(), in.readFully)
  }

  override def read(kryo: Kryo, in: Input): Unit = {
    read(() => in.readBoolean(), () => in.readLong(), in.readBytes)
  }
}

class LongHashedRelation(
    private var nFields: Int,
    private var map: LongToUnsafeRowMap) extends HashedRelation with Externalizable {

  private var resultRow: UnsafeRow = new UnsafeRow(nFields)

  // Needed for serialization (it is public to make Java serialization work)
  def this() = this(0, null)

  override def asReadOnlyCopy(): LongHashedRelation = new LongHashedRelation(nFields, map)

  override def estimatedSize: Long = map.getTotalMemoryConsumption

  override def get(key: InternalRow): Iterator[InternalRow] = {
    if (key.isNullAt(0)) {
      null
    } else {
      get(key.getLong(0))
    }
  }

  override def getValue(key: InternalRow): InternalRow = {
    if (key.isNullAt(0)) {
      null
    } else {
      getValue(key.getLong(0))
    }
  }

  override def get(key: Long): Iterator[InternalRow] = map.get(key, resultRow)

  override def getValue(key: Long): InternalRow = map.getValue(key, resultRow)

  override def keyIsUnique: Boolean = map.keyIsUnique

  override def close(): Unit = {
    map.free()
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeInt(nFields)
    out.writeObject(map)
  }

  override def readExternal(in: ObjectInput): Unit = {
    nFields = in.readInt()
    resultRow = new UnsafeRow(nFields)
    map = in.readObject().asInstanceOf[LongToUnsafeRowMap]
  }

  /**
   * Returns an iterator for keys of InternalRow type.
   */
  override def keys(): Iterator[InternalRow] = map.keys()

  override def getWithKeyIndex(key: InternalRow): Iterator[ValueRowWithKeyIndex] = {
    throw new UnsupportedOperationException
  }

  override def getValueWithKeyIndex(key: InternalRow): ValueRowWithKeyIndex = {
    throw new UnsupportedOperationException
  }

  override def valuesWithKeyIndex(): Iterator[ValueRowWithKeyIndex] = {
    throw new UnsupportedOperationException
  }

  override def maxNumKeysIndex: Int = {
    throw new UnsupportedOperationException
  }
}

/**
 * Create hashed relation with key that is long.
 */
private[joins] object LongHashedRelation {
  def apply(
      input: Iterator[InternalRow],
      key: Seq[Expression],
      sizeEstimate: Int,
      taskMemoryManager: TaskMemoryManager,
      isNullAware: Boolean = false): HashedRelation = {

    val map = new LongToUnsafeRowMap(taskMemoryManager, sizeEstimate)
    val keyGenerator = UnsafeProjection.create(key)

    // Create a mapping of key -> rows
    var numFields = 0
    while (input.hasNext) {
      val unsafeRow = input.next().asInstanceOf[UnsafeRow]
      numFields = unsafeRow.numFields()
      val rowKey = keyGenerator(unsafeRow)
      if (!rowKey.isNullAt(0)) {
        val key = rowKey.getLong(0)
        map.append(key, unsafeRow)
      } else if (isNullAware) {
        map.free()
        return HashedRelationWithAllNullKeys
      }
    }
    map.optimize()
    new LongHashedRelation(numFields, map)
  }
}

/**
 * A special HashedRelation indicating that it's built from a empty input:Iterator[InternalRow].
 * get & getValue will return null just like
 * empty LongHashedRelation or empty UnsafeHashedRelation does.
 */
case object EmptyHashedRelation extends HashedRelation {
  override def get(key: Long): Iterator[InternalRow] = null

  override def get(key: InternalRow): Iterator[InternalRow] = null

  override def getValue(key: Long): InternalRow = null

  override def getValue(key: InternalRow): InternalRow = null

  override def asReadOnlyCopy(): EmptyHashedRelation.type = this

  override def keyIsUnique: Boolean = true

  override def keys(): Iterator[InternalRow] = {
    Iterator.empty
  }

  override def close(): Unit = {}

  override def estimatedSize: Long = 0
}

/**
 * A special HashedRelation indicating that it's built from a non-empty input:Iterator[InternalRow]
 * with all the keys to be null.
 */
case object HashedRelationWithAllNullKeys extends HashedRelation {
  override def get(key: InternalRow): Iterator[InternalRow] = {
    throw new UnsupportedOperationException
  }

  override def getValue(key: InternalRow): InternalRow = {
    throw new UnsupportedOperationException
  }

  override def asReadOnlyCopy(): HashedRelationWithAllNullKeys.type = this

  override def keyIsUnique: Boolean = true

  override def keys(): Iterator[InternalRow] = {
    throw new UnsupportedOperationException
  }

  override def close(): Unit = {}

  override def estimatedSize: Long = 0
}

/** The HashedRelationBroadcastMode requires that rows are broadcasted as a HashedRelation. */
case class HashedRelationBroadcastMode(key: Seq[Expression], isNullAware: Boolean = false)
  extends BroadcastMode {

  override def transform(rows: Array[InternalRow]): HashedRelation = {
    transform(rows.iterator, Some(rows.length))
  }

  override def transform(
      rows: Iterator[InternalRow],
      sizeHint: Option[Long]): HashedRelation = {
    sizeHint match {
      case Some(numRows) =>
        HashedRelation(rows, canonicalized.key, numRows.toInt, isNullAware = isNullAware)
      case None =>
        HashedRelation(rows, canonicalized.key, isNullAware = isNullAware)
    }
  }

  override lazy val canonicalized: HashedRelationBroadcastMode = {
    this.copy(key = key.map(_.canonicalized))
  }
}
