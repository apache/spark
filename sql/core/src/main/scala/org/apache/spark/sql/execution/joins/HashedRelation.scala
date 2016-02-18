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
import java.nio.ByteOrder
import java.util.{HashMap => JavaHashMap}

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.memory.{StaticMemoryManager, TaskMemoryManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkSqlSerializer
import org.apache.spark.sql.execution.local.LocalNode
import org.apache.spark.sql.execution.metric.{LongSQLMetric, SQLMetrics}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.map.BytesToBytesMap
import org.apache.spark.unsafe.memory.MemoryLocation
import org.apache.spark.util.{KnownSizeEstimation, SizeEstimator, Utils}
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

/**
 * A general [[HashedRelation]] backed by a hash map that maps the key into a sequence of values.
 */
private[joins] class GeneralHashedRelation(
    private var hashTable: JavaHashMap[InternalRow, CompactBuffer[InternalRow]])
  extends HashedRelation with Externalizable {

  // Needed for serialization (it is public to make Java serialization work)
  def this() = this(null)

  override def get(key: InternalRow): Seq[InternalRow] = hashTable.get(key)

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
private[joins] class UniqueKeyHashedRelation(
  private var hashTable: JavaHashMap[InternalRow, InternalRow])
  extends UniqueHashedRelation with Externalizable {

  // Needed for serialization (it is public to make Java serialization work)
  def this() = this(null)

  override def getValue(key: InternalRow): InternalRow = hashTable.get(key)

  override def writeExternal(out: ObjectOutput): Unit = {
    writeBytes(out, SparkSqlSerializer.serialize(hashTable))
  }

  override def readExternal(in: ObjectInput): Unit = {
    hashTable = SparkSqlSerializer.deserialize(readBytes(in))
  }
}


private[execution] object HashedRelation {

  def apply(localNode: LocalNode, keyGenerator: Projection): HashedRelation = {
    apply(localNode.asIterator, keyGenerator)
  }

  def apply(
      input: Iterator[InternalRow],
      keyGenerator: Projection,
      sizeEstimate: Int = 64): HashedRelation = {

    if (keyGenerator.isInstanceOf[UnsafeProjection]) {
      return UnsafeHashedRelation(
        input, keyGenerator.asInstanceOf[UnsafeProjection], sizeEstimate)
    }

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
 * A HashedRelation for UnsafeRow, which is backed by HashMap or BytesToBytesMap that maps the key
 * into a sequence of values.
 *
 * When it's created, it uses HashMap. After it's serialized and deserialized, it switch to use
 * BytesToBytesMap for better memory performance (multiple values for the same are stored as a
 * continuous byte array.
 *
 * It's serialized in the following format:
 *  [number of keys]
 *  [size of key] [size of all values in bytes] [key bytes] [bytes for all values]
 *  ...
 *
 * All the values are serialized as following:
 *   [number of fields] [number of bytes] [underlying bytes of UnsafeRow]
 *   ...
 */
private[joins] final class UnsafeHashedRelation(
    private var hashTable: JavaHashMap[UnsafeRow, CompactBuffer[UnsafeRow]])
  extends HashedRelation
  with KnownSizeEstimation
  with Externalizable {

  private[joins] def this() = this(null)  // Needed for serialization

  // Use BytesToBytesMap in executor for better performance (it's created when deserialization)
  // This is used in broadcast joins and distributed mode only
  @transient private[this] var binaryMap: BytesToBytesMap = _

  /**
   * Return the size of the unsafe map on the executors.
   *
   * For broadcast joins, this hashed relation is bigger on the driver because it is
   * represented as a Java hash map there. While serializing the map to the executors,
   * however, we rehash the contents in a binary map to reduce the memory footprint on
   * the executors.
   *
   * For non-broadcast joins or in local mode, return 0.
   */
  override def getMemorySize: Long = {
    if (binaryMap != null) {
      binaryMap.getTotalMemoryConsumption
    } else {
      0
    }
  }

  override def estimatedSize: Long = {
    if (binaryMap != null) {
      binaryMap.getTotalMemoryConsumption
    } else {
      SizeEstimator.estimate(hashTable)
    }
  }

  override def get(key: InternalRow): Seq[InternalRow] = {
    val unsafeKey = key.asInstanceOf[UnsafeRow]

    if (binaryMap != null) {
      // Used in Broadcast join
      val map = binaryMap  // avoid the compiler error
      val loc = new map.Location  // this could be allocated in stack
      binaryMap.safeLookup(unsafeKey.getBaseObject, unsafeKey.getBaseOffset,
        unsafeKey.getSizeInBytes, loc, unsafeKey.hashCode())
      if (loc.isDefined) {
        val buffer = CompactBuffer[UnsafeRow]()

        val base = loc.getValueBase
        var offset = loc.getValueOffset
        val last = offset + loc.getValueLength
        while (offset < last) {
          val numFields = Platform.getInt(base, offset)
          val sizeInBytes = Platform.getInt(base, offset + 4)
          offset += 8

          val row = new UnsafeRow(numFields)
          row.pointTo(base, offset, sizeInBytes)
          buffer += row
          offset += sizeInBytes
        }
        buffer
      } else {
        null
      }

    } else {
      // Use the Java HashMap in local mode or for non-broadcast joins (e.g. ShuffleHashJoin)
      hashTable.get(unsafeKey)
    }
  }

  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    if (binaryMap != null) {
      // This could happen when a cached broadcast object need to be dumped into disk to free memory
      out.writeInt(binaryMap.numElements())

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
        // [key size] [values size] [key bytes] [values bytes]
        out.writeInt(loc.getKeyLength)
        out.writeInt(loc.getValueLength)
        write(loc.getKeyBase, loc.getKeyOffset, loc.getKeyLength)
        write(loc.getValueBase, loc.getValueOffset, loc.getValueLength)
      }

    } else {
      assert(hashTable != null)
      out.writeInt(hashTable.size())

      val iter = hashTable.entrySet().iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        val key = entry.getKey
        val values = entry.getValue

        // write all the values as single byte array
        var totalSize = 0L
        var i = 0
        while (i < values.length) {
          totalSize += values(i).getSizeInBytes + 4 + 4
          i += 1
        }
        assert(totalSize < Integer.MAX_VALUE, "values are too big")

        // [key size] [values size] [key bytes] [values bytes]
        out.writeInt(key.getSizeInBytes)
        out.writeInt(totalSize.toInt)
        out.write(key.getBytes)
        i = 0
        while (i < values.length) {
          // [num of fields] [num of bytes] [row bytes]
          // write the integer in native order, so they can be read by UNSAFE.getInt()
          if (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN) {
            out.writeInt(values(i).numFields())
            out.writeInt(values(i).getSizeInBytes)
          } else {
            out.writeInt(Integer.reverseBytes(values(i).numFields()))
            out.writeInt(Integer.reverseBytes(values(i).getSizeInBytes))
          }
          out.write(values(i).getBytes)
          i += 1
        }
      }
    }
  }

  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    val nKeys = in.readInt()
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
    while (i < nKeys) {
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

      // put it into binary map
      val loc = binaryMap.lookup(keyBuffer, Platform.BYTE_ARRAY_OFFSET, keySize)
      assert(!loc.isDefined, "Duplicated key found!")
      val putSuceeded = loc.putNewKey(
        keyBuffer, Platform.BYTE_ARRAY_OFFSET, keySize,
        valuesBuffer, Platform.BYTE_ARRAY_OFFSET, valuesSize)
      if (!putSuceeded) {
        throw new IOException("Could not allocate memory to grow BytesToBytesMap")
      }
      i += 1
    }
  }
}

private[joins] object UnsafeHashedRelation {

  def apply(
      input: Iterator[InternalRow],
      keyGenerator: UnsafeProjection,
      sizeEstimate: Int): HashedRelation = {

    // Use a Java hash table here because unsafe maps expect fixed size records
    val hashTable = new JavaHashMap[UnsafeRow, CompactBuffer[UnsafeRow]](sizeEstimate)

    // Create a mapping of buildKeys -> rows
    while (input.hasNext) {
      val unsafeRow = input.next().asInstanceOf[UnsafeRow]
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

    // TODO: create UniqueUnsafeRelation
    new UnsafeHashedRelation(hashTable)
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

    // Use a Java hash table here because unsafe maps expect fixed size records
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
        matchList += unsafeRow.copy()
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
