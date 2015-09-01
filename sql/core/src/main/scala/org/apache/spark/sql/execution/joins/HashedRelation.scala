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

import org.apache.spark.shuffle.ShuffleMemoryManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkSqlSerializer
import org.apache.spark.sql.execution.metric.LongSQLMetric
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.map.BytesToBytesMap
import org.apache.spark.unsafe.memory.{MemoryLocation, ExecutorMemoryManager, MemoryAllocator, TaskMemoryManager}
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.CompactBuffer
import org.apache.spark.{SparkConf, SparkEnv}


/**
 * Interface for a hashed relation by some key. Use [[HashedRelation.apply]] to create a concrete
 * object.
 */
private[joins] sealed trait HashedRelation {
  def get(key: InternalRow): Seq[InternalRow]

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
private[joins]
final class UniqueKeyHashedRelation(private var hashTable: JavaHashMap[InternalRow, InternalRow])
  extends HashedRelation with Externalizable {

  // Needed for serialization (it is public to make Java serialization work)
  def this() = this(null)

  override def get(key: InternalRow): Seq[InternalRow] = {
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
      numInputRows: LongSQLMetric,
      keyGenerator: Projection,
      sizeEstimate: Int = 64): HashedRelation = {

    if (keyGenerator.isInstanceOf[UnsafeProjection]) {
      return UnsafeHashedRelation(
        input, numInputRows, keyGenerator.asInstanceOf[UnsafeProjection], sizeEstimate)
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
      numInputRows += 1
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
  extends HashedRelation with Externalizable {

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
  def getUnsafeSize: Long = {
    if (binaryMap != null) {
      binaryMap.getTotalMemoryConsumption
    } else {
      0
    }
  }

  override def get(key: InternalRow): Seq[InternalRow] = {
    val unsafeKey = key.asInstanceOf[UnsafeRow]

    if (binaryMap != null) {
      // Used in Broadcast join
      val map = binaryMap  // avoid the compiler error
      val loc = new map.Location  // this could be allocated in stack
      binaryMap.safeLookup(unsafeKey.getBaseObject, unsafeKey.getBaseOffset,
        unsafeKey.getSizeInBytes, loc)
      if (loc.isDefined) {
        val buffer = CompactBuffer[UnsafeRow]()

        val base = loc.getValueAddress.getBaseObject
        var offset = loc.getValueAddress.getBaseOffset
        val last = loc.getValueAddress.getBaseOffset + loc.getValueLength
        while (offset < last) {
          val numFields = Platform.getInt(base, offset)
          val sizeInBytes = Platform.getInt(base, offset + 4)
          offset += 8

          val row = new UnsafeRow
          row.pointTo(base, offset, numFields, sizeInBytes)
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
      def write(addr: MemoryLocation, length: Int): Unit = {
        if (buffer.length < length) {
          buffer = new Array[Byte](length)
        }
        Platform.copyMemory(addr.getBaseObject, addr.getBaseOffset,
          buffer, Platform.BYTE_ARRAY_OFFSET, length)
        out.write(buffer, 0, length)
      }

      val iter = binaryMap.iterator()
      while (iter.hasNext) {
        val loc = iter.next()
        // [key size] [values size] [key bytes] [values bytes]
        out.writeInt(loc.getKeyLength)
        out.writeInt(loc.getValueLength)
        write(loc.getKeyAddress, loc.getKeyLength)
        write(loc.getValueAddress, loc.getValueLength)
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
    val taskMemoryManager = new TaskMemoryManager(new ExecutorMemoryManager(MemoryAllocator.HEAP))

    val pageSizeBytes = Option(SparkEnv.get).map(_.shuffleMemoryManager.pageSizeBytes)
      .getOrElse(new SparkConf().getSizeAsBytes("spark.buffer.pageSize", "16m"))

    // Dummy shuffle memory manager which always grants all memory allocation requests.
    // We use this because it doesn't make sense count shared broadcast variables' memory usage
    // towards individual tasks' quotas. In the future, we should devise a better way of handling
    // this.
    val shuffleMemoryManager =
      ShuffleMemoryManager.create(maxMemory = Long.MaxValue, pageSizeBytes = pageSizeBytes)

    binaryMap = new BytesToBytesMap(
      taskMemoryManager,
      shuffleMemoryManager,
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
      numInputRows: LongSQLMetric,
      keyGenerator: UnsafeProjection,
      sizeEstimate: Int): HashedRelation = {

    // Use a Java hash table here because unsafe maps expect fixed size records
    val hashTable = new JavaHashMap[UnsafeRow, CompactBuffer[UnsafeRow]](sizeEstimate)

    // Create a mapping of buildKeys -> rows
    while (input.hasNext) {
      val unsafeRow = input.next().asInstanceOf[UnsafeRow]
      numInputRows += 1
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
