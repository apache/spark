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

package org.apache.spark.storage

import java.io.{Externalizable, IOException, ObjectInput, ObjectOutput}

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * Flags for controlling the storage of an RDD. Each StorageLevel records whether to use memory,
 * or Tachyon, whether to drop the RDD to disk if it falls out of memory or Tachyon , whether to
 * keep the data in memory in a serialized format, and whether to replicate the RDD partitions on
 * multiple nodes.
 *
 * The [[org.apache.spark.storage.StorageLevel$]] singleton object contains some static constants
 * for commonly useful storage levels. To create your own storage level object, use the
 * factory method of the singleton object (`StorageLevel(...)`).
 */
@DeveloperApi
class StorageLevel private(
    private var useDisk_ : Boolean,
    private var useMemory_ : Boolean,
    private var useOffHeap_ : Boolean,
    private var deserialized_ : Boolean,
    private var replication_ : Int = 1)
  extends Externalizable {

  // TODO: Also add fields for caching priority, dataset ID, and flushing.
  private def this(flags: Int, replication: Int) {
    this((flags & 8) != 0, (flags & 4) != 0, (flags & 2) != 0, (flags & 1) != 0, replication)
  }

  def this() = this(false, true, false, false)  // For deserialization

  def useDisk = useDisk_
  def useMemory = useMemory_
  def useOffHeap = useOffHeap_
  def deserialized = deserialized_
  def replication = replication_

  assert(replication < 40, "Replication restricted to be less than 40 for calculating hashcodes")

  if (useOffHeap) {
    require(!useDisk, "Off-heap storage level does not support using disk")
    require(!useMemory, "Off-heap storage level does not support using heap memory")
    require(!deserialized, "Off-heap storage level does not support deserialized storage")
    require(replication == 1, "Off-heap storage level does not support multiple replication")
  }

  override def clone(): StorageLevel = new StorageLevel(
    this.useDisk, this.useMemory, this.useOffHeap, this.deserialized, this.replication)

  override def equals(other: Any): Boolean = other match {
    case s: StorageLevel =>
      s.useDisk == useDisk &&
      s.useMemory == useMemory &&
      s.useOffHeap == useOffHeap &&
      s.deserialized == deserialized &&
      s.replication == replication
    case _ =>
      false
  }

  def isValid = ((useMemory || useDisk || useOffHeap) && (replication > 0))

  def toInt: Int = {
    var ret = 0
    if (useDisk_) {
      ret |= 8
    }
    if (useMemory_) {
      ret |= 4
    }
    if (useOffHeap_) {
      ret |= 2
    }
    if (deserialized_) {
      ret |= 1
    }
    ret
  }

  override def writeExternal(out: ObjectOutput) {
    out.writeByte(toInt)
    out.writeByte(replication_)
  }

  override def readExternal(in: ObjectInput) {
    val flags = in.readByte()
    useDisk_ = (flags & 8) != 0
    useMemory_ = (flags & 4) != 0
    useOffHeap_ = (flags & 2) != 0
    deserialized_ = (flags & 1) != 0
    replication_ = in.readByte()
  }

  @throws(classOf[IOException])
  private def readResolve(): Object = StorageLevel.getCachedStorageLevel(this)

  override def toString: String = "StorageLevel(%b, %b, %b, %b, %d)".format(
    useDisk, useMemory, useOffHeap, deserialized, replication)

  override def hashCode(): Int = toInt * 41 + replication
  def description : String = {
    var result = ""
    result += (if (useDisk) "Disk " else "")
    result += (if (useMemory) "Memory " else "")
    result += (if (useOffHeap) "Tachyon " else "")
    result += (if (deserialized) "Deserialized " else "Serialized ")
    result += "%sx Replicated".format(replication)
    result
  }
}


/**
 * Various [[org.apache.spark.storage.StorageLevel]] defined and utility functions for creating
 * new storage levels.
 */
object StorageLevel {
  val NONE = new StorageLevel(false, false, false, false)
  val DISK_ONLY = new StorageLevel(true, false, false, false)
  val DISK_ONLY_2 = new StorageLevel(true, false, false, false, 2)
  val MEMORY_ONLY = new StorageLevel(false, true, false, true)
  val MEMORY_ONLY_2 = new StorageLevel(false, true, false, true, 2)
  val MEMORY_ONLY_SER = new StorageLevel(false, true, false, false)
  val MEMORY_ONLY_SER_2 = new StorageLevel(false, true, false, false, 2)
  val MEMORY_AND_DISK = new StorageLevel(true, true, false, true)
  val MEMORY_AND_DISK_2 = new StorageLevel(true, true, false, true, 2)
  val MEMORY_AND_DISK_SER = new StorageLevel(true, true, false, false)
  val MEMORY_AND_DISK_SER_2 = new StorageLevel(true, true, false, false, 2)
  val OFF_HEAP = new StorageLevel(false, false, true, false)

  /**
   * :: DeveloperApi ::
   * Create a new StorageLevel object without setting useOffHeap.
   */
  @DeveloperApi
  def apply(useDisk: Boolean, useMemory: Boolean, useOffHeap: Boolean,
    deserialized: Boolean, replication: Int) = getCachedStorageLevel(
      new StorageLevel(useDisk, useMemory, useOffHeap, deserialized, replication))

  /**
   * :: DeveloperApi ::
   * Create a new StorageLevel object.
   */
  @DeveloperApi
  def apply(useDisk: Boolean, useMemory: Boolean,
    deserialized: Boolean, replication: Int = 1) = getCachedStorageLevel(
      new StorageLevel(useDisk, useMemory, false, deserialized, replication))

  /**
   * :: DeveloperApi ::
   * Create a new StorageLevel object from its integer representation.
   */
  @DeveloperApi
  def apply(flags: Int, replication: Int): StorageLevel =
    getCachedStorageLevel(new StorageLevel(flags, replication))

  /**
   * :: DeveloperApi ::
   * Read StorageLevel object from ObjectInput stream.
   */
  @DeveloperApi
  def apply(in: ObjectInput): StorageLevel = {
    val obj = new StorageLevel()
    obj.readExternal(in)
    getCachedStorageLevel(obj)
  }

  private[spark]
  val storageLevelCache = new java.util.concurrent.ConcurrentHashMap[StorageLevel, StorageLevel]()

  private[spark] def getCachedStorageLevel(level: StorageLevel): StorageLevel = {
    storageLevelCache.putIfAbsent(level, level)
    storageLevelCache.get(level)
  }
}
