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

package org.apache.spark.io

import java.io.{RandomAccessFile, DataInput, InputStream, OutputStream}
import java.nio.{ByteBuffer, BufferUnderflowException, BufferOverflowException}
import java.nio.channels.{WritableByteChannel, ReadableByteChannel}

import org.apache.spark.util.collection.ChainedBuffer

import scala.collection.mutable.{ArrayBuffer, HashSet}

import org.apache.spark.Logging
import org.apache.spark.storage.{FileSegment, BlockManager}



trait LargeByteBuffer {
//  def position(): Long
//
//  def limit(): Long

  def capacity(): Long

  def get(): Byte //needed for ByteBufferInputStream

  def get(dst: Array[Byte], offset: Int, length: Int): Unit // for ByteBufferInputStream

  def position(position: Long): Unit //for ByteBufferInputStream

  def position(): Long //for ByteBufferInputStream

  /** doesn't copy data, just copies references & offsets */
  def duplicate(): LargeByteBuffer

  def put(bytes: LargeByteBuffer): Unit

  //also need whatever is necessary for ByteArrayOutputStream for BlockManager#dataSerialize


  //TODO checks on limit semantics

  /**
   * Sets this buffer's limit. If the position is larger than the new limit then it is set to the
   * new limit. If the mark is defined and larger than the new limit then it is discarded.
   */
  def limit(newLimit: Long): Unit

  /**
   * return this buffer's limit
   * @return
   */
  def limit(): Long

//
//  def skip(skipBy: Long): Unit
//
//  def position(newPosition: Long): Unit
//
//  /**
//   * Clears this buffer.  The position is set to zero, the limit is set to
//   * the capacity, and the mark is discarded.
//   *
//   * <p> Invoke this method before using a sequence of channel-read or
//   * <i>put</i> operations to fill this buffer.
//   *
//   * <p> This method does not actually erase the data in the buffer, but it
//   * is named as if it did because it will most often be used in situations
//   * in which that might as well be the case. </p>
//   */
//  def clear(): Unit
//
//  /**
//   * Flips this buffer.  The limit is set to the current position and then
//   * the position is set to zero.  If the mark is defined then it is
//   * discarded.
//   *
//   * <p> After a sequence of channel-read or <i>put</i> operations, invoke
//   * this method to prepare for a sequence of channel-write or relative
//   * <i>get</i> operations.
//   */
//  def flip(): Unit

  /**
   * Rewinds this buffer.  The position is set to zero and the mark is
   * discarded.
   *
   * <p> Invoke this method before a sequence of channel-write or <i>get</i>
   * operations, assuming that the limit has already been set
   * appropriately.
   */
  def rewind(): Unit

  /**
   * Returns the number of elements between the current position and the
   * limit. </p>
   *
   * @return  The number of elements remaining in this buffer
   */
  def remaining(): Long
}

class ChainedLargeByteBuffer(private[io] val underlying: ChainedBuffer) extends LargeByteBuffer {

  def capacity = underlying.capacity

  var _pos = 0l

  def get(dst: Array[Byte],offset: Int,length: Int): Unit = {
    underlying.read(_pos, dst, offset, length)
    _pos += length
  }

  def get(): Byte = {
    val b = underlying.read(_pos)
    _pos += 1
    b
  }

  def put(bytes: LargeByteBuffer): Unit = {
    ???
  }

  def position: Long = _pos
  def position(position: Long): Unit = {
    _pos = position
  }
  def remaining(): Long = {
    underlying.size - position
  }

  def duplicate(): ChainedLargeByteBuffer = {
    new ChainedLargeByteBuffer(underlying)
  }

  def rewind(): Unit = {
    _pos = 0
  }

  def limit(): Long = {
    capacity
  }

  def limit(newLimit: Long): Unit = {
    ???
  }
}

class WrappedLargeByteBuffer(private val underlying: ByteBuffer) extends LargeByteBuffer {
  def capacity = underlying.capacity

  def get(dst: Array[Byte], offset: Int, length: Int): Unit = {
    underlying.get(dst, offset, length)
  }

  def get(): Byte = {
    underlying.get()
  }

  def position: Long = underlying.position
  def position(position: Long): Unit = {
    //XXX check range?
    underlying.position(position.toInt)
  }
  def remaining(): Long = {
    underlying.remaining()
  }

  def duplicate(): WrappedLargeByteBuffer = {
    new WrappedLargeByteBuffer(underlying.duplicate())
  }

  def rewind(): Unit = {
    underlying.duplicate()
  }

  def limit(): Long = {
    underlying.limit()
  }

  def limit(newLimit: Long) = {
    //XXX check range?
    underlying.limit(newLimit.toInt)
  }

}

object LargeByteBuffer {
  def allocateOnHeap(size: Long, maxChunk: Int): LargeByteBuffer = {
    val buffer = ChainedBuffer.withInitialSize(maxChunk, size)
    new ChainedLargeByteBuffer(buffer)
  }
}


//
///**
// * This is a variant of ByteBuffer to be used internally in spark, which is not limited to 2G
// * which ByteBuffers are limited to.
// * Externally, it exposes all the api which java.nio.ByteBuffer exposes.
// * Internally, it maintains a sequence of Containers which manage the ByteBuffer data.
// * Not all the data might be loaded into memory  (like disk or tachyon data) - so actual
// * memory footprint - heap and vm could be much lower than capacity.
// *
// * TODO: Currently we are slightly fast and loose in terms of concurrent modifications to this
// * buffer, maybe revisit this later ? Note: this is not much different from earlier though !
// *
// * TODO: Explore if (at all) we can leverage zero copy transfers. The issue (currently) is that this
// * will require the file to be kept open (repeatedly opening/closing file is not good
// * for each transfer) and this has an impact on ulimit. Not to mention writing of mmap'ed buffer is
// * pretty quick (it is the first failover in case direct transfer is not possible in file zero copy)
// *
// * TODO: After redesign to containers, we got rid of parent containers to free - the side effect is
// * that if there are direct ByteBuffers, we are not handling explicit cleanup of those in some
// * cases (when we duplicate/slice them). Currently spark does not need this, but might in future
// * so relook at it later.
// */
//// We should make this constructor private: but for now,
//// leaving it public since TachyonStore needs it
//class LargeByteBuffer private[spark](private val inputContainers: ArrayBuffer[ByteBufferContainer],
//    private val needDuplicate: Boolean, val ephemeralDiskBacked: Boolean) extends Logging {
//
//  // TODO: TEMP code: to flush out potential resource leaks. REMOVE ME
//  private val allocateLocationThrowable: Throwable = {
//    if (inputContainers.exists(c => c.requireRelease() || c.requireFree())) {
//      new Throwable("blockId = " + BlockManager.getLookupBlockId)
//    } else {
//      null
//    }
//  }
//  private var disposeLocationThrowable: Throwable = null
//
//  @volatile private var allowCleanerOverride = true
//  @volatile private var cleaner: BufferCleaner = new BufferCleaner {
//    override def doClean(buffer: LargeByteBuffer) = {
//      assert (LargeByteBuffer.this == buffer)
//      doDispose(needRelease = false)
//    }
//  }
//
//  // should not be empty
//  assert (null != inputContainers && ! inputContainers.isEmpty)
//  // should not have any null's
//  assert (inputContainers.find(_ == null).isEmpty)
//
//  // println("Num containers = " + inputContainers.size)
//
//  // Position, limit and capacity relevant over the engire LargeByteBuffer
//  @volatile private var globalPosition = 0L
//  @volatile private var globalLimit = 0L
//  @volatile private var currentContainerIndex = 0
//
//  // The buffers in which the actual data is held.
//  private var containers: Array[ByteBufferContainer] = null
//
//  // aggregate capacities of the individual buffers.
//  // bufferPositionStart(0) will be capacity of 1st buffer, bufferPositionStart(1) will be
//  // sum of capacity of 0th and 1st block buffer
//  private var bufferPositionStart: Array[Long] = null
//
//  // Contains the indices of a containers which requires release before subsequent invocation of
//  // read/write should be serviced. This is required since current read/write might have moved the
//  // position but since we are returning bytebuffers which depend on the validity of the existing
//  // bytebuffer, we cant release them yet.
//  private var needReleaseIndices = new HashSet[Int]()
//
//  private val readable = ! inputContainers.exists(! _.isReadable)
//  private val writable = ! inputContainers.exists(! _.isWritable)
//
//
//  // initialize
//  @volatile private var globalCapacity = {
//
//    // Ensure that there are no empty buffers : messes up with our code : unless it
//    // is a single buffer (for empty buffer for marker case)
//    assert (inputContainers.find(0 == _.capacity()).isEmpty || 1 == inputContainers.length)
//
//    containers = {
//      if (needDuplicate) inputContainers.map(_.duplicate()).toArray else inputContainers.toArray
//    }
//    containers.foreach(_.validate())
//
//    def initializeBufferPositionStart(arr: Array[ByteBufferContainer]) {
//      val buff = new ArrayBuffer[Long](arr.length + 1)
//      buff += 0L
//
//      buff ++= arr.map(_.capacity().asInstanceOf[Long]).scanLeft(0L)(_ + _).slice(1, arr.length + 1)
//      assert (buff.length == arr.length + 1)
//      bufferPositionStart = buff.toArray
//    }
//
//    initializeBufferPositionStart(containers)
//
//    // remove references from inputBuffers
//    inputContainers.clear()
//
//    globalLimit = bufferPositionStart(containers.length)
//    globalPosition = 0L
//    currentContainerIndex = 0
//
//    assert (globalLimit == containers.map(_.capacity().asInstanceOf[Long]).sum)
//
//    globalLimit
//  }
//
//  final def position(): Long = globalPosition
//
//  final def limit(): Long = globalLimit
//
//  final def capacity(): Long = globalCapacity
//
//  final def limit(newLimit: Long) {
//    if ((newLimit > capacity()) || (newLimit < 0)) {
//      throw new IllegalArgumentException("newLimit = " + newLimit + ", capacity = " + capacity())
//    }
//
//    globalLimit = newLimit
//    if (position() > newLimit) position(newLimit)
//  }
//
//  def skip(skipBy: Long) = position(position() + skipBy)
//
//  private def releasePendingContainers() {
//    if (! needReleaseIndices.isEmpty) {
//      val iter = needReleaseIndices.iterator
//      while (iter.hasNext) {
//        val index = iter.next()
//        assert (index >= 0 && index < containers.length)
//        // It is possible to move from one container to next before the previous
//        // container was acquired. For example, get forcing move to next container
//        // since current was exhausted immediatelly followed by a position()
//        // so the container we moved to was never acquired.
//
//        // assert (containers(index).isAcquired)
//        // will this always be satisfied ?
//        // assert (index != currentContainerIndex)
//        if (containers(index).isAcquired) containers(index).release()
//      }
//      needReleaseIndices.clear()
//    }
//  }
//
//  private def toNewContainer(newIndex: Int) {
//    if (newIndex != currentContainerIndex && currentContainerIndex < containers.length) {
//
//      assert (currentContainerIndex >= 0)
//      needReleaseIndices += currentContainerIndex
//    }
//    currentContainerIndex = newIndex
//  }
//
//  // expensive method, sigh ... optimize it later ?
//  final def position(newPosition: Long) {
//
//    if ((newPosition > globalLimit) || (newPosition < 0)) throw new IllegalArgumentException()
//
//    if (currentContainerIndex < bufferPositionStart.length - 1 &&
//        newPosition >= bufferPositionStart(currentContainerIndex) &&
//        newPosition < bufferPositionStart(currentContainerIndex + 1)) {
//      // Same buffer - easy method ...
//      globalPosition = newPosition
//      // Changed position - free previously returned buffers.
//      releasePendingContainers()
//      return
//    }
//
//    // Find appropriate currentContainerIndex
//    // Since bufferPositionStart is sorted, can be replaced with binary search if required.
//    // For now, not in the perf critical path since buffers size is very low typically.
//    var index = 0
//    val cLen = containers.length
//    while (index < cLen) {
//      if (newPosition >= bufferPositionStart(index) &&
//        newPosition < bufferPositionStart(index + 1)) {
//        globalPosition = newPosition
//        toNewContainer(index)
//        // Changed position - free earlier and previously returned buffers.
//        releasePendingContainers()
//        return
//      }
//      index += 1
//    }
//
//    if (newPosition == globalLimit && newPosition == bufferPositionStart(cLen)) {
//      // boundary.
//      globalPosition = newPosition
//      toNewContainer(cLen)
//      // Changed position - free earlier and previously returned buffers.
//      releasePendingContainers()
//      return
//    }
//
//    assert (assertion = false, "Unexpected to come here .... newPosition = " + newPosition +
//      ", bufferPositionStart = " + bufferPositionStart.mkString("[", ", ", "]"))
//  }
//
//
//  /**
//   * Clears this buffer.  The position is set to zero, the limit is set to
//   * the capacity, and the mark is discarded.
//   *
//   * <p> Invoke this method before using a sequence of channel-read or
//   * <i>put</i> operations to fill this buffer.
//   *
//   * <p> This method does not actually erase the data in the buffer, but it
//   * is named as if it did because it will most often be used in situations
//   * in which that might as well be the case. </p>
//   */
//  final def clear() {
//    // if (0 == globalCapacity) return
//
//    needReleaseIndices += 0
//    globalPosition = 0L
//    toNewContainer(0)
//    globalLimit = globalCapacity
//
//    // Now free all pending containers
//    releasePendingContainers()
//  }
//
//  /**
//   * Flips this buffer.  The limit is set to the current position and then
//   * the position is set to zero.  If the mark is defined then it is
//   * discarded.
//   *
//   * <p> After a sequence of channel-read or <i>put</i> operations, invoke
//   * this method to prepare for a sequence of channel-write or relative
//   * <i>get</i> operations.
//   */
//  final def flip() {
//    needReleaseIndices += 0
//    globalLimit = globalPosition
//    globalPosition = 0L
//    toNewContainer(0)
//
//    // Now free all pending containers
//    releasePendingContainers()
//  }
//
//  /**
//   * Rewinds this buffer.  The position is set to zero and the mark is
//   * discarded.
//   *
//   * <p> Invoke this method before a sequence of channel-write or <i>get</i>
//   * operations, assuming that the limit has already been set
//   * appropriately.
//   */
//  final def rewind() {
//    needReleaseIndices += 0
//    globalPosition = 0L
//    toNewContainer(0)
//
//    // Now free all pending containers
//    releasePendingContainers()
//  }
//
//  /**
//   * Returns the number of elements between the current position and the
//   * limit. </p>
//   *
//   * @return  The number of elements remaining in this buffer
//   */
//  final def remaining(): Long = {
//    globalLimit - globalPosition
//  }
//
//  /**
//   * Tells whether there are any elements between the current position and
//   * the limit. </p>
//   *
//   * @return  <tt>true</tt> if, and only if, there is at least one element
//   *          remaining in this buffer
//   */
//  final def hasRemaining() = {
//    globalPosition < globalLimit
//  }
//
//  // private def currentBuffer(): ByteBuffer = buffers(currentContainerIndex)
//
//  // number of bytes remaining in currently active underlying buffer
//  private def currentRemaining(): Int = {
//    if (hasRemaining()) {
//      // validate currentContainerIndex is valid
//      assert (globalPosition >= bufferPositionStart(currentContainerIndex) &&
//        globalPosition < bufferPositionStart(currentContainerIndex + 1),
//        "globalPosition = " + globalPosition +
//          ", currentContainerIndex = " + currentContainerIndex +
//        ", bufferPositionStart = " + bufferPositionStart.mkString("[", ", ", " ]"))
//
//      currentRemaining0(currentContainerIndex)
//    } else 0
//  }
//
//  // Without any validation : required when we are bumping the index (when validation will fail) ...
//  private def currentRemaining0(which: Int): Int = {
//    // currentBuffer().remaining()
//    math.max(0, math.min(bufferPositionStart(which + 1),
//      globalLimit) - globalPosition).asInstanceOf[Int]
//  }
//
//  // Set the approppriate position/limit for the current underlying buffer to mirror our
//  // the LargeByteBuffer's state.
//  private def fetchCurrentBuffer(): ByteBuffer = {
//    releasePendingContainers()
//
//    assert (currentContainerIndex < containers.length)
//
//    val container = containers(currentContainerIndex)
//    if (! container.isAcquired) {
//      container.acquire()
//    }
//
//    assert (container.isAcquired)
//    if (LargeByteBuffer.enableExpensiveAssert) {
//      assert (! containers.exists( b => (b ne container) && b.isAcquired))
//    }
//
//    assert (currentContainerIndex < bufferPositionStart.length &&
//      globalPosition < bufferPositionStart(currentContainerIndex + 1),
//      "currentContainerIndex = " + currentContainerIndex + ", bufferPositionStart = " +
//        bufferPositionStart.mkString("[", ", ", "]") + ", this = " + this)
//
//    val buffPosition = (globalPosition - bufferPositionStart(currentContainerIndex)).
//      asInstanceOf[Int]
//
//    val buffer = container.getByteBuffer
//    buffer.position(buffPosition)
//    val diff = buffer.capacity - buffPosition
//    val left = remaining()
//    if (diff <= left) {
//      buffer.limit(buffer.capacity())
//    } else {
//      // Can happen if limit() was called.
//      buffer.limit(buffPosition + left.asInstanceOf[Int])
//    }
//
//    buffer
//  }
//
//  // To be used ONLY to test in suites.
//  private[spark] def fetchCurrentBufferForTesting(): ByteBuffer = {
//    if ("1" != System.getProperty("SPARK_TESTING")) {
//      throw new IllegalStateException("This method is to be used ONLY within spark test suites")
//    }
//
//    fetchCurrentBuffer()
//  }
//
//  // Expects that the invoker has ensured that this can be safely invoked.
//  // That is, it wont be invoked when the loop wont terminate.
//  private def toNonEmptyBuffer() {
//
//    if (! hasRemaining()) {
//      var newIndex = currentContainerIndex
//      // Ensure we are in the right block or not.
//      while (newIndex < containers.length && globalPosition >= bufferPositionStart(newIndex + 1)) {
//        newIndex += 1
//      }
//      toNewContainer(newIndex)
//      // Do not do this - since we might not yet have consumed the buffer which caused EOF right now
//      /*
//      // Add last one also, and release it too - since we are at the end of the buffer with nothing
//      // more pending.
//      if (newIndex >= 0 && currentContainerIndex < containers.length) {
//        needReleaseIndices += newIndex
//      }
//      */
//      assert (currentContainerIndex >= 0)
//      // releasePendingContainers()
//      return
//    }
//
//    var index = currentContainerIndex
//    while (0 == currentRemaining0(index) && index < containers.length) {
//      index += 1
//    }
//    assert (currentContainerIndex < containers.length)
//    toNewContainer(index)
//    assert (0 != currentRemaining())
//  }
//
//  private def assertPreconditions(containerIndex: Int) {
//    assert (globalPosition >= bufferPositionStart(containerIndex),
//      "globalPosition = " + globalPosition + ", containerIndex = " + containerIndex +
//        ", bufferPositionStart = " + bufferPositionStart.mkString("[", ", ", " ]"))
//    assert (globalPosition < bufferPositionStart(containerIndex + 1),
//      "globalPosition = " + globalPosition + ", containerIndex = " + containerIndex +
//        ", bufferPositionStart = " + bufferPositionStart.mkString("[", ", ", " ]"))
//
//    assert (globalLimit <= globalCapacity)
//    assert (containerIndex < containers.length)
//  }
//
//
//  /**
//   * Attempts to return a ByteBuffer of the requested size.
//   * It is possible to return a buffer of size smaller than requested
//   * even though hasRemaining == true
//   *
//   * On return, position would have been moved 'ahead' by the size of the buffer returned :
//   * that is, we treat that the returned buffer has been already 'read' from this LargeByteBuffer
//   *
//   *
//   * This is used to primarily retrieve content of this buffer to expose via ByteBuffer
//   * to some other api which is deemed too cumbersome to move to LargeByteBuffer (like the
//   * chunked sending of contents via ConnectionManager) Note that the lifecycle of the ByteBuffer
//   * returned is inherently tied to the state of this LargeByteBuffer. For example,if the underlying
//   * container is a disk backed container, and we make subsequent calls to get(), the returned
//   * ByteBuffer can be dispose'ed off
//   *
//   * @param maxChunkSize Max size of the ByteBuffer to retrieve.
//   * @return
//   */
//
//  private def fetchBufferOfSize(maxChunkSize: Int): ByteBuffer = {
//    fetchBufferOfSizeImpl(maxChunkSize, canReleaseContainers = true)
//  }
//
//  private def fetchBufferOfSizeImpl(maxChunkSize: Int,
//      canReleaseContainers: Boolean): ByteBuffer = {
//    if (canReleaseContainers) releasePendingContainers()
//    assert (maxChunkSize > 0)
//
//    // not checking for degenerate case of maxChunkSize == 0
//    if (globalPosition >= globalLimit) {
//      // throw exception
//      throw new BufferUnderflowException()
//    }
//
//    // Check preconditions : disable these later, since they might be expensive to
//    // evaluate for every IO op
//    assertPreconditions(currentContainerIndex)
//
//    val currentBufferRemaining = currentRemaining()
//
//    assert (currentBufferRemaining > 0)
//
//    val size = math.min(currentBufferRemaining, maxChunkSize)
//
//    val newBuffer = if (currentBufferRemaining > maxChunkSize) {
//      val currentBuffer = fetchCurrentBuffer()
//      val buff = ByteBufferContainer.createSlice(currentBuffer,
//        currentBuffer.position(), maxChunkSize)
//      assert (buff.remaining() == maxChunkSize)
//      buff
//    } else {
//      val currentBuffer = fetchCurrentBuffer()
//      val buff = currentBuffer.slice()
//      assert (buff.remaining() == currentBufferRemaining)
//      buff
//    }
//
//    assert (size == newBuffer.remaining())
//    assert (0 == newBuffer.position())
//    assert (size == newBuffer.limit())
//    assert (newBuffer.capacity() == newBuffer.limit())
//
//    globalPosition += newBuffer.remaining
//    toNonEmptyBuffer()
//
//    newBuffer
//  }
//
//  // Can we service the read/write from the currently active (underlying) bytebuffer or not.
//  // For almost all cases, this will return true allowing us to optimize away the more expensive
//  // computations.
//  private def localReadWritePossible(size: Int) =
//    size >= 0 && globalPosition + size <= bufferPositionStart(currentContainerIndex + 1)
//
//
//  def getLong(): Long = {
//    assert (readable)
//    releasePendingContainers()
//
//    if (remaining() < 8) throw new BufferUnderflowException
//
//    if (localReadWritePossible(8)) {
//      val buff = fetchCurrentBuffer()
//      assert (buff.remaining() >= 8)
//      val retval = buff.getLong
//      globalPosition += 8
//      toNonEmptyBuffer()
//      return retval
//    }
//
//    val buff = readFully(8)
//    buff.getLong
//  }
//
//  def getInt(): Int = {
//    assert (readable)
//    releasePendingContainers()
//
//    if (remaining() < 4) throw new BufferUnderflowException
//
//    if (localReadWritePossible(4)) {
//      val buff = fetchCurrentBuffer()
//      assert (buff.remaining() >= 4)
//      val retval = buff.getInt
//      globalPosition += 4
//      toNonEmptyBuffer()
//      return retval
//    }
//
//    val buff = readFully(4)
//    buff.getInt
// }
//
//  def getChar(): Char = {
//    assert (readable)
//    releasePendingContainers()
//
//    if (remaining() < 2) throw new BufferUnderflowException
//
//    if (localReadWritePossible(2)) {
//      val buff = fetchCurrentBuffer()
//      assert (buff.remaining() >= 2)
//      val retval = buff.getChar
//      globalPosition += 2
//      toNonEmptyBuffer()
//      return retval
//    }
//
//    // if slice is becoming too expensive, revisit this ...
//    val buff = readFully(2)
//    buff.getChar
//  }
//
//  def get(): Byte = {
//    assert (readable)
//    releasePendingContainers()
//
//    if (! hasRemaining()) throw new BufferUnderflowException
//
//    // If we have remaining bytes, previous invocations MUST have ensured that we are at
//    // a buffer which has data to be read.
//    assert (localReadWritePossible(1))
//
//    val buff = fetchCurrentBuffer()
//    assert (buff.remaining() >= 1, "buff.remaining = " + buff.remaining())
//    val retval = buff.get()
//    globalPosition += 1
//    toNonEmptyBuffer()
//
//    retval
//  }
//
//  def get(arr: Array[Byte], offset: Int, size: Int): Int = {
//    assert (readable)
//    releasePendingContainers()
//
//    LargeByteBuffer.checkOffsets(arr, offset, size)
//
//    // kyro depends on this it seems ?
//    // assert (size > 0)
//    if (0 == size) return 0
//
//    if (! hasRemaining()) return -1
//
//    if (localReadWritePossible(size)) {
//      val buff = fetchCurrentBuffer()
//      assert (buff.remaining() >= size)
//      buff.get(arr, offset, size)
//      globalPosition += size
//      toNonEmptyBuffer()
//      return size
//    }
//
//    var remainingSize = math.min(size, remaining()).asInstanceOf[Int]
//    var currentOffset = offset
//
//    while (remainingSize > 0) {
//      val buff = fetchBufferOfSize(remainingSize)
//      val toCopy = math.min(buff.remaining(), remainingSize)
//
//      buff.get(arr, currentOffset, toCopy)
//      currentOffset += toCopy
//      remainingSize -= toCopy
//    }
//
//    currentOffset - offset
//  }
//
//
//  private def createSlice(size: Long): LargeByteBuffer = {
//
//    releasePendingContainers()
//
//    if (remaining() < size) {
//      // logInfo("createSlice. remaining = " + remaining() + ", size " + size + ", this = " + this)
//      throw new BufferOverflowException
//    }
//
//    // kyro depends on this it seems ?
//    // assert (size > 0)
//    if (0 == size) return LargeByteBuffer.EMPTY_BUFFER
//
//    val arr = new ArrayBuffer[ByteBufferContainer](2)
//    var totalLeft = size
//
//    // assert (currentRemaining() < totalLeft || totalLeft != size || currentAsByteBuffer)
//
//    var containerIndex = currentContainerIndex
//    while (totalLeft > 0 && hasRemaining()) {
//      assertPreconditions(containerIndex)
//      val container = containers(containerIndex)
//      val currentLeft = currentRemaining0(containerIndex)
//
//      assert (globalPosition + currentLeft <= globalLimit)
//      assert (globalPosition >= bufferPositionStart(containerIndex) &&
//        (globalPosition < bufferPositionStart(containerIndex + 1)))
//
//      val from = (globalPosition - bufferPositionStart(containerIndex)).asInstanceOf[Int]
//      val sliceSize = math.min(totalLeft, currentLeft)
//      assert (from >= 0)
//      assert (sliceSize > 0 && sliceSize <= Int.MaxValue)
//
//      val slice = container.createSlice(from, sliceSize.asInstanceOf[Int])
//      arr += slice
//
//      globalPosition += sliceSize
//      totalLeft -= sliceSize
//      if (currentLeft == sliceSize) containerIndex += 1
//    }
//
//    // Using toNonEmptyBuffer instead of directly moving to next here so that
//    // other checks can be performed there.
//    toNonEmptyBuffer()
//    // force cleanup - this is fine since we are not using the buffers directly
//    // which are actively needed (the returned value is on containers which can
//    // recreate)
//    releasePendingContainers()
//    // free current container if acquired.
//    if (currentContainerIndex < containers.length) {
//      containers(currentContainerIndex).release()
//    }
//    assert (currentContainerIndex == containerIndex)
//
//    val retval = new LargeByteBuffer(arr, false, ephemeralDiskBacked)
//    retval.overrideCleaner(LargeByteBuffer.noopDisposeFunction)
//    retval
//  }
//
//  // Get a composite sequence of ByteBuffer which might straddle one or more underlying buffers
//  // This is to be used only for writes : and ensures that writes are done into the appropriate
//  // underlying bytebuffers.
//  def getCompositeWriteBuffer(size: Long): LargeByteBuffer = {
//    assert(writable)
//    assert(size >= 0)
//
//    createSlice(size)
//  }
//
//  // get a buffer which is of the specified size and contains data from the underlying buffers
//  // Note, the actual data might be spread across the underlying buffers.
//  // This MUST BE used only for specific usecases like getInt, etc. Not for bulk copy !
//  private def readFully(size: Int): ByteBuffer = {
//    assert (readable)
//
//    if (remaining() < size) {
//      // throw exception
//      throw new BufferUnderflowException()
//    }
//
//    // kyro depends on this it seems ?
//    // assert (size > 0)
//    if (0 == size) return LargeByteBuffer.EMPTY_BYTEBUFFER
//
//    // Expected to be handled elsewhere.
//    assert (! localReadWritePossible(size))
//
//    val localBuff =  {
//      val buff = fetchBufferOfSize(size)
//      // assert(buff.remaining() <= size)
//      // if (buff.remaining() == size) return buff
//      assert(buff.remaining() < size)
//      ByteBuffer.allocate(size).put(buff)
//    }
//
//    // assert (localBuff.hasRemaining)
//
//    while (localBuff.hasRemaining) {
//      val buff = fetchBufferOfSize(localBuff.remaining())
//      localBuff.put(buff)
//    }
//
//    localBuff.flip()
//    localBuff
//  }
//
//
//
//  def put(b: Byte) {
//    assert (writable)
//    if (remaining() < 1) {
//      // logInfo("put byte. remaining = " + remaining() + ", this = " + this)
//      throw new BufferOverflowException
//    }
//
//    assert (currentRemaining() > 0)
//
//    fetchCurrentBuffer().put(b)
//    globalPosition += 1
//    // Check to need to bump the index ?
//    toNonEmptyBuffer()
//  }
//
//
//  def put(buffer: ByteBuffer) {
//    assert (writable)
//    if (remaining() < buffer.remaining()) {
//      throw new BufferOverflowException
//    }
//
//    val bufferRemaining = buffer.remaining()
//    if (localReadWritePossible(bufferRemaining)) {
//
//      assert (currentRemaining() >= bufferRemaining)
//
//      fetchCurrentBuffer().put(buffer)
//
//      globalPosition += bufferRemaining
//      toNonEmptyBuffer()
//      return
//    }
//
//    while (buffer.hasRemaining) {
//      val currentBufferRemaining = currentRemaining()
//      val bufferRemaining = buffer.remaining()
//
//      if (currentBufferRemaining >= bufferRemaining) {
//        fetchCurrentBuffer().put(buffer)
//        globalPosition += bufferRemaining
//      } else {
//        // Split across buffers.
//        val currentBuffer = fetchCurrentBuffer()
//        assert (currentBuffer.remaining() >= currentBufferRemaining)
//        val sliced = ByteBufferContainer.createSlice(buffer, buffer.position(),
//          currentBufferRemaining)
//        assert (sliced.remaining() == currentBufferRemaining)
//        currentBuffer.put(sliced)
//        // move buffer pos
//        buffer.position(buffer.position() + currentBufferRemaining)
//
//        globalPosition += currentBufferRemaining
//      }
//      toNonEmptyBuffer()
//    }
//
//    assert (! hasRemaining() || currentRemaining() > 0)
//  }
//
//  def put(other: LargeByteBuffer) {
//    assert (writable)
//    if (this.remaining() < other.remaining()) {
//      throw new BufferOverflowException
//    }
//
//    while (other.hasRemaining()) {
//      val buffer = other.fetchBufferOfSize(other.currentRemaining())
//      this.put(buffer)
//    }
//  }
//
//
//  def duplicate(): LargeByteBuffer = {
//    val containersCopy = new ArrayBuffer[ByteBufferContainer](containers.size)
//    // We do a duplicate as part of construction - so avoid double duplicate.
//    // containersCopy ++= containers.map(_.duplicate())
//    containersCopy ++= containers
//    val retval = new LargeByteBuffer(containersCopy, true, ephemeralDiskBacked)
//
//    // set limit and position (in that order) ...
//    retval.limit(this.limit())
//    retval.position(this.position())
//
//    // Now release our containers - if any had been acquired
//    releasePendingContainers()
//
//    retval
//  }
//
//
//  /**
//   * 'read' a LargeByteBuffer of size specified and return that.
//   * Position will be incremented by size
//   *
//   * The name might be slightly confusing : rename ?
//   *
//   * @param size Amount of data to be read from this buffer and returned
//   * @return
//   */
//  def readLargeBuffer(size: Long, partialReadAllowed: Boolean): LargeByteBuffer = {
//    if (! hasRemaining() && ! partialReadAllowed) throw new BufferUnderflowException
//    if (remaining() < size && ! partialReadAllowed) throw new BufferUnderflowException
//
//
//    assert (readable)
//    assert (size >= 0)
//
//    releasePendingContainers()
//
//    if (0 == size) return LargeByteBuffer.EMPTY_BUFFER
//
//    createSlice(size)
//  }
//
//
//  // This is essentially a workaround to exposing underlying buffers
//  def readFrom(channel: ReadableByteChannel): Long = {
//
//    assert (writable)
//    releasePendingContainers()
//
//    // this also allows us to avoid nasty corner cases in the loop.
//    if (! hasRemaining()) {
//      // logInfo("readFrom channel. remaining = " + remaining() + ", this = " + this)
//      throw new BufferOverflowException
//    }
//
//    var totalBytesRead = 0L
//
//    while (hasRemaining()) {
//      // read what we can ...
//      val buffer = fetchCurrentBuffer()
//      val bufferRemaining = currentRemaining()
//      val bytesRead = channel.read(buffer)
//
//      if (bytesRead > 0) {
//        totalBytesRead += bytesRead
//        // bump position too ..
//        globalPosition += bytesRead
//        if (bytesRead >= bufferRemaining) toNonEmptyBuffer()
//      }
//      else if (-1 == bytesRead) {
//        // if we had already read some data in the loop, return that.
//        if (totalBytesRead > 0) return totalBytesRead
//        return -1
//      }  // nothing available to read, retry later. return
//      else if (0 == bytesRead) {
//        return totalBytesRead
//      }
//
//      // toNonEmptyBuffer()
//    }
//
//    // Cleanup last buffer ?
//    toNonEmptyBuffer()
//    totalBytesRead
//  }
//
//  // This is essentially a workaround to exposing underlying buffers
//  def readFrom(inStrm: InputStream): Long = {
//
//    assert (writable)
//    releasePendingContainers()
//
//    // this also allows us to avoid nasty corner cases in the loop.
//    // if (! hasRemaining()) throw new BufferOverflowException
//    if (! hasRemaining()) return 0
//
//    var totalBytesRead = 0L
//
//    val buff = new Array[Byte](LargeByteBuffer.TEMP_ARRAY_SIZE)
//
//    while (hasRemaining()) {
//      // read what we can ... note, since there is no gaurantee that underlying buffer might
//      // expose array() method, we do double copy - from stream to buff and from buff to bytearray.
//      // see if we can optimize this later ...
//      val buffer = fetchCurrentBuffer()
//      val bufferRemaining = buffer.remaining()
//      val max = math.min(buff.length, bufferRemaining)
//      val bytesRead = inStrm.read(buff, 0, max)
//
//      if (bytesRead > 0) {
//        buffer.put(buff, 0, bytesRead)
//        totalBytesRead += bytesRead
//        // bump position too ..
//        globalPosition += bytesRead
//        // buffer.position(buffer.position + bytesRead)
//        if (bytesRead >= bufferRemaining) toNonEmptyBuffer()
//      }
//      else if (-1 == bytesRead) {
//        // if we had already read some data in the loop, return that.
//        if (totalBytesRead > 0) return totalBytesRead
//        return -1
//      }  // nothing available to read, retry later. return
//      else if (0 == bytesRead) {
//        return totalBytesRead
//      }
//
//      // toNonEmptyBuffer()
//    }
//
//    totalBytesRead
//  }
//
//  // This is essentially a workaround to exposing underlying buffers
//  // Note: very similar to readFrom(InputStream) : not trying anything fancy to reduce
//  // code for performance reasons.
//  def readFrom(inStrm: DataInput): Long = {
//
//    assert (writable)
//    releasePendingContainers()
//
//    // this also allows us to avoid nasty corner cases in the loop.
//    // if (! hasRemaining()) throw new BufferOverflowException
//    if (! hasRemaining()) return 0
//
//    var totalBytesRead = 0L
//
//    val buff = new Array[Byte](LargeByteBuffer.TEMP_ARRAY_SIZE)
//
//    while (hasRemaining()) {
//      // read what we can ... note, since there is no gaurantee that underlying buffer might
//      // expose array() method, we do double copy - from stream to buff and from buff to bytearray.
//      // see if we can optimize this later ...
//      val buffer = fetchCurrentBuffer()
//      val bufferRemaining = buffer.remaining()
//      val max = math.min(buff.length, bufferRemaining)
//      inStrm.readFully(buff, 0, max)
//      val bytesRead = max
//
//      if (bytesRead > 0) {
//        buffer.put(buff, 0, bytesRead)
//        totalBytesRead += bytesRead
//        // bump position too ..
//        globalPosition += bytesRead
//        // buffer.position(buffer.position() + bytesRead)
//        if (bytesRead >= bufferRemaining) toNonEmptyBuffer()
//      }
//      else if (-1 == bytesRead) {
//        // if we had already read some data in the loop, return that.
//        if (totalBytesRead > 0) return totalBytesRead
//        return -1
//      }  // nothing available to read, retry later. return
//      else if (0 == bytesRead) {
//        return totalBytesRead
//      }
//
//      // toNonEmptyBuffer()
//    }
//
//    totalBytesRead
//  }
//
//  // This is essentially a workaround to exposing underlying buffers
//  // Note: tries to do it efficiently without needing to load everything into memory
//  // (particularly for diskbacked buffers, etc).
//  def writeTo(channel: WritableByteChannel, cleanup: Boolean): Long = {
//
//    assert (readable)
//    releasePendingContainers()
//
//    // this also allows us to avoid nasty corner cases in the loop.
//    if (! hasRemaining()) throw new BufferUnderflowException
//
//    var totalBytesWritten = 0L
//
//    while (hasRemaining()) {
//      // Write what we can ...
//      val buffer = fetchCurrentBuffer()
//      val bufferRemaining = buffer.remaining()
//      assert (bufferRemaining > 0)
//      val bytesWritten = channel.write(buffer)
//
//      if (bytesWritten > 0) {
//        totalBytesWritten += bytesWritten
//        // bump position too ..
//        globalPosition += bytesWritten
//        if (bytesWritten >= bufferRemaining) toNonEmptyBuffer()
//        assert (! hasRemaining() || currentRemaining() > 0)
//      }
//      else if (0 == bytesWritten) {
//        return totalBytesWritten
//      }
//
//      // toNonEmptyBuffer()
//    }
//
//    assert (! hasRemaining())
//    if (cleanup) {
//      free()
//    }
//    totalBytesWritten
//  }
//
//  // This is essentially a workaround to exposing underlying buffers
//  def writeTo(outStrm: OutputStream, cleanup: Boolean): Long = {
//
//    assert (readable)
//    releasePendingContainers()
//
//    // this also allows us to avoid nasty corner cases in the loop.
//    if (! hasRemaining()) throw new BufferUnderflowException
//
//    var totalBytesWritten = 0L
//    val buff = new Array[Byte](LargeByteBuffer.TEMP_ARRAY_SIZE)
//
//    while (hasRemaining()) {
//      // write what we can ... note, since there is no gaurantee that underlying buffer might
//      // expose array() method, we do double copy - from bytearray to buff and from
//      // buff to outputstream. see if we can optimize this later ...
//      val buffer = fetchCurrentBuffer()
//      val bufferRemaining = buffer.remaining()
//      val size = math.min(bufferRemaining, buff.length)
//      buffer.get(buff, 0, size)
//      outStrm.write(buff, 0, size)
//
//      totalBytesWritten += size
//      // bump position too ..
//      globalPosition += size
//
//      if (size >= bufferRemaining) toNonEmptyBuffer()
//    }
//
//    toNonEmptyBuffer()
//    if (cleanup) {
//      free()
//    }
//    totalBytesWritten
//  }
//
//  def asInputStream(): InputStream = {
//    new InputStream() {
//      override def read(): Int = {
//        if (! hasRemaining()) return -1
//        get()
//      }
//
//      override def read(arr: Array[Byte], off: Int, len: Int): Int = {
//        if (! hasRemaining()) return -1
//
//        get(arr, off, len)
//      }
//
//      override def available(): Int = {
//        // current remaining is what can be read without blocking
//        // anything higher might need disk access/buffer swapping.
//        /*
//        val left = remaining()
//        math.min(left, Int.MaxValue).asInstanceOf[Int]
//        */
//        currentRemaining()
//      }
//    }
//  }
//
//  def getCleaner() = cleaner
//
//  /**
//   * @param cleaner The previous cleaner, so that the caller can chain them if required.
//   * @return
//   */
//  private[spark] def overrideCleaner(cleaner: BufferCleaner): BufferCleaner = {
//    overrideCleaner(cleaner, allowOverride = true)
//  }
//
//  private def overrideCleaner(cleaner: BufferCleaner, allowOverride: Boolean): BufferCleaner = {
//    if (! this.allowCleanerOverride) {
//      // allowCleanerOverride = false is used for EMPTY_BUFFER - where we do not allow free
//      return this.cleaner
//    }
//
//    this.allowCleanerOverride = allowOverride
//    assert (null != cleaner)
//    val prev = this.cleaner
//    this.cleaner = cleaner
//    // logInfo("Overriding " + prev + " with " + this.cleaner)
//    prev
//  }
//
//  private def doReleaseAll() {
//    for (container <- containers) {
//      container.release()
//    }
//  }
//
//  def free(invokeCleaner: Boolean = true) {
//    // logInfo("Free on " + this + ", cleaner = " + cleaner)
//    // always invoking release
//    doReleaseAll()
//
//    if (invokeCleaner) cleaner.clean(this)
//  }
//
//  private def doDispose(needRelease: Boolean) {
//
//    if (disposeLocationThrowable ne null) {
//      logError("Already free'ed earlier at : ", disposeLocationThrowable)
//      logError("Current at ", new Throwable)
//      throw new IllegalStateException("Already freed.")
//    }
//    disposeLocationThrowable = new Throwable()
//
//    // Forcefully cleanup all
//    if (needRelease) doReleaseAll()
//
//    // Free in a different loop, in case different containers refer to same resource
//    // to release (like file)
//    for (container <- containers) {
//      container.free()
//    }
//
//    needReleaseIndices.clear()
//
//    // We should not use this buffer anymore : set the values such that                 f
//    // we dont ...
//    globalPosition = 0
//    globalLimit = 0
//    globalCapacity = 0
//  }
//
//  // copy data over ... MUST be used only for cases where array is known to be
//  // small to begin with. slightly risky method due to that assumption
//  def toByteArray(): Array[Byte] = {
//    val positionBackup = position()
//    val size = remaining()
//    if (size > Int.MaxValue) {
//      throw new IllegalStateException(
//        "Attempt to convert LargeByteBuffer to byte array when data held is more than 2G")
//    }
//
//    val retval = new Array[Byte](size.asInstanceOf[Int])
//    val readSize = get(retval, 0, retval.length)
//    assert (readSize == retval.length,
//      "readSize = " + readSize + ", retval.length = " + retval.length)
//
//    position(positionBackup)
//
//    retval
//  }
//
//  // copy data over ... MUST be used only for cases where array is known to be
//  // small to begin with. slightly risky method due to that assumption
//  def toByteBuffer(): ByteBuffer = {
//    ByteBuffer.wrap(toByteArray())
//  }
//
//  def toInMemoryBuffer(ioConf: IOConfig): LargeByteBuffer = {
//    val retval = LargeByteBuffer.allocateMemoryBuffer(remaining(), ioConf)
//    val currentPosition = position()
//    retval.put(this)
//    position(currentPosition)
//    retval.clear()
//    retval
//  }
//
//
//
//  // This is ONLY used for testing : that too as part of development of this and associated classes
//  // remove before contributing to spark.
//  def hexDump(): String = {
//    if (remaining() * 64 > Int.MaxValue) {
//      throw new UnsupportedOperationException("buffer too large " + remaining())
//    }
//
//    val sb = new StringBuilder((remaining() * 2).asInstanceOf[Int])
//
//    var perLine = 0
//    var first = true
//    for (b <- toByteArray()) {
//      perLine += 1
//      if (perLine % 8 == 0) {
//        sb.append('\n')
//        first = true
//      }
//      if (! first) sb.append(' ')
//      first = false
//      sb.append(java.lang.Integer.toHexString(b & 0xff))
//    }
//    sb.append('\n')
//    sb.toString()
//  }
//
//  override def toString: String = {
//    val sb: StringBuffer = new StringBuffer
//    sb.append(getClass.getName)
//    sb.append(' ')
//    sb.append(System.identityHashCode(this))
//    sb.append("@[pos=")
//    sb.append(position())
//    sb.append(" lim=")
//    sb.append(limit())
//    sb.append(" cap=")
//    sb.append(capacity())
//    sb.append("]")
//    sb.toString
//  }
//
//
//
//  override def finalize(): Unit = {
//    var marked = false
//    if (containers ne null) {
//      if (containers.exists(container => container.isAcquired && container.requireRelease())) {
//        marked = true
//        logError("BUG: buffer was not released - and now going out of scope. " +
//          "Potential resource leak. Allocated at ", allocateLocationThrowable)
//        containers.foreach(_.release())
//      }
//      if (containers.exists(container => !container.isFreed && container.requireFree())) {
//        if (!marked) {
//          logError("BUG: buffer was not freed - and now going out of scope. Potential resource leak",
//            allocateLocationThrowable)
//        }
//        else {
//          logError("BUG: buffer was not freed - and now going out of scope. Potential resource leak")
//        }
//        containers.foreach(_.free())
//      }
//    }
//    super.finalize()
//  }
//}
//
//
//object LargeByteBuffer extends Logging {
//
//  private val noopDisposeFunction = new BufferCleaner() {
//    protected def doClean(buffer: LargeByteBuffer) {
//      buffer.free(invokeCleaner = false)
//    }
//  }
//
//  val enableExpensiveAssert = false
//  private val EMPTY_BYTEBUFFER = ByteBuffer.allocate(0)
//  val EMPTY_BUFFER = new LargeByteBuffer(ArrayBuffer(
//    new HeapByteBufferContainer(EMPTY_BYTEBUFFER, false)), false, false)
//  // Do not allow anyone else to override cleaner
//  EMPTY_BUFFER.overrideCleaner(noopDisposeFunction, allowOverride = false)
//
//  // 8K sufficient ?
//  private val TEMP_ARRAY_SIZE = 8192
//
//  /**
//   * Create a LargeByteBuffer of specified size which is split across
//   * ByteBuffer's of size DiskStore.MAX_BLOCK_SIZE and backed by in memory
//   * ByteBuffer
//   *
//   */
//  def allocateMemoryBuffer(totalSize: Long, ioConf: IOConfig): LargeByteBuffer = {
//    if (0 == totalSize) {
//      return EMPTY_BUFFER
//    }
//
//    assert (totalSize > 0)
//
//    val blockSize = ioConf.getMaxBlockSize(BufferType.MEMORY)
//    val numBlocks = ioConf.numBlocks(BufferType.MEMORY, totalSize)
//    val lastBlockSize = ioConf.lastBlockSize(BufferType.MEMORY, totalSize)
//
//    assert (lastBlockSize > 0)
//
//    val bufferArray = {
//      val arr = new ArrayBuffer[ByteBufferContainer](numBlocks)
//      for (index <- 0 until numBlocks - 1) {
//        val buff = ByteBuffer.allocate(blockSize)
//        // buff.clear()
//        arr += new HeapByteBufferContainer(buff, true)
//      }
//      arr += new HeapByteBufferContainer(ByteBuffer.allocate(lastBlockSize), true)
//      assert (arr.length == numBlocks)
//      arr
//    }
//
//    new LargeByteBuffer(bufferArray, false, false)
//  }
//
//  /**
//   * Create a LargeByteBuffer of specified size which is split across
//   * ByteBuffer's of size DiskStore.MAX_BLOCK_SIZE and backed by on disk
//   *
//   */
//  private def allocateDiskBuffer(totalSize: Long,
//      blockManager: BlockManager): LargeByteBuffer = {
//    if (0 == totalSize) {
//      return EMPTY_BUFFER
//    }
//
//    assert (totalSize > 0)
//
//    // Create a file of the specified size.
//    val file = blockManager.diskBlockManager.createTempBlock()._2
//    val raf = new RandomAccessFile(file, "rw")
//    try {
//      raf.setLength(totalSize)
//    } finally {
//      raf.close()
//    }
//
//    readWriteDiskSegment(new FileSegment(file, 0, totalSize),
//      ephemeralDiskBacked = true, blockManager.ioConf)
//  }
//
//  // The returned buffer takes up ownership of the underlying buffers
//  // (including dispos'ing that when done)
//  def fromBuffers(buffers: ByteBuffer*): LargeByteBuffer = {
//    val nonEmpty = buffers.filter(_.hasRemaining)
//
//    // cleanup the empty buffers
//    buffers.filter(! _.hasRemaining).foreach(b => BlockManager.dispose(b))
//
//
//    if (nonEmpty.isEmpty) {
//      return EMPTY_BUFFER
//    }
//
//    // slice so that offsets match our requirement
//    new LargeByteBuffer(new ArrayBuffer() ++ nonEmpty.map(b =>
//      new HeapByteBufferContainer(b.slice(), true)), false, false)
//  }
//
//  def fromByteArrays(byteArrays: Array[Byte]*): LargeByteBuffer = {
//    // only non empty arrays
//    val arrays = byteArrays.filter(_.length > 0)
//    if (0 == arrays.length) return EMPTY_BUFFER
//
//    new LargeByteBuffer(new ArrayBuffer() ++ arrays.map(arr =>
//      new HeapByteBufferContainer(ByteBuffer.wrap(arr), true)), false, false)
//  }
//
//  def fromLargeByteBuffers(canDispose: Boolean, inputBuffers: LargeByteBuffer*): LargeByteBuffer = {
//
//    if (inputBuffers.isEmpty) return EMPTY_BUFFER
//
//    if (! inputBuffers.exists(_.hasRemaining())) {
//      if (canDispose) inputBuffers.map(_.free())
//      return EMPTY_BUFFER
//    }
//
//    // release all temp resources acquired
//    inputBuffers.foreach(buff => buff.releasePendingContainers())
//    // free current container if acquired.
//    inputBuffers.foreach(buff => if (buff.currentContainerIndex < buff.containers.length) {
//      buff.containers(buff.currentContainerIndex).release()
//    })
//    // inputBuffers.foreach(b => b.doReleaseAll())
//
//
//    // Dispose of any empty buffers
//    if (canDispose) inputBuffers.filter(! _.hasRemaining()).foreach(_.free())
//
//    // Find all containers we need.
//    val buffers = inputBuffers.filter(_.hasRemaining()).map(b => b.createSlice(b.remaining()))
//
//    val containers = buffers.flatMap(_.containers)
//    assert (! containers.isEmpty)
//    // The in order containers of "buffers" seq constitute the required return value
//    val retval = new LargeByteBuffer(new ArrayBuffer() ++ containers,
//      // if you cant dispose, then we dont own the buffers : in which case, need duplicate
//      ! canDispose, inputBuffers.exists(_.ephemeralDiskBacked))
//
//    if (canDispose) {
//      // override dispose of all other buffers.
//      val disposeFunctions = inputBuffers.map {
//        buffer => {
//          (buffer, buffer.overrideCleaner(noopDisposeFunction))
//        }
//      }
//
//      val cleaner = retval.getCleaner()
//      val newCleaner = new BufferCleaner {
//        protected def doClean(buffer: LargeByteBuffer) {
//
//          assert (retval == buffer)
//          // default cleaner.
//          cleaner.clean(retval)
//          // not required, since we are within clean anyway.
//          // retval.free(invokeCleaner = false)
//
//          // retval.doDispose(needRelease = true)
//
//          // This might actually call dispose twice on some (initially) empty buffers,
//          // which is fine since we now guard against that.
//          disposeFunctions.foreach(v => v._2.clean(v._1))
//          // Call the free method too : so that buffers are marked free ...
//          disposeFunctions.foreach(v => v._1.free(invokeCleaner = false))
//        }
//      }
//
//      val prev = retval.overrideCleaner(newCleaner)
//      assert (prev == cleaner)
//    }
//
//    retval
//  }
//
//  private def checkOffsets(arr: Array[Byte], offset: Int, size: Int) {
//    if (arr == null) {
//      throw new NullPointerException
//    } else if (offset < 0 || size < 0 || offset + size > arr.length) {
//      throw new IndexOutOfBoundsException
//    }
//  }
//
//  def allocateTransientBuffer(size: Long, blockManager: BlockManager) = {
//    if (size <= blockManager.ioConf.maxInMemSize) {
//      LargeByteBuffer.allocateMemoryBuffer(size, blockManager.ioConf)
//    } else {
//      LargeByteBuffer.allocateDiskBuffer(size, blockManager)
//    }
//  }
//
//  def readFromDiskSegment(segment: FileSegment, ioConf: IOConfig,
//      ephemeralDiskBacked: Boolean): LargeByteBuffer = {
//    // Split the block into multiple of BlockStore.maxBlockSize
//    val segmentSize = segment.length
//    val blockSize = ioConf.getMaxBlockSize(BufferType.DISK).asInstanceOf[Long]
//    val numBlocks = ioConf.numBlocks(BufferType.DISK, segmentSize)
//    val lastBlockSize = ioConf.lastBlockSize(BufferType.DISK, segmentSize)
//
//    val buffers = new ArrayBuffer[ByteBufferContainer](numBlocks)
//
//    for (index <- 0 until numBlocks - 1) {
//      buffers += new ReadOnlyFileContainer(new FileSegment(segment.file,
//        segment.offset + index * blockSize, blockSize), ioConf)
//    }
//
//    // Last block
//    buffers += new ReadOnlyFileContainer(new FileSegment(segment.file,
//      segment.offset + (numBlocks - 1) * blockSize, lastBlockSize), ioConf)
//
//    new LargeByteBuffer(buffers, false, ephemeralDiskBacked)
//  }
//
//  def readWriteDiskSegment(segment: FileSegment, ephemeralDiskBacked: Boolean,
//      ioConf: IOConfig): LargeByteBuffer = {
//
//    // Split the block into multiple of BlockStore.maxBlockSize
//    val segmentSize = segment.length
//    val blockSize = ioConf.getMaxBlockSize(BufferType.DISK).asInstanceOf[Long]
//    val numBlocks = ioConf.numBlocks(BufferType.DISK, segmentSize)
//    val lastBlockSize = ioConf.lastBlockSize(BufferType.DISK, segmentSize)
//
//    logInfo("readWriteDiskSegment = " + segment + ", numBlocks = " + numBlocks +
//      ", lastBlockSize = " + lastBlockSize)
//    val buffers = new ArrayBuffer[ByteBufferContainer](numBlocks)
//
//    for (index <- 0 until numBlocks - 1) {
//      buffers += new ReadWriteFileContainer(new FileSegment(segment.file,
//        segment.offset + index * blockSize, blockSize), ephemeralDiskBacked, null)
//    }
//
//    // Last block
//    buffers += new ReadWriteFileContainer(new FileSegment(segment.file,
//      segment.offset + (numBlocks - 1) * blockSize, lastBlockSize), ephemeralDiskBacked, null)
//
//    new LargeByteBuffer(buffers, false, ephemeralDiskBacked)
//  }
//}
