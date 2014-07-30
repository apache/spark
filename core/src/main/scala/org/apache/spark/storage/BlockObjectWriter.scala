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

import java.io.{BufferedOutputStream, File, FileNotFoundException, FileOutputStream}
import java.io.{SyncFailedException, IOException, OutputStream}
import java.nio.channels.FileChannel

import org.apache.spark.Logging
import org.apache.spark.serializer.{SerializationStream, Serializer}
import org.apache.spark.util.Utils

/**
 * An interface for writing JVM objects to some underlying storage. This interface allows
 * appending data to an existing block, and can guarantee atomicity in the case of faults
 * as it allows the caller to revert partial writes.
 *
 * This interface does not support concurrent writes.
 */
private[spark] abstract class BlockObjectWriter(val blockId: BlockId) {

  /**
   * Flush the partial writes and commit them as a single atomic block. Return the
   * number of bytes written for this commit.
   */
  def commitAndClose()

  /**
   * Reverts writes that haven't been flushed yet. Callers should invoke this function
   * when there are runtime exceptions.
   */
  def revertPartialWritesAndClose()

  /**
   * Writes an object.
   */
  def write(value: Any)

  /**
   * Returns the file segment of committed data that this Writer has written.
   */
  def fileSegment(): FileSegment

  /**
   * Cumulative time spent performing blocking writes, in ns.
   */
  def timeWriting(): Long

  /**
   * Number of bytes written so far
   */
  def bytesWritten: Long
}

/**
 * BlockObjectWriter which writes directly to a file on disk. Appends to the given file.
 * Note, this impl is NOT MT-safe : use external synchronization if you need it to be so.
 *
 * TODO: Some of the asserts, particularly which use File.* methods can be expensive - ensure they
 * are not in critical path.
 */
private[spark] class DiskBlockObjectWriter(
    blockId: BlockId,
    file: File,
    serializer: Serializer,
    bufferSize: Int,
    compressStream: OutputStream => OutputStream,
    syncWrites: Boolean)
  extends BlockObjectWriter(blockId)
  with Logging
{

  /** Intercepts write calls and tracks total time spent writing. Not thread safe. */
  private class TimeTrackingOutputStream(out: OutputStream) extends OutputStream {
    def timeWriting = _timeWriting
    private var _timeWriting = 0L

    private def callWithTiming(f: => Unit) = {
      val start = System.nanoTime()
      f
      _timeWriting += (System.nanoTime() - start)
    }

    def write(i: Int): Unit = callWithTiming(out.write(i))
    override def write(b: Array[Byte]) = callWithTiming(out.write(b))
    override def write(b: Array[Byte], off: Int, len: Int) = callWithTiming(out.write(b, off, len))
    override def close() = out.close()
    override def flush() = out.flush()
  }

  /** The file channel, used for repositioning / truncating the file. */
  private var channel: FileChannel = null
  private var bs: OutputStream = null
  private var fos: FileOutputStream = null
  private var ts: TimeTrackingOutputStream = null
  private var objOut: SerializationStream = null

  // Did we create this file or was it already present : used in revert to decide
  // if we should delete this file or not. Also used to detect if file was deleted
  // between creation of BOW and its actual init
  private val initiallyExists = file.exists() && file.isFile
  private val initialPosition = file.length()
  private var lastValidPosition = initialPosition

  private var initialized = false
  // closed explicitly ?
  private var closed = false
  // Attempt to cleanly close ? (could also be closed via revert)
  // Note, a cleanly closed file could be subsequently reverted
  private var cleanCloseAttempted = false
  // Was the file actually opened atleast once.
  // Note: initialized/streams change state with close/revert.
  private var wasOpenedOnce = false
  private var _timeWriting = 0L

  // Due to some directory creation race issues in spark, it has been observed that
  // sometimes file creation happens 'before' the actual directory has been created
  // So we attempt to retry atleast once with a mkdirs in case directory was missing.
  private def init() {
    init(canRetry = true)
  }

  private def init(canRetry: Boolean) {

    if (closed) throw new IOException("Already closed")

    assert(! initialized)
    assert(! wasOpenedOnce)
    var exists = false
    try {
      exists = file.exists()
      if (! exists && initiallyExists && 0 != initialPosition && ! Utils.inShutdown) {
        // Was deleted by cleanup thread ?
        throw new IOException("file " + file + " cleaned up ? exists = " + exists +
          ", initiallyExists = " + initiallyExists + ", initialPosition = " + initialPosition)
      }
      fos = new FileOutputStream(file, true)
    } catch {
      case fEx: FileNotFoundException =>
        // There seems to be some race in directory creation.
        // Attempts to fix it dont seem to have worked : working around the problem for now.
        logDebug("Unable to open " + file + ", canRetry = " + canRetry + ", exists = " + exists +
          ", initialPosition = " + initialPosition + ", in shutdown = " + Utils.inShutdown(), fEx)
        if (canRetry && ! Utils.inShutdown()) {
          // try creating the parent directory if that is the issue.
          // Since there can be race with others, dont bother checking for
          // success/failure - the call to init() will resolve if fos can be created.
          file.getParentFile.mkdirs()
          // Note, if directory did not exist, then file does not either - and so
          // initialPosition would be zero in either case.
          init(canRetry = false)
          return
        } else throw fEx
    }

    try {
      // This is to workaround case where creation of object and actual init
      // (which can happen much later) happens after a delay and the cleanup thread
      // cleaned up the file.
      channel = fos.getChannel
      val fosPos = channel.position()
      if (initialPosition != fosPos) {
        throw new IOException("file cleaned up ? " + file.exists() + 
          ", initialpos = " + initialPosition +
          "current len = " + fosPos + ", in shutdown ? " + Utils.inShutdown)
      }

      ts = new TimeTrackingOutputStream(fos)
      val bos = new BufferedOutputStream(ts, bufferSize)
      bs = compressStream(bos)
      objOut = serializer.newInstance().serializeStream(bs)
      initialized = true
      wasOpenedOnce = true;
    } finally {
      if (! initialized) {
        // failed, cleanup state.
        val tfos = fos
        updateCloseState()
        tfos.close()
      }
    }
  }

  private def open(): BlockObjectWriter = {
    init()
    lastValidPosition = initialPosition
    this
  }

  private def updateCloseState() {

    if (null != ts) _timeWriting += ts.timeWriting

    bs = null
    channel = null
    fos = null
    ts = null
    objOut = null
    initialized = false
  }

  private def flushAll() {
    if (closed) throw new IOException("Already closed")

    // NOTE: Because Kryo doesn't flush the underlying stream we explicitly flush both the
    //       serializer stream and the lower level stream.
    if (null != objOut) {
      objOut.flush()
      bs.flush()
    }
  }

  private def closeAll(needFlush: Boolean, needRevert: Boolean) {

    if (null != objOut) {
      val truncatePos = if (needRevert) initialPosition else -1L
      assert(! this.closed)

      // In case syncWrites is true or we need to truncate
      var cleanlyClosed = false
      try {
        // Flushing if we need to truncate also. Currently, we reopen to truncate
        // so this is not strictly required (since close could write further to streams).
        // Keeping it around in case that gets relaxed.
        if (needFlush || needRevert) flushAll()

        val start = System.nanoTime()
        try {
          if (syncWrites) {
            // Force outstanding writes to disk and track how long it takes
            fos.getFD.sync()
          }
        } catch {
          case sfe: SyncFailedException => // ignore
        }
        // must cause cascading close. Note, repeated close on closed streams should not cause
        // issues : except some libraries do not honour it - hence not explicitly closing bs/fos
        objOut.close()
        // bs.close()
        // fos.close()
        _timeWriting += System.nanoTime() - start

        // fos MUST have been closed.
        assert(null == channel || !channel.isOpen)
        cleanlyClosed = true

      } finally {

        this.closed = true
        if (! cleanlyClosed) {
          // could not cleanly close. We have two cases here -
          // a) normal close,
          // b) revert
          // If (a) then then streams/data is in inconsistent so we cant really recover
          // simply release fd and allow exception to bubble up.
          // If (b) and file length >= initialPosition, then truncate file and ignore exception
          // else,cause exception to bubble up since we cant recover
          assert(null != fos)
          try { fos.close() } catch { case ioEx: IOException => /* best case attempt, ignore */ }
        }

        updateCloseState()

        // Since close can end up writing data in general case (inspite of flush),
        // we reopen to truncate file.
        if (needRevert) {
          // remove if not earlier existed : best case effort so we dont care about return value
          // of delete (it can fail if file was already deleted by cleaner threads for example)
          if (! initiallyExists) {
            file.delete()
            // Explicitly ignore exceptions (when cleanlyClosed = false) and return
            // from here. Usually not good idea in finally, but it is ok here.
            return
          } else {
            val fileLen = file.length()
            if (fileLen >= truncatePos) {
              if (fileLen > truncatePos) DiskBlockObjectWriter.truncateIfExists(file, truncatePos)

              // Validate length.
              assert(truncatePos == file.length() || Utils.inShutdown(),
                "truncatePos = " + truncatePos + ", len = " + file.length() +
                    ", in shutdown = " + Utils.inShutdown())

              // Explicitly ignore exceptions (when cleanlyClosed = false) and return
              // from here. Usually not good idea in finally, but it is ok here.
              return
            } // else cause the exception to bubble up if thrown
          }
        }
      }
    } else {
      // it is possible for open to have never been called - no data written to this
      // partition for example. so objOut == null
      this.closed = true
    }
    initialized = false
  }

  private def validateBytesWritten() {
    // This should happen due to file deletion, during cleanup. Ensure bytesWritten is in sane 
    // state. Note, parallel threads continue to run while shutdown threads are running : so 
    // this prevents unwanted assertion failures and exception elsewhere.
    if (lastValidPosition < initialPosition) {
      // This is invoked so that assertions within bytes written are validated.
      assert(bytesWritten >= 0)
      lastValidPosition = initialPosition
    }
  }

  override def commitAndClose() {
    if (initialized) {
      // opened, file still open
      assert(wasOpenedOnce)
      // Note, set cleanCloseAttempted even before we finish the close : so that a revert on this
      // in case close fails can truncate to previous state !
      cleanCloseAttempted = true
      closeAll(needFlush = true, needRevert = false)

      val prevPos = lastValidPosition
      assert(prevPos == initialPosition)
      assert(null == fos)

      lastValidPosition = file.length()
      validateBytesWritten()
      // review: remove ?
      assert(bytesWritten >= 0, "bytesWritten = " + bytesWritten +
        ", initial pos = " + initialPosition + ", last valid pos = " + lastValidPosition)

    } else if (cleanCloseAttempted) {
      // opened and closed cleanly
      assert(closed)
      assert(wasOpenedOnce)
      // size should be lastValidPosition, or file deleted due to shutdown.
      assert(lastValidPosition == file.length() || Utils.inShutdown,
        "lastValidPosition = " + lastValidPosition  +
          ", file len = " + file.length() + ", exists = " + file.exists())

    } else {
      // reverted or never opened.
      this.closed = true
      assert(initialPosition == file.length() || (0 == initialPosition && ! initiallyExists) ||
          Utils.inShutdown, "initialPosition = " + initialPosition +
          ", file len = " + file.length() + ", exists = " + file.exists())
      assert(lastValidPosition == initialPosition)
    }
  }

  override def revertPartialWritesAndClose() {
    if (initialized) {
      // opened, file still open
      // Discard current writes. We do this by truncating the file to the last valid position.
      closeAll(needFlush = true, needRevert = true)
      validateBytesWritten()
      assert(bytesWritten == 0, "bytesWritten = " + bytesWritten +
        ", initial pos = " + initialPosition + ", last valid pos = " + lastValidPosition)
      assert(initialPosition == file.length() || Utils.inShutdown,
        "initialPosition = " + initialPosition +
          ", file len = " + file.length() + ", exists = " + file.exists())
    } else if (cleanCloseAttempted) {
      // Already opened and closed : truncate to last location (or delete
      // if created in this instance)
      assert(closed)
      cleanCloseAttempted = false

      // truncate to initialPosition
      // remove if not earlier existed
      if (! initiallyExists) {
        // best case effort so we dont care about return value
        // of delete (it can fail if file was already deleted by cleaner threads for example)
        file.delete()
      } else if (file.exists()) {
        DiskBlockObjectWriter.truncateIfExists(file, initialPosition)
      }
      // reset position.
      lastValidPosition = initialPosition


      assert(file.length() == initialPosition || Utils.inShutdown,
        "initialPosition = " + initialPosition +
          ", file len = " + file.length() + ", exists = " + file.exists())
    } else {
      this.closed = true
      assert(initialPosition == file.length() || (0 == initialPosition && ! initiallyExists) ||
          Utils.inShutdown,
        "initialPosition = " + initialPosition +
          ", file len = " + file.length() + ", exists = " + file.exists())
    }
  }

  override def write(value: Any) {
    if (!initialized) {
      open()
    }
    // Not checking if closed on purpose ... introduce it ? No usecase for it right now.
    objOut.writeObject(value)
  }

  override def fileSegment(): FileSegment = {
    assert(! initialized)
    assert(null == fos)
    assert(wasOpenedOnce || 0 == bytesWritten,
      "wasOpenedOnce = " + wasOpenedOnce + ", initialPosition = " + initialPosition +
        ", bytesWritten = " + bytesWritten + ", file len = " + file.length())

    new FileSegment(file, initialPosition, bytesWritten)
  }

  // Only valid if called after close()
  override def timeWriting() = _timeWriting

  // Only valid if called after commit()
  override def bytesWritten: Long = {
    val retval = lastValidPosition - initialPosition

    assert(retval >= 0 || Utils.inShutdown(),
      "exists = " + file.exists() + ", bytesWritten = " + retval +
      ", lastValidPosition = " + lastValidPosition + ", initialPosition = " + initialPosition +
      ", in shutdown = " + Utils.inShutdown())

    // TODO: Comment this out when we are done validating : can be expensive due to file.length()
    assert(file.length() >= lastValidPosition || Utils.inShutdown(),
      "exists = " + file.exists() + ", file len = " + file.length() +
          ", bytesWritten = " + retval + ", lastValidPosition = " + lastValidPosition +
          ", initialPosition = " + initialPosition + ", in shutdown = " + Utils.inShutdown())

    if (retval >= 0) retval else 0
  }
}

object DiskBlockObjectWriter{

  // Unfortunately, cant do it atomically ...
  private def truncateIfExists(file: File, truncatePos: Long) {
    var fos: FileOutputStream = null
    try {
      // There is no way to do this atomically iirc.
      if (file.exists() && file.length() != truncatePos) {
        fos = new FileOutputStream(file, true)
        fos.getChannel.truncate(truncatePos)
      }
    } finally {
      if (null != fos) {
        fos.close()
      }
    }
  }
}

