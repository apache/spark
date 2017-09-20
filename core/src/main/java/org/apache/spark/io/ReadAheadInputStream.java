/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.io;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.spark.util.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * {@link InputStream} implementation which asynchronously reads ahead from the underlying input
 * stream when specified amount of data has been read from the current buffer. It does it by
 * maintaining two buffers - active buffer and read ahead buffer. Active buffer contains data
 * which should be returned when a read() call is issued. The read ahead buffer is used to
 * asynchronously read from the underlying input stream and once the current active buffer is
 * exhausted, we flip the two buffers so that we can start reading from the read ahead buffer
 * without being blocked in disk I/O.
 */
public class ReadAheadInputStream extends InputStream {

  private static final Logger logger = LoggerFactory.getLogger(ReadAheadInputStream.class);

  private ReentrantLock stateChangeLock = new ReentrantLock();

  @GuardedBy("stateChangeLock")
  private ByteBuffer activeBuffer;

  @GuardedBy("stateChangeLock")
  private ByteBuffer readAheadBuffer;

  @GuardedBy("stateChangeLock")
  private boolean endOfStream;

  @GuardedBy("stateChangeLock")
  // true if async read is in progress
  private boolean readInProgress;

  @GuardedBy("stateChangeLock")
  // true if read is aborted due to an exception in reading from underlying input stream.
  private boolean readAborted;

  @GuardedBy("stateChangeLock")
  private Throwable readException;

  @GuardedBy("stateChangeLock")
  // whether the close method is called.
  private boolean isClosed;

  @GuardedBy("stateChangeLock")
  // true when the close method will close the underlying input stream. This is valid only if
  // `isClosed` is true.
  private boolean isUnderlyingInputStreamBeingClosed;

  @GuardedBy("stateChangeLock")
  // whether there is a read ahead task running,
  private boolean isReading;

  // If the remaining data size in the current buffer is below this threshold,
  // we issue an async read from the underlying input stream.
  private final int readAheadThresholdInBytes;

  private final InputStream underlyingInputStream;

  private final ExecutorService executorService =
      ThreadUtils.newDaemonSingleThreadExecutor("read-ahead");

  private final Condition asyncReadComplete = stateChangeLock.newCondition();

  private static final ThreadLocal<byte[]> oneByte = ThreadLocal.withInitial(() -> new byte[1]);

  /**
   * Creates a <code>ReadAheadInputStream</code> with the specified buffer size and read-ahead
   * threshold
   *
   * @param inputStream The underlying input stream.
   * @param bufferSizeInBytes The buffer size.
   * @param readAheadThresholdInBytes If the active buffer has less data than the read-ahead
   *                                  threshold, an async read is triggered.
   */
  public ReadAheadInputStream(
      InputStream inputStream, int bufferSizeInBytes, int readAheadThresholdInBytes) {
    Preconditions.checkArgument(bufferSizeInBytes > 0,
        "bufferSizeInBytes should be greater than 0, but the value is " + bufferSizeInBytes);
    Preconditions.checkArgument(readAheadThresholdInBytes > 0 &&
            readAheadThresholdInBytes < bufferSizeInBytes,
        "readAheadThresholdInBytes should be greater than 0 and less than bufferSizeInBytes, " +
            "but the value is " + readAheadThresholdInBytes);
    activeBuffer = ByteBuffer.allocate(bufferSizeInBytes);
    readAheadBuffer = ByteBuffer.allocate(bufferSizeInBytes);
    this.readAheadThresholdInBytes = readAheadThresholdInBytes;
    this.underlyingInputStream = inputStream;
    activeBuffer.flip();
    readAheadBuffer.flip();
  }

  private boolean isEndOfStream() {
    return (!activeBuffer.hasRemaining() && !readAheadBuffer.hasRemaining() && endOfStream);
  }

  private void checkReadException() throws IOException {
    if (readAborted) {
      Throwables.propagateIfPossible(readException, IOException.class);
      throw new IOException(readException);
    }
  }

  /** Read data from underlyingInputStream to readAheadBuffer asynchronously. */
  private void readAsync() throws IOException {
    stateChangeLock.lock();
    final byte[] arr = readAheadBuffer.array();
    try {
      if (endOfStream || readInProgress) {
        return;
      }
      checkReadException();
      readAheadBuffer.position(0);
      readAheadBuffer.flip();
      readInProgress = true;
    } finally {
      stateChangeLock.unlock();
    }
    executorService.execute(new Runnable() {

      @Override
      public void run() {
        stateChangeLock.lock();
        try {
          if (isClosed) {
            readInProgress = false;
            return;
          }
          // Flip this so that the close method will not close the underlying input stream when we
          // are reading.
          isReading = true;
        } finally {
          stateChangeLock.unlock();
        }

        // Please note that it is safe to release the lock and read into the read ahead buffer
        // because either of following two conditions will hold - 1. The active buffer has
        // data available to read so the reader will not read from the read ahead buffer.
        // 2. This is the first time read is called or the active buffer is exhausted,
        // in that case the reader waits for this async read to complete.
        // So there is no race condition in both the situations.
        int read = 0;
        Throwable exception = null;
        try {
          while (true) {
            read = underlyingInputStream.read(arr);
            if (0 != read) break;
          }
        } catch (Throwable ex) {
          exception = ex;
          if (ex instanceof Error) {
            // `readException` may not be reported to the user. Rethrow Error to make sure at least
            // The user can see Error in UncaughtExceptionHandler.
            throw (Error) ex;
          }
        } finally {
          stateChangeLock.lock();
          if (read < 0 || (exception instanceof EOFException)) {
            endOfStream = true;
          } else if (exception != null) {
            readAborted = true;
            readException = exception;
          } else {
            readAheadBuffer.limit(read);
          }
          readInProgress = false;
          signalAsyncReadComplete();
          stateChangeLock.unlock();
          closeUnderlyingInputStreamIfNecessary();
        }
      }
    });
  }

  private void closeUnderlyingInputStreamIfNecessary() {
    boolean needToCloseUnderlyingInputStream = false;
    stateChangeLock.lock();
    try {
      isReading = false;
      if (isClosed && !isUnderlyingInputStreamBeingClosed) {
        // close method cannot close underlyingInputStream because we were reading.
        needToCloseUnderlyingInputStream = true;
      }
    } finally {
      stateChangeLock.unlock();
    }
    if (needToCloseUnderlyingInputStream) {
      try {
        underlyingInputStream.close();
      } catch (IOException e) {
        logger.warn(e.getMessage(), e);
      }
    }
  }

  private void signalAsyncReadComplete() {
    stateChangeLock.lock();
    try {
      asyncReadComplete.signalAll();
    } finally {
      stateChangeLock.unlock();
    }
  }

  private void waitForAsyncReadComplete() throws IOException {
    stateChangeLock.lock();
    try {
      while (readInProgress) {
        asyncReadComplete.await();
      }
    } catch (InterruptedException e) {
      InterruptedIOException iio = new InterruptedIOException(e.getMessage());
      iio.initCause(e);
      throw iio;
    } finally {
      stateChangeLock.unlock();
    }
    checkReadException();
  }

  @Override
  public int read() throws IOException {
    byte[] oneByteArray = oneByte.get();
    return read(oneByteArray, 0, 1) == -1 ? -1 : oneByteArray[0] & 0xFF;
  }

  @Override
  public int read(byte[] b, int offset, int len) throws IOException {
    if (offset < 0 || len < 0 || len > b.length - offset) {
      throw new IndexOutOfBoundsException();
    }
    if (len == 0) {
      return 0;
    }
    stateChangeLock.lock();
    try {
      return readInternal(b, offset, len);
    } finally {
      stateChangeLock.unlock();
    }
  }

  /**
   * flip the active and read ahead buffer
   */
  private void swapBuffers() {
    ByteBuffer temp = activeBuffer;
    activeBuffer = readAheadBuffer;
    readAheadBuffer = temp;
  }

  /**
   * Internal read function which should be called only from read() api. The assumption is that
   * the stateChangeLock is already acquired in the caller before calling this function.
   */
  private int readInternal(byte[] b, int offset, int len) throws IOException {
    assert (stateChangeLock.isLocked());
    if (!activeBuffer.hasRemaining()) {
      waitForAsyncReadComplete();
      if (readAheadBuffer.hasRemaining()) {
        swapBuffers();
      } else {
        // The first read or activeBuffer is skipped.
        readAsync();
        waitForAsyncReadComplete();
        if (isEndOfStream()) {
          return -1;
        }
        swapBuffers();
      }
    } else {
      checkReadException();
    }
    len = Math.min(len, activeBuffer.remaining());
    activeBuffer.get(b, offset, len);

    if (activeBuffer.remaining() <= readAheadThresholdInBytes && !readAheadBuffer.hasRemaining()) {
      readAsync();
    }
    return len;
  }

  @Override
  public int available() throws IOException {
    stateChangeLock.lock();
    // Make sure we have no integer overflow.
    try {
      return (int) Math.min((long) Integer.MAX_VALUE,
          (long) activeBuffer.remaining() + readAheadBuffer.remaining());
    } finally {
      stateChangeLock.unlock();
    }
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0L) {
      return 0L;
    }
    stateChangeLock.lock();
    long skipped;
    try {
      skipped = skipInternal(n);
    } finally {
      stateChangeLock.unlock();
    }
    return skipped;
  }

  /**
   * Internal skip function which should be called only from skip() api. The assumption is that
   * the stateChangeLock is already acquired in the caller before calling this function.
   */
  private long skipInternal(long n) throws IOException {
    assert (stateChangeLock.isLocked());
    waitForAsyncReadComplete();
    if (isEndOfStream()) {
      return 0;
    }
    if (available() >= n) {
      // we can skip from the internal buffers
      int toSkip = (int) n;
      if (toSkip <= activeBuffer.remaining()) {
        // Only skipping from active buffer is sufficient
        activeBuffer.position(toSkip + activeBuffer.position());
        if (activeBuffer.remaining() <= readAheadThresholdInBytes
            && !readAheadBuffer.hasRemaining()) {
          readAsync();
        }
        return n;
      }
      // We need to skip from both active buffer and read ahead buffer
      toSkip -= activeBuffer.remaining();
      activeBuffer.position(0);
      activeBuffer.flip();
      readAheadBuffer.position(toSkip + readAheadBuffer.position());
      swapBuffers();
      readAsync();
      return n;
    } else {
      int skippedBytes = available();
      long toSkip = n - skippedBytes;
      activeBuffer.position(0);
      activeBuffer.flip();
      readAheadBuffer.position(0);
      readAheadBuffer.flip();
      long skippedFromInputStream = underlyingInputStream.skip(toSkip);
      readAsync();
      return skippedBytes + skippedFromInputStream;
    }
  }

  @Override
  public void close() throws IOException {
    boolean isSafeToCloseUnderlyingInputStream = false;
    stateChangeLock.lock();
    try {
      if (isClosed) {
        return;
      }
      isClosed = true;
      if (!isReading) {
        // Nobody is reading, so we can close the underlying input stream in this method.
        isSafeToCloseUnderlyingInputStream = true;
        // Flip this to make sure the read ahead task will not close the underlying input stream.
        isUnderlyingInputStreamBeingClosed = true;
      }
    } finally {
      stateChangeLock.unlock();
    }

    try {
      executorService.shutdownNow();
      executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      InterruptedIOException iio = new InterruptedIOException(e.getMessage());
      iio.initCause(e);
      throw iio;
    } finally {
      if (isSafeToCloseUnderlyingInputStream) {
        underlyingInputStream.close();
      }
    }
  }
}
