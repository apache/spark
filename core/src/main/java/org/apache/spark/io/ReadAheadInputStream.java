package org.apache.spark.io;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * {@link InputStream} implementation which asynchronously reads ahead from the underlying input
 * stream when specified amount of data has been read from the current buffer. It does it by maintaining
 * two buffer - active buffer and read ahead buffer. Active buffer contains data which should be returned
 * when a read() call is issued. The read ahead buffer is used to asynchronously read from the underlying
 * input stream and once the current active buffer is exhausted, we flip the two buffers so that we can
 * start reading from the read ahead buffer without being blocked in disk I/O.
 */
public class ReadAheadInputStream extends InputStream {

  private Lock stateChangeLock = new ReentrantLock();

  @GuardedBy("stateChangeLock")
  private ByteBuffer activeBuffer;

  @GuardedBy("stateChangeLock")
  private ByteBuffer readAheadBuffer;

  @GuardedBy("stateChangeLock")
  private boolean endOfStream;

  @GuardedBy("stateChangeLock")
  // true if async read is in progress
  private boolean isReadInProgress;

  @GuardedBy("stateChangeLock")
  // true if read is aborted due to an exception in reading from underlying input stream.
  private boolean isReadAborted;

  @GuardedBy("stateChangeLock")
  private Exception readException;

  // If the remaining data size in the current buffer is below this threshold,
  // we issue an async read from the underlying input stream.
  private final int readAheadThresholdInBytes;

  private final InputStream underlyingInputStream;

  private final ExecutorService executorService = Executors.newSingleThreadExecutor();

  private final Condition asyncReadComplete = stateChangeLock.newCondition();

  private final byte[] oneByte = new byte[1];

  public ReadAheadInputStream(InputStream inputStream, int bufferSizeInBytes, int readAheadThresholdInBytes) {
    Preconditions.checkArgument(bufferSizeInBytes > 0, "bufferSizeInBytes should be greater than 0");
    Preconditions.checkArgument(readAheadThresholdInBytes > 0 && readAheadThresholdInBytes < bufferSizeInBytes,
                                "readAheadThresholdInBytes should be greater than 0 and less than bufferSizeInBytes" );
    activeBuffer = ByteBuffer.allocate(bufferSizeInBytes);
    readAheadBuffer = ByteBuffer.allocate(bufferSizeInBytes);
    this.readAheadThresholdInBytes = readAheadThresholdInBytes;
    this.underlyingInputStream = inputStream;
    activeBuffer.flip();
    readAheadBuffer.flip();
  }

  private boolean isEndOfStream() {
    if(activeBuffer.remaining() == 0 && readAheadBuffer.remaining() == 0 && endOfStream) {
      return true;
    }
    return  false;
  }


  private void readAsync(final ByteBuffer byteBuffer) throws IOException {
    stateChangeLock.lock();
    if (endOfStream || isReadInProgress) {
      stateChangeLock.unlock();
      return;
    }
    byteBuffer.position(0);
    byteBuffer.flip();
    isReadInProgress = true;
    stateChangeLock.unlock();
    executorService.execute(() -> {
      byte[] arr;
      stateChangeLock.lock();
      arr = byteBuffer.array();
      stateChangeLock.unlock();
      // Please note that it is safe to release the lock and read into the read ahead buffer
      // because either of following two conditions will hold - 1. The active buffer has
      // data available to read so the reader will not read from the read ahead buffer.
      // 2. The active buffer is exhausted, in that case the reader waits for this async
      // read to complete. So there is no race condition in both the situations.
      int nRead = 0;
      while (nRead == 0) {
        try {
          nRead = underlyingInputStream.read(arr);
          if (nRead < 0) {
            // We hit end of the underlying input stream
            break;
          }
        } catch (Exception e) {
          stateChangeLock.lock();
          // We hit a read exception, which should be propagated to the reader
          // in the next read() call.
          isReadAborted = true;
          readException = e;
          stateChangeLock.unlock();
        }
      }
      stateChangeLock.lock();
      if (nRead < 0) {
        endOfStream = true;
      }
      else {
        // fill the byte buffer
        byteBuffer.limit(nRead);
      }
      isReadInProgress = false;
      signalAsyncReadComplete();
      stateChangeLock.unlock();
    });
  }

  private void signalAsyncReadComplete() {
    stateChangeLock.lock();
    try {
      asyncReadComplete.signalAll();
    } finally {
      stateChangeLock.unlock();
    }
  }

  private void waitForAsyncReadComplete() {
    stateChangeLock.lock();
    try {
      asyncReadComplete.await();
    } catch (InterruptedException e) {
    } finally {
      stateChangeLock.unlock();
    }
  }

  @Override
  public synchronized int read() throws IOException {
    int val = read(oneByte, 0, 1);
    if (val == -1) {
      return -1;
    }
    return oneByte[0] & 0xFF;
  }

  @Override
  public synchronized int read(byte[] b, int offset, int len) throws IOException {
    stateChangeLock.lock();
    try {
      len = readInternal(b, offset, len);
    }
    finally {
      stateChangeLock.unlock();
    }
    return len;
  }

  /**
   * Internal read function which should be called only from read() api. The assumption is that
   * the stateChangeLock is already acquired in the caller before calling this function.
   */
  private int readInternal(byte[] b, int offset, int len) throws IOException {

    if (offset < 0 || len < 0 || offset + len < 0 || offset + len > b.length) {
      throw new IndexOutOfBoundsException();
    }
    if (!activeBuffer.hasRemaining() && !isReadInProgress) {
      // This condition will only be triggered for the first time read is called.
      readAsync(activeBuffer);
      waitForAsyncReadComplete();
    }
    if (!activeBuffer.hasRemaining() && isReadInProgress) {
      waitForAsyncReadComplete();
    }

    if (isReadAborted) {
      throw new IOException(readException);
    }
    if (isEndOfStream()) {
      return -1;
    }
    len = Math.min(len, activeBuffer.remaining());
    activeBuffer.get(b, offset, len);

    if (activeBuffer.remaining() <= readAheadThresholdInBytes && !readAheadBuffer.hasRemaining()) {
      readAsync(readAheadBuffer);
    }
    if (!activeBuffer.hasRemaining()) {
      ByteBuffer temp = activeBuffer;
      activeBuffer = readAheadBuffer;
      readAheadBuffer = temp;
    }
    return len;
  }

  @Override
  public synchronized int available() throws IOException {
    stateChangeLock.lock();
    int val = activeBuffer.remaining() + readAheadBuffer.remaining();
    stateChangeLock.unlock();
    return val;
  }

  @Override
  public synchronized long skip(long n) throws IOException {
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
    if (n <= 0L) {
      return 0L;
    }
    if (isReadInProgress) {
      waitForAsyncReadComplete();
    }
    if (available() >= n) {
      // we can skip from the internal buffers
      int toSkip = (int)n;
      byte[] temp = new byte[toSkip];
      while (toSkip > 0) {
        int skippedBytes = read(temp, 0, toSkip);
        toSkip -= skippedBytes;
      }
      return n;
    }
    int skippedBytes = available();
    long toSkip = n - skippedBytes;
    activeBuffer.position(0);
    activeBuffer.flip();
    readAheadBuffer.position(0);
    readAheadBuffer.flip();
    long skippedFromInputStream = underlyingInputStream.skip(toSkip);
    return skippedBytes + skippedFromInputStream;
  }
}
