/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.io;

import java.io.FileDescriptor;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.nativeio.NativeIO;

import org.apache.hadoop.thirdparty.guava.common.base.Preconditions;
import org.apache.hadoop.thirdparty.guava.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Manages a pool of threads which can issue readahead requests on file descriptors.
 */
//@InterfaceAudience.Private
//@InterfaceStability.Evolving
public class ReadaheadPool {
  static final Log LOG = LogFactory.getLog(ReadaheadPool.class);
  private static final int POOL_SIZE = 4;
  private static final int MAX_POOL_SIZE = 16;
  private static final int CAPACITY = 1024;
  private final ThreadPoolExecutor pool;
  
  private static ReadaheadPool instance;

  /**
   * Return the singleton instance for the current process.
   */
  public static ReadaheadPool getInstance() {
    synchronized (ReadaheadPool.class) {
      if (instance == null && NativeIO.isAvailable()) {
        instance = new ReadaheadPool();
      }
      return instance;
    }
  }
  
  private ReadaheadPool() {
    pool = new ThreadPoolExecutor(POOL_SIZE, MAX_POOL_SIZE, 3L, TimeUnit.SECONDS,
        new ArrayBlockingQueue<Runnable>(CAPACITY));
    pool.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardOldestPolicy());
    pool.setThreadFactory(new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat("Readahead Thread #%d")
      .build());
  }

  /**
   * Issue a request to readahead on the given file descriptor.
   * 
   * @param identifier a textual identifier that will be used in error
   * messages (e.g. the file name)
   * @param fd the file descriptor to read ahead
   * @param curPos the current offset at which reads are being issued
   * @param readaheadLength the configured length to read ahead
   * @param maxOffsetToRead the maximum offset that will be readahead
   *        (useful if, for example, only some segment of the file is
   *        requested by the user). Pass {@link Long.MAX_VALUE} to allow
   *        readahead to the end of the file.
   * @param lastReadahead the result returned by the previous invocation
   *        of this function on this file descriptor, or null if this is
   *        the first call
   * @return an object representing this outstanding request, or null
   *        if no readahead was performed
   */
  public ReadaheadRequest readaheadStream(
      String identifier,
      FileDescriptor fd,
      long curPos,
      long readaheadLength,
      long maxOffsetToRead,
      ReadaheadRequest lastReadahead) {
    
    Preconditions.checkArgument(curPos <= maxOffsetToRead,
        "Readahead position %s higher than maxOffsetToRead %s",
        curPos, maxOffsetToRead);

    if (readaheadLength <= 0) {
      return null;
    }
    
    long lastOffset = Long.MIN_VALUE;
    
    if (lastReadahead != null) {
      lastOffset = lastReadahead.getOffset();
    }

    // trigger each readahead when we have reached the halfway mark
    // in the previous readahead. This gives the system time
    // to satisfy the readahead before we start reading the data.
    long nextOffset = lastOffset + readaheadLength / 2; 
    if (curPos >= nextOffset) {
      // cancel any currently pending readahead, to avoid
      // piling things up in the queue. Each reader should have at most
      // one outstanding request in the queue.
      if (lastReadahead != null) {
        lastReadahead.cancel();
        lastReadahead = null;
      }
      
      long length = Math.min(readaheadLength,
          maxOffsetToRead - curPos);

      if (length <= 0) {
        // we've reached the end of the stream
        return null;
      }
      
      return submitReadahead(identifier, fd, curPos, length);
    } else {
      return lastReadahead;
    }
  }
      
  /**
   * Submit a request to readahead on the given file descriptor.
   * @param identifier a textual identifier used in error messages, etc.
   * @param fd the file descriptor to readahead
   * @param off the offset at which to start the readahead
   * @param len the number of bytes to read
   * @return an object representing this pending request
   */
  public ReadaheadRequest submitReadahead(
      String identifier, FileDescriptor fd, long off, long len) {
    ReadaheadRequestImpl req = new ReadaheadRequestImpl(
        identifier, fd, off, len);
    pool.execute(req);
    if (LOG.isTraceEnabled()) {
      LOG.trace("submit readahead: " + req);
    }
    return req;
  }
  
  /**
   * An outstanding readahead request that has been submitted to
   * the pool. This request may be pending or may have been
   * completed.
   */
  public interface ReadaheadRequest {
    /**
     * Cancels the request for readahead. This should be used
     * if the reader no longer needs the requested data, <em>before</em>
     * closing the related file descriptor.
     * 
     * It is safe to use even if the readahead request has already
     * been fulfilled.
     */
    public void cancel();
    
    /**
     * @return the requested offset
     */
    public long getOffset();

    /**
     * @return the requested length
     */
    public long getLength();
  }
  
  private static class ReadaheadRequestImpl implements Runnable, ReadaheadRequest {
    private final String identifier;
    private final FileDescriptor fd;
    private final long off, len;
    private volatile boolean canceled = false;
    
    private ReadaheadRequestImpl(String identifier, FileDescriptor fd, long off, long len) {
      this.identifier = identifier;
      this.fd = fd;
      this.off = off;
      this.len = len;
    }
    
    public void run() {
      if (canceled) return;
      // There's a very narrow race here that the file will close right at
      // this instant. But if that happens, we'll likely receive an EBADF
      // error below, and see that it's canceled, ignoring the error.
      // It's also possible that we'll end up requesting readahead on some
      // other FD, which may be wasted work, but won't cause a problem.
      try {
        NativeIO.posixFadviseIfPossible(fd, off, len,
            NativeIO.POSIX_FADV_WILLNEED);
      } catch (IOException ioe) {
        if (canceled) {
          // no big deal - the reader canceled the request and closed
          // the file.
          return;
        }
        LOG.warn("Failed readahead on " + identifier,
            ioe);
      }
    }

    @Override
    public void cancel() {
      canceled = true;
      // We could attempt to remove it from the work queue, but that would
      // add complexity. In practice, the work queues remain very short,
      // so removing canceled requests has no gain.
    }

    @Override
    public long getOffset() {
      return off;
    }

    @Override
    public long getLength() {
      return len;
    }

    @Override
    public String toString() {
      return "ReadaheadRequestImpl [identifier='" + identifier + "', fd=" + fd
          + ", off=" + off + ", len=" + len + "]";
    }
  }
}
