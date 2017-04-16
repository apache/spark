/*
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

package org.apache.hadoop.io.compress.zlib;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.util.NativeCodeLoader;

/**
 * A {@link Decompressor} based on the popular 
 * zlib compression algorithm.
 * http://www.zlib.net/
 * 
 */
public class ZlibDecompressor implements Decompressor {
  private static final int DEFAULT_DIRECT_BUFFER_SIZE = 64*1024;
  
  // HACK - Use this as a global lock in the JNI layer
  private static Class clazz = ZlibDecompressor.class;
  
  private long stream;
  private CompressionHeader header;
  private int directBufferSize;
  private Buffer compressedDirectBuf = null;
  private int compressedDirectBufOff, compressedDirectBufLen;
  private Buffer uncompressedDirectBuf = null;
  private byte[] userBuf = null;
  private int userBufOff = 0, userBufLen = 0;
  private boolean finished;
  private boolean needDict;

  /**
   * The headers to detect from compressed data.
   */
  public static enum CompressionHeader {
    /**
     * No headers/trailers/checksums.
     */
    NO_HEADER (-15),
    
    /**
     * Default headers/trailers/checksums.
     */
    DEFAULT_HEADER (15),
    
    /**
     * Simple gzip headers/trailers.
     */
    GZIP_FORMAT (31),
    
    /**
     * Autodetect gzip/zlib headers/trailers.
     */
    AUTODETECT_GZIP_ZLIB (47);

    private final int windowBits;
    
    CompressionHeader(int windowBits) {
      this.windowBits = windowBits;
    }
    
    public int windowBits() {
      return windowBits;
    }
  }

  private static boolean nativeZlibLoaded = false;
  
  static {
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      try {
        // Initialize the native library
        initIDs();
        nativeZlibLoaded = true;
      } catch (Throwable t) {
        // Ignore failure to load/initialize native-zlib
      }
    }
  }
  
  static boolean isNativeZlibLoaded() {
    return nativeZlibLoaded;
  }

  /**
   * Creates a new decompressor.
   */
  public ZlibDecompressor(CompressionHeader header, int directBufferSize) {
    this.header = header;
    this.directBufferSize = directBufferSize;
    compressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    uncompressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);
    
    stream = init(this.header.windowBits());
  }
  
  public ZlibDecompressor() {
    this(CompressionHeader.DEFAULT_HEADER, DEFAULT_DIRECT_BUFFER_SIZE);
  }

  public synchronized void setInput(byte[] b, int off, int len) {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }
  
    this.userBuf = b;
    this.userBufOff = off;
    this.userBufLen = len;
    
    setInputFromSavedData();
    
    // Reinitialize zlib's output direct buffer 
    uncompressedDirectBuf.limit(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);
  }
  
  synchronized void setInputFromSavedData() {
    compressedDirectBufOff = 0;
    compressedDirectBufLen = userBufLen;
    if (compressedDirectBufLen > directBufferSize) {
      compressedDirectBufLen = directBufferSize;
    }

    // Reinitialize zlib's input direct buffer
    compressedDirectBuf.rewind();
    ((ByteBuffer)compressedDirectBuf).put(userBuf, userBufOff, 
                                          compressedDirectBufLen);
    
    // Note how much data is being fed to zlib
    userBufOff += compressedDirectBufLen;
    userBufLen -= compressedDirectBufLen;
  }

  public synchronized void setDictionary(byte[] b, int off, int len) {
    if (stream == 0 || b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }
    setDictionary(stream, b, off, len);
    needDict = false;
  }

  public synchronized boolean needsInput() {
    // Consume remanining compressed data?
    if (uncompressedDirectBuf.remaining() > 0) {
      return false;
    }
    
    // Check if zlib has consumed all input
    if (compressedDirectBufLen <= 0) {
      // Check if we have consumed all user-input
      if (userBufLen <= 0) {
        return true;
      } else {
        setInputFromSavedData();
      }
    }
    
    return false;
  }

  public synchronized boolean needsDictionary() {
    return needDict;
  }

  public synchronized boolean finished() {
    // Check if 'zlib' says its 'finished' and
    // all compressed data has been consumed
    return (finished && uncompressedDirectBuf.remaining() == 0);
  }

  public synchronized int decompress(byte[] b, int off, int len) 
    throws IOException {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }
    
    int n = 0;
    
    // Check if there is uncompressed data
    n = uncompressedDirectBuf.remaining();
    if (n > 0) {
      n = Math.min(n, len);
      ((ByteBuffer)uncompressedDirectBuf).get(b, off, n);
      return n;
    }
    
    // Re-initialize the zlib's output direct buffer
    uncompressedDirectBuf.rewind();
    uncompressedDirectBuf.limit(directBufferSize);

    // Decompress data
    n = inflateBytesDirect();
    uncompressedDirectBuf.limit(n);

    // Get atmost 'len' bytes
    n = Math.min(n, len);
    ((ByteBuffer)uncompressedDirectBuf).get(b, off, n);

    return n;
  }
  
  /**
   * Returns the total number of compressed bytes output so far.
   *
   * @return the total (non-negative) number of compressed bytes output so far
   */
  public synchronized long getBytesWritten() {
    checkStream();
    return getBytesWritten(stream);
  }

  /**
   * Returns the total number of uncompressed bytes input so far.</p>
   *
   * @return the total (non-negative) number of uncompressed bytes input so far
   */
  public synchronized long getBytesRead() {
    checkStream();
    return getBytesRead(stream);
  }

  public synchronized void reset() {
    checkStream();
    reset(stream);
    finished = false;
    needDict = false;
    compressedDirectBufOff = compressedDirectBufLen = 0;
    uncompressedDirectBuf.limit(directBufferSize);
    uncompressedDirectBuf.position(directBufferSize);
    userBufOff = userBufLen = 0;
  }

  public synchronized void end() {
    if (stream != 0) {
      end(stream);
      stream = 0;
    }
  }

  protected void finalize() {
    end();
  }
  
  private void checkStream() {
    if (stream == 0)
      throw new NullPointerException();
  }
  
  private native static void initIDs();
  private native static long init(int windowBits);
  private native static void setDictionary(long strm, byte[] b, int off,
                                           int len);
  private native int inflateBytesDirect();
  private native static long getBytesRead(long strm);
  private native static long getBytesWritten(long strm);
  private native static void reset(long strm);
  private native static void end(long strm);
}
