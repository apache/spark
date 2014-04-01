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
package org.apache.hadoop.fs;

import java.io.BufferedInputStream;
import java.io.IOException;


/**
 * A class optimizes reading from FSInputStream by bufferring
 */


public class BufferedFSInputStream extends BufferedInputStream
implements Seekable, PositionedReadable {
  /**
   * Creates a <code>BufferedFSInputStream</code>
   * with the specified buffer size,
   * and saves its  argument, the input stream
   * <code>in</code>, for later use.  An internal
   * buffer array of length  <code>size</code>
   * is created and stored in <code>buf</code>.
   *
   * @param   in     the underlying input stream.
   * @param   size   the buffer size.
   * @exception IllegalArgumentException if size <= 0.
   */
  public BufferedFSInputStream(FSInputStream in, int size) {
    super(in, size);
  }

  public long getPos() throws IOException {
    return ((FSInputStream)in).getPos()-(count-pos);
  }

  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }

    seek(getPos()+n);
    return n;
  }

  public void seek(long pos) throws IOException {
    if( pos<0 ) {
      return;
    }
    // optimize: check if the pos is in the buffer
    long end = ((FSInputStream)in).getPos();
    long start = end - count;
    if( pos>=start && pos<end) {
      this.pos = (int)(pos-start);
      return;
    }

    // invalidate buffer
    this.pos = 0;
    this.count = 0;

    ((FSInputStream)in).seek(pos);
  }

  public boolean seekToNewSource(long targetPos) throws IOException {
    pos = 0;
    count = 0;
    return ((FSInputStream)in).seekToNewSource(targetPos);
  }

  public int read(long position, byte[] buffer, int offset, int length) throws IOException {
    return ((FSInputStream)in).read(position, buffer, offset, length) ;
  }

  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    ((FSInputStream)in).readFully(position, buffer, offset, length);
  }

  public void readFully(long position, byte[] buffer) throws IOException {
    ((FSInputStream)in).readFully(position, buffer);
  }
}
