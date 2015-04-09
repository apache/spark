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
package org.apache.spark.network.buffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public interface LargeByteBuffer {
  public byte get();

  /**
   * Bulk copy data from this buffer into the given array.  First checks there is sufficient
   * data in this buffer; if not, throws a {@link java.nio.BufferUnderflowException}.
   *
   * @param dst
   * @param offset
   * @param length
   */
  public void get(byte[] dst, int offset, int length);

  public LargeByteBuffer rewind();

  /**
   * Return a deep copy of this buffer.
   * The returned buffer will have position == 0.  The position
   * of this buffer will not change as a result of copying.
   *
   * @return a new buffer with a full copy of this buffer's data
   */
  public LargeByteBuffer deepCopy();

  /**
   * Advance the position in this buffer by up to <code>n</code> bytes.  <code>n</code> may be
   * positive or negative.  It will move the full <code>n</code> unless that moves
   * it past the end (or beginning) of the buffer, in which case it will move to the end
   * (or beginning).
   *
   * @return the number of bytes moved forward (can be negative if <code>n</code> is negative)
   */
  public long skip(long n);

  public long position();

  /**
   * Creates a new byte buffer that shares this buffer's content.
   * <p/>
   * <p> The content of the new buffer will be that of this buffer.  Changes
   * to this buffer's content will be visible in the new buffer, and vice
   * versa; the two buffers' positions will be independent.
   * <p/>
   * <p> The new buffer's position will be identical to those of this buffer
   */
  public LargeByteBuffer duplicate();


  public long remaining();

  /**
   * Total number of bytes in this buffer
   *
   * @return
   */
  public long size();

  /**
   * Writes the data from the current <code>position()</code> to the end of this buffer
   * to the given channel.  The <code>position()</code> will be moved to the end of
   * the buffer after this.
   * <p/>
   * Note that this method will continually attempt to push data to the given channel.  If the
   * channel cannot accept more data, this will continuously retry until the channel accepts
   * the data.
   *
   * @param channel
   * @return the number of bytes written to the channel
   * @throws IOException
   */
  public long writeTo(WritableByteChannel channel) throws IOException;

  /**
   * Get the entire contents of this as one ByteBuffer, if possible.  The returned ByteBuffer
   * will always have the position set 0, and the limit set to the end of the data.  Each
   * call will return a new ByteBuffer, but will not require copying the data (eg., it will
   * use ByteBuffer#duplicate()).  The returned byte buffer and this may or may not share data.
   *
   * @return
   * @throws BufferTooLargeException if this buffer is too large to fit in one {@link ByteBuffer}
   */
  public ByteBuffer asByteBuffer() throws BufferTooLargeException;

  /**
   * Attempt to clean up if it is memory-mapped. This uses an *unsafe* Sun API that
   * might cause errors if one attempts to read from the unmapped buffer, but it's better than
   * waiting for the GC to find it because that could lead to huge numbers of open files. There's
   * unfortunately no standard API to do this.
   */
  public void dispose();
}
