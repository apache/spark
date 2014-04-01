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

package org.apache.hadoop.net;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;


/**
 * This implements an input stream that can have a timeout while reading.
 * This sets non-blocking flag on the socket channel.
 * So after create this object, read() on 
 * {@link Socket#getInputStream()} and write() on 
 * {@link Socket#getOutputStream()} for the associated socket will throw 
 * IllegalBlockingModeException. 
 * Please use {@link SocketOutputStream} for writing.
 */
public class SocketInputStream extends InputStream
                               implements ReadableByteChannel {

  private Reader reader;

  private static class Reader extends SocketIOWithTimeout {
    ReadableByteChannel channel;
    
    Reader(ReadableByteChannel channel, long timeout) throws IOException {
      super((SelectableChannel)channel, timeout);
      this.channel = channel;
    }
    
    int performIO(ByteBuffer buf) throws IOException {
      return channel.read(buf);
    }
  }
  
  /**
   * Create a new input stream with the given timeout. If the timeout
   * is zero, it will be treated as infinite timeout. The socket's
   * channel will be configured to be non-blocking.
   * 
   * @param channel 
   *        Channel for reading, should also be a {@link SelectableChannel}.
   *        The channel will be configured to be non-blocking.
   * @param timeout timeout in milliseconds. must not be negative.
   * @throws IOException
   */
  public SocketInputStream(ReadableByteChannel channel, long timeout)
                                                        throws IOException {
    SocketIOWithTimeout.checkChannelValidity(channel);
    reader = new Reader(channel, timeout);
  }

  /**
   * Same as SocketInputStream(socket.getChannel(), timeout): <br><br>
   * 
   * Create a new input stream with the given timeout. If the timeout
   * is zero, it will be treated as infinite timeout. The socket's
   * channel will be configured to be non-blocking.
   * 
   * @see SocketInputStream#SocketInputStream(ReadableByteChannel, long)
   *  
   * @param socket should have a channel associated with it.
   * @param timeout timeout timeout in milliseconds. must not be negative.
   * @throws IOException
   */
  public SocketInputStream(Socket socket, long timeout) 
                                         throws IOException {
    this(socket.getChannel(), timeout);
  }
  
  /**
   * Same as SocketInputStream(socket.getChannel(), socket.getSoTimeout())
   * :<br><br>
   * 
   * Create a new input stream with the given timeout. If the timeout
   * is zero, it will be treated as infinite timeout. The socket's
   * channel will be configured to be non-blocking.
   * @see SocketInputStream#SocketInputStream(ReadableByteChannel, long)
   *  
   * @param socket should have a channel associated with it.
   * @throws IOException
   */
  public SocketInputStream(Socket socket) throws IOException {
    this(socket.getChannel(), socket.getSoTimeout());
  }
  
  @Override
  public int read() throws IOException {
    /* Allocation can be removed if required.
     * probably no need to optimize or encourage single byte read.
     */
    byte[] buf = new byte[1];
    int ret = read(buf, 0, 1);
    if (ret > 0) {
      return (byte)buf[0];
    }
    if (ret != -1) {
      // unexpected
      throw new IOException("Could not read from stream");
    }
    return ret;
  }

  public int read(byte[] b, int off, int len) throws IOException {
    return read(ByteBuffer.wrap(b, off, len));
  }

  public synchronized void close() throws IOException {
    /* close the channel since Socket.getInputStream().close()
     * closes the socket.
     */
    reader.channel.close();
    reader.close();
  }

  /**
   * Returns underlying channel used by inputstream.
   * This is useful in certain cases like channel for 
   * {@link FileChannel#transferFrom(ReadableByteChannel, long, long)}.
   */
  public ReadableByteChannel getChannel() {
    return reader.channel; 
  }
  
  //ReadableByteChannel interface
    
  public boolean isOpen() {
    return reader.isOpen();
  }
    
  public int read(ByteBuffer dst) throws IOException {
    return reader.doIO(dst, SelectionKey.OP_READ);
  }
  
  /**
   * waits for the underlying channel to be ready for reading.
   * The timeout specified for this stream applies to this wait.
   * 
   * @throws SocketTimeoutException 
   *         if select on the channel times out.
   * @throws IOException
   *         if any other I/O error occurs. 
   */
  public void waitForReadable() throws IOException {
    reader.waitForIO(SelectionKey.OP_READ);
  }

  public void setTimeout(long timeoutMs) {
    reader.setTimeout(timeoutMs);
  }
}
