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
import java.io.InterruptedIOException;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;

/**
 * This supports input and output streams for a socket channels. 
 * These streams can have a timeout.
 */
abstract class SocketIOWithTimeout {
  // This is intentionally package private.

  static final Log LOG = LogFactory.getLog(SocketIOWithTimeout.class);    
  
  private SelectableChannel channel;
  private long timeout;
  private boolean closed = false;
  
  private static SelectorPool selector = new SelectorPool();
  
  /* A timeout value of 0 implies wait for ever. 
   * We should have a value of timeout that implies zero wait.. i.e. 
   * read or write returns immediately.
   * 
   * This will set channel to non-blocking.
   */
  SocketIOWithTimeout(SelectableChannel channel, long timeout) 
                                                 throws IOException {
    checkChannelValidity(channel);
    
    this.channel = channel;
    this.timeout = timeout;
    // Set non-blocking
    channel.configureBlocking(false);
  }
  
  void close() {
    closed = true;
  }

  boolean isOpen() {
    return !closed && channel.isOpen();
  }

  SelectableChannel getChannel() {
    return channel;
  }
  
  /** 
   * Utility function to check if channel is ok.
   * Mainly to throw IOException instead of runtime exception
   * in case of mismatch. This mismatch can occur for many runtime
   * reasons.
   */
  static void checkChannelValidity(Object channel) throws IOException {
    if (channel == null) {
      /* Most common reason is that original socket does not have a channel.
       * So making this an IOException rather than a RuntimeException.
       */
      throw new IOException("Channel is null. Check " +
                            "how the channel or socket is created.");
    }
    
    if (!(channel instanceof SelectableChannel)) {
      throw new IOException("Channel should be a SelectableChannel");
    }    
  }
  
  /**
   * Performs actual IO operations. This is not expected to block.
   *  
   * @param buf
   * @return number of bytes (or some equivalent). 0 implies underlying
   *         channel is drained completely. We will wait if more IO is 
   *         required.
   * @throws IOException
   */
  abstract int performIO(ByteBuffer buf) throws IOException;  
  
  /**
   * Performs one IO and returns number of bytes read or written.
   * It waits up to the specified timeout. If the channel is 
   * not read before the timeout, SocketTimeoutException is thrown.
   * 
   * @param buf buffer for IO
   * @param ops Selection Ops used for waiting. Suggested values: 
   *        SelectionKey.OP_READ while reading and SelectionKey.OP_WRITE while
   *        writing. 
   *        
   * @return number of bytes read or written. negative implies end of stream.
   * @throws IOException
   */
  int doIO(ByteBuffer buf, int ops) throws IOException {
    
    /* For now only one thread is allowed. If user want to read or write
     * from multiple threads, multiple streams could be created. In that
     * case multiple threads work as well as underlying channel supports it.
     */
    if (!buf.hasRemaining()) {
      throw new IllegalArgumentException("Buffer has no data left.");
      //or should we just return 0?
    }

    while (buf.hasRemaining()) {
      if (closed) {
        return -1;
      }

      try {
        int n = performIO(buf);
        if (n != 0) {
          // successful io or an error.
          return n;
        }
      } catch (IOException e) {
        if (!channel.isOpen()) {
          closed = true;
        }
        throw e;
      }

      //now wait for socket to be ready.
      int count = 0;
      try {
        count = selector.select(channel, ops, timeout);  
      } catch (IOException e) { //unexpected IOException.
        closed = true;
        throw e;
      } 

      if (count == 0) {
        throw new SocketTimeoutException(timeoutExceptionString(channel,
                                                                timeout, ops));
      }
      // otherwise the socket should be ready for io.
    }
    
    return 0; // does not reach here.
  }
  
  /**
   * The contract is similar to {@link SocketChannel#connect(SocketAddress)} 
   * with a timeout.
   * 
   * @see SocketChannel#connect(SocketAddress)
   * 
   * @param channel - this should be a {@link SelectableChannel}
   * @param endpoint
   * @throws IOException
   */
  static void connect(SocketChannel channel, 
                      SocketAddress endpoint, int timeout) throws IOException {
    
    boolean blockingOn = channel.isBlocking();
    if (blockingOn) {
      channel.configureBlocking(false);
    }
    
    try { 
      if (channel.connect(endpoint)) {
        return;
      }

      long timeoutLeft = timeout;
      long endTime = (timeout > 0) ? (System.currentTimeMillis() + timeout): 0;
      
      while (true) {
        // we might have to call finishConnect() more than once
        // for some channels (with user level protocols)
        
        int ret = selector.select((SelectableChannel)channel, 
                                  SelectionKey.OP_CONNECT, timeoutLeft);
        
        if (ret > 0 && channel.finishConnect()) {
          return;
        }
        
        if (ret == 0 ||
            (timeout > 0 &&  
              (timeoutLeft = (endTime - System.currentTimeMillis())) <= 0)) {
          throw new SocketTimeoutException(
                    timeoutExceptionString(channel, timeout, 
                                           SelectionKey.OP_CONNECT));
        }
      }
    } catch (IOException e) {
      // javadoc for SocketChannel.connect() says channel should be closed.
      try {
        channel.close();
      } catch (IOException ignored) {}
      throw e;
    } finally {
      if (blockingOn && channel.isOpen()) {
        channel.configureBlocking(true);
      }
    }
  }

  /**
   * This is similar to {@link #doIO(ByteBuffer, int)} except that it
   * does not perform any I/O. It just waits for the channel to be ready
   * for I/O as specified in ops.
   * 
   * @param ops Selection Ops used for waiting
   * 
   * @throws SocketTimeoutException 
   *         if select on the channel times out.
   * @throws IOException
   *         if any other I/O error occurs. 
   */
  void waitForIO(int ops) throws IOException {
    
    if (selector.select(channel, ops, timeout) == 0) {
      throw new SocketTimeoutException(timeoutExceptionString(channel, timeout,
                                                              ops)); 
    }
  }

  public void setTimeout(long timeoutMs) {
    this.timeout = timeoutMs;
  }
    
  private static String timeoutExceptionString(SelectableChannel channel,
                                               long timeout, int ops) {
    
    String waitingFor;
    switch(ops) {
    
    case SelectionKey.OP_READ :
      waitingFor = "read"; break;
      
    case SelectionKey.OP_WRITE :
      waitingFor = "write"; break;      
      
    case SelectionKey.OP_CONNECT :
      waitingFor = "connect"; break;
      
    default :
      waitingFor = "" + ops;  
    }
    
    return timeout + " millis timeout while " +
           "waiting for channel to be ready for " + 
           waitingFor + ". ch : " + channel;    
  }
  
  /**
   * This maintains a pool of selectors. These selectors are closed
   * once they are idle (unused) for a few seconds.
   */
  private static class SelectorPool {
    
    private static class SelectorInfo {
      Selector              selector;
      long                  lastActivityTime;
      LinkedList<SelectorInfo> queue; 
      
      void close() {
        if (selector != null) {
          try {
            selector.close();
          } catch (IOException e) {
            LOG.warn("Unexpected exception while closing selector : " +
                     StringUtils.stringifyException(e));
          }
        }
      }    
    }
    
    private static class ProviderInfo {
      SelectorProvider provider;
      LinkedList<SelectorInfo> queue; // lifo
      ProviderInfo next;
    }
    
    private static final long IDLE_TIMEOUT = 10 * 1000; // 10 seconds.
    
    private ProviderInfo providerList = null;
    
    /**
     * Waits on the channel with the given timeout using one of the 
     * cached selectors. It also removes any cached selectors that are
     * idle for a few seconds.
     * 
     * @param channel
     * @param ops
     * @param timeout
     * @return
     * @throws IOException
     */
    int select(SelectableChannel channel, int ops, long timeout) 
                                                   throws IOException {
     
      SelectorInfo info = get(channel);
      
      SelectionKey key = null;
      int ret = 0;
      
      try {
        while (true) {
          long start = (timeout == 0) ? 0 : System.currentTimeMillis();

          key = channel.register(info.selector, ops);
          ret = info.selector.select(timeout);
          
          if (ret != 0) {
            return ret;
          }
          
          /* Sometimes select() returns 0 much before timeout for 
           * unknown reasons. So select again if required.
           */
          if (timeout > 0) {
            timeout -= System.currentTimeMillis() - start;
            if (timeout <= 0) {
              return 0;
            }
          }
          
          if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedIOException("Interruped while waiting for " +
                                             "IO on channel " + channel +
                                             ". " + timeout + 
                                             " millis timeout left.");
          }
        }
      } finally {
        if (key != null) {
          key.cancel();
        }
        
        //clear the canceled key.
        try {
          info.selector.selectNow();
        } catch (IOException e) {
          LOG.info("Unexpected Exception while clearing selector : " +
                   StringUtils.stringifyException(e));
          // don't put the selector back.
          info.close();
          return ret; 
        }
        
        release(info);
      }
    }
    
    /**
     * Takes one selector from end of LRU list of free selectors.
     * If there are no selectors awailable, it creates a new selector.
     * Also invokes trimIdleSelectors(). 
     * 
     * @param channel
     * @return 
     * @throws IOException
     */
    private synchronized SelectorInfo get(SelectableChannel channel) 
                                                         throws IOException {
      SelectorInfo selInfo = null;
      
      SelectorProvider provider = channel.provider();
      
      // pick the list : rarely there is more than one provider in use.
      ProviderInfo pList = providerList;
      while (pList != null && pList.provider != provider) {
        pList = pList.next;
      }      
      if (pList == null) {
        //LOG.info("Creating new ProviderInfo : " + provider.toString());
        pList = new ProviderInfo();
        pList.provider = provider;
        pList.queue = new LinkedList<SelectorInfo>();
        pList.next = providerList;
        providerList = pList;
      }
      
      LinkedList<SelectorInfo> queue = pList.queue;
      
      if (queue.isEmpty()) {
        Selector selector = provider.openSelector();
        selInfo = new SelectorInfo();
        selInfo.selector = selector;
        selInfo.queue = queue;
      } else {
        selInfo = queue.removeLast();
      }
      
      trimIdleSelectors(System.currentTimeMillis());
      return selInfo;
    }
    
    /**
     * puts selector back at the end of LRU list of free selectos.
     * Also invokes trimIdleSelectors().
     * 
     * @param info
     */
    private synchronized void release(SelectorInfo info) {
      long now = System.currentTimeMillis();
      trimIdleSelectors(now);
      info.lastActivityTime = now;
      info.queue.addLast(info);
    }
    
    /**
     * Closes selectors that are idle for IDLE_TIMEOUT (10 sec). It does not
     * traverse the whole list, just over the one that have crossed 
     * the timeout.
     */
    private void trimIdleSelectors(long now) {
      long cutoff = now - IDLE_TIMEOUT;
      
      for(ProviderInfo pList=providerList; pList != null; pList=pList.next) {
        if (pList.queue.isEmpty()) {
          continue;
        }
        for(Iterator<SelectorInfo> it = pList.queue.iterator(); it.hasNext();) {
          SelectorInfo info = it.next();
          if (info.lastActivityTime > cutoff) {
            break;
          }
          it.remove();
          info.close();
        }
      }
    }
  }
}
