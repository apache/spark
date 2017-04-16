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

package org.apache.hadoop.hdfs;

import java.net.Socket;
import java.net.SocketAddress;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.thirdparty.guava.common.base.Preconditions;
import org.apache.hadoop.thirdparty.guava.common.collect.LinkedListMultimap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IOUtils;

/**
 * A cache of sockets.
 */
class SocketCache {
  static final Log LOG = LogFactory.getLog(SocketCache.class);

  private final LinkedListMultimap<SocketAddress, Socket> multimap;
  private final int capacity;

  /**
   * Create a SocketCache with the given capacity.
   * @param capacity  Max cache size.
   */
  public SocketCache(int capacity) {
    multimap = LinkedListMultimap.create();
    this.capacity = capacity;
  }

  /**
   * Get a cached socket to the given address.
   * @param remote  Remote address the socket is connected to.
   * @return  A socket with unknown state, possibly closed underneath. Or null.
   */
  public synchronized Socket get(SocketAddress remote) {
    List<Socket> socklist = multimap.get(remote);
    if (socklist == null) {
      return null;
    }

    Iterator<Socket> iter = socklist.iterator();
    while (iter.hasNext()) {
      Socket candidate = iter.next();
      iter.remove();
      if (!candidate.isClosed()) {
        return candidate;
      }
    }
    return null;
  }

  /**
   * Give an unused socket to the cache.
   * @param sock socket not used by anyone.
   */
  public synchronized void put(Socket sock) {
    Preconditions.checkNotNull(sock);

    SocketAddress remoteAddr = sock.getRemoteSocketAddress();
    if (remoteAddr == null) {
      LOG.warn("Cannot cache (unconnected) socket with no remote address: " +
               sock);
      IOUtils.closeSocket(sock);
      return;
    }

    if (capacity == multimap.size()) {
      evictOldest();
    }
    multimap.put(remoteAddr, sock);
  }

  public synchronized int size() {
    return multimap.size();
  }

  /**
   * Evict the oldest entry in the cache.
   */
  private synchronized void evictOldest() {
    Iterator<Entry<SocketAddress, Socket>> iter =
      multimap.entries().iterator();
    if (!iter.hasNext()) {
      throw new IllegalStateException("Cannot evict from empty cache!");
    }
    Entry<SocketAddress, Socket> entry = iter.next();
    iter.remove();
    Socket sock = entry.getValue();
    IOUtils.closeSocket(sock);
  }

  /**
   * Empty the cache, and close all sockets.
   */
  public synchronized void clear() {
    for (Socket sock : multimap.values()) {
      IOUtils.closeSocket(sock);
    }
    multimap.clear();
  }

  protected void finalize() {
    clear();
  }

}
