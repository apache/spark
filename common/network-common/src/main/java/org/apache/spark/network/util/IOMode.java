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

package org.apache.spark.network.util;

/**
 * Selector for which form of low-level IO we should use.
 */
public enum IOMode {
  /**
   * Java NIO (Selector), cross-platform portable
   */
  NIO,
  /**
   * Native EPOLL via JNI, Linux only
   */
  EPOLL,
  /**
   * Native KQUEUE via JNI, MacOS/BSD only
   */
  KQUEUE,
  /**
   * Native io_uring via JNI, Linux only. Requires kernel 5.10+.
   */
  IO_URING,
  /**
   * Prefer to use a native transport when available. On Linux, io_uring is preferred over EPOLL
   * when the running kernel supports it AND the JVM can actually allocate an io_uring ring
   * (probed once via {@link NettyUtils#isIoUringUsable()}; environments with low
   * {@code RLIMIT_MEMLOCK} fall through to EPOLL). On MacOS/BSD, KQUEUE is used. Falls back to
   * NIO when no native transport is available.
   */
  AUTO
}
