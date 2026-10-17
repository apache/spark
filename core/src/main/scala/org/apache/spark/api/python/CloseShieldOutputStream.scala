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

package org.apache.spark.api.python

import java.io.{FilterOutputStream, OutputStream}

/**
 * An output stream that writes through to `out` but does not close it.
 *
 * The Arrow writers write to the Python worker through `Channels.newChannel(dataOut)`, an
 * interruptible channel. When another thread interrupts the writing thread during a write, the
 * JDK closes that channel, and with it `dataOut`, on the interrupting thread. If `dataOut` frees
 * its buffer on close, as `DirectByteBufferOutputStream` does, the writing thread then keeps
 * copying into freed memory until the write returns. Writing through this stream leaves the
 * buffer to be freed only by its owner. The writing thread still stops, since the closed channel
 * throws `ClosedByInterruptException`.
 */
private[python] class CloseShieldOutputStream(out: OutputStream) extends FilterOutputStream(out) {

  override def write(b: Array[Byte], off: Int, len: Int): Unit = out.write(b, off, len)

  override def close(): Unit = flush()
}
