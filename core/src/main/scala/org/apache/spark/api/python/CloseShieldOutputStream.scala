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
 * An output stream that forwards writes to `out` but does not close it on [[close()]].
 *
 * Python runners serialize their input into a `DirectByteBufferOutputStream`, whose `close()`
 * frees its native buffer. Arrow writers write that stream through `Channels.newChannel(...)`,
 * an interruptible channel: when another thread interrupts the writing thread inside a channel
 * write (e.g. a task kill with `interruptThread = true`), `Thread.interrupt()` closes the
 * channel, and so the wrapped stream, on the interrupting thread while the writing thread may
 * still be copying into the buffer. Handing the writer this stream keeps the buffer alive; the
 * writing thread still stops with a `ClosedByInterruptException`, and the buffer is freed only
 * by its owner.
 */
private[python] class CloseShieldOutputStream(out: OutputStream) extends FilterOutputStream(out) {

  // FilterOutputStream writes an array one byte at a time.
  override def write(b: Array[Byte], off: Int, len: Int): Unit = out.write(b, off, len)

  override def close(): Unit = flush()
}
