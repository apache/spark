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

package org.apache.spark.util.io

import java.io.InputStream


/**
 * An InputStream that decorator that limits the number of bytes returned from the underlying
 * InputStream. If the underlying InputStream returns end of stream (-1 value in read) before
 * the limit is reached, then the end of stream is properly returned to the caller as well.
 *
 * One use case is to read a segment of a file (FileInputStream always reads until the end of a
 * file, whereas caller can use this one to limit it to stop before the end of the file).
 *
 * @param underlying Underlying InputStream to read from
 * @param limit number of bytes to read before returning EOF
 */
final class LengthBoundedInputStream(underlying: InputStream, limit: Long) extends InputStream {

  private[this] var pos: Long = 0L

  override def close(): Unit = underlying.close()

  override def read(): Int = {
    if (pos >= limit) {
      -1
    } else {
      val buf = underlying.read()
      pos += 1
      buf
    }
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    if (pos >= limit) {
      -1
    } else {
      val bytesRead = underlying.read(b, off, math.min(limit - pos, len).toInt)
      if (bytesRead == -1) {
        -1
      } else {
        pos += bytesRead
        bytesRead
      }
    }
  }

  override def available(): Int = throw new UnsupportedOperationException

  override def reset(): Unit = throw new UnsupportedOperationException

  override def skip(n: Long): Long = throw new UnsupportedOperationException
}
