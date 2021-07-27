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

package org.apache.spark.io

import java.io.OutputStream
import java.util.zip.Checksum

/**
 * A variant of [[java.util.zip.CheckedOutputStream]] which can
 * change the checksum calculator at runtime.
 */
class MutableCheckedOutputStream(out: OutputStream) extends OutputStream {
  private var checksum: Checksum = _

  def setChecksum(c: Checksum): Unit = {
    this.checksum = c
  }

  override def write(b: Int): Unit = {
    assert(checksum != null, "Checksum is not set.")
    checksum.update(b)
    out.write(b)
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    assert(checksum != null, "Checksum is not set.")
    checksum.update(b, off, len)
    out.write(b, off, len)
  }

  override def flush(): Unit = out.flush()

  override def close(): Unit = out.close()
}
