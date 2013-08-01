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

package spark.util

import java.io.InputStream

/**
 * Nested input stream: insert nested streams into outer streams without closing them.
 * <p>
 * This stream doesn't really provide its own nested envelope and does not put an EOF marker. It is
 * still a responsibility of outer code to write a stop token. In fact, the only purpose of this
 * wrapper stream is to prevent outer stream (such as JavaSerializer's stream) from closing the
 * output.
 */
class NestedInputStream(val in: InputStream) extends InputStream {
  override def read(): Int = in.read()
  override def read(b: Array[Byte], off: Int, len: Int): Int = in.read(b, off, len)
}
