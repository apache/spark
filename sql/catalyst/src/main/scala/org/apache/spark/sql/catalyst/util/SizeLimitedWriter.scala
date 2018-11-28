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

package org.apache.spark.sql.catalyst.util

import java.io.Writer

class WriterSizeException(val attemptedSize: Long, val charLimit: Long) extends Exception(
	s"Attempted to write $attemptedSize characters to a writer that is limited to $charLimit")

/**
	* This class is used to control the size of generated writers.  Guarantees that the total number
	* of characters written will be less than the specified size.
	*
	* Checks size before writing and throws a WriterSizeException if the total size would count the limit.
	*/
class SizeLimitedWriter(underlying: Writer, charLimit: Long) extends Writer {

	var charsWritten: Long = 0

	override def write(cbuf: Array[Char], off: Int, len: Int): Unit = {
		val newLength = charsWritten + Math.min(cbuf.length - off, len)
		if(newLength > charLimit) {
			throw new WriterSizeException(newLength, charLimit)
		}
		charsWritten = newLength
		underlying.write(cbuf, off, len)
	}

	override def flush(): Unit = underlying.flush()

	override def close(): Unit = underlying.close()
}
