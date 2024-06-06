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

package org.apache.spark.util.collection

private object ErrorMessage {
  final val msg: String = "mutable operation is not supported"
}

// An immutable BitSet that initializes set bits in its constructor.
class ImmutableBitSet(val numBits: Int, val bitsToSet: Int*) extends BitSet(numBits) {

  // Initialize the set bits.
  {
    val bitsIterator = bitsToSet.iterator
    while (bitsIterator.hasNext) {
      super.set(bitsIterator.next())
    }
  }

  override def clear(): Unit = {
    throw new UnsupportedOperationException(ErrorMessage.msg)
  }

  override def clearUntil(bitIndex: Int): Unit = {
    throw new UnsupportedOperationException(ErrorMessage.msg)
  }

  override def set(index: Int): Unit = {
    throw new UnsupportedOperationException(ErrorMessage.msg)
  }

  override def setUntil(bitIndex: Int): Unit = {
    throw new UnsupportedOperationException(ErrorMessage.msg)
  }

  override def unset(index: Int): Unit = {
    throw new UnsupportedOperationException(ErrorMessage.msg)
  }

  override def union(other: BitSet): Unit = {
    throw new UnsupportedOperationException(ErrorMessage.msg)
  }
}
