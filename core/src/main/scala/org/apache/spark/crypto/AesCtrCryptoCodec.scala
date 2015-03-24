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
package org.apache.spark.crypto

import com.google.common.base.Preconditions
import scala.Byte

/**
 * For AES, the algorithm block is fixed size of 128 bits.
 * @see http://en.wikipedia.org/wiki/Advanced_Encryption_Standard
 */
abstract class AesCtrCryptoCodec extends CryptoCodec {
  val CTR_OFFSET: Integer = 8
  val SUITE: CipherSuite = AES_CTR_NOPADDING
  val AES_BLOCK_SIZE: Integer = SUITE.algoBlockSize
  override def getCipherSuite(): CipherSuite = {
    SUITE
  }

  override def calculateIV(initIV: Array[Byte], counterVal: Long, IV: Array[Byte]) = {
    var counter: Long = counterVal
    Preconditions.checkArgument(initIV.length == AES_BLOCK_SIZE)
    Preconditions.checkArgument(IV.length == AES_BLOCK_SIZE)
    var i: Int = IV.length - 1
    var j: Int = 1
    var sum: Int = 0
    val SIZE: Int = 8
    while (i > 0) {
      sum = (initIV(i) & 0xff) + (sum >>> SIZE)
      if (j < 8) {
        sum += counter.asInstanceOf[Byte] & 0xff
        counter = counter >>> 8
      }
      j = j + 1
      IV(i) = sum.asInstanceOf[Byte]
      i = i - 1
    }
  }
}

