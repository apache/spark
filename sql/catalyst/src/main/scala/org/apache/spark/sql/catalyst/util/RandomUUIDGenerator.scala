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

import java.util.UUID

import org.apache.commons.math3.random.MersenneTwister

import org.apache.spark.unsafe.types.UTF8String

/**
 * This class is used to generate a UUID from Pseudo-Random Numbers.
 *
 * For the algorithm, see RFC 4122: A Universally Unique IDentifier (UUID) URN Namespace,
 * section 4.4 "Algorithms for Creating a UUID from Truly Random or Pseudo-Random Numbers".
 */
case class RandomUUIDGenerator(randomSeed: Long) {
  private val random = new MersenneTwister(randomSeed)

  def getNextUUID(): UUID = {
    val mostSigBits = (random.nextLong() & 0xFFFFFFFFFFFF0FFFL) | 0x0000000000004000L
    val leastSigBits = (random.nextLong() | 0x8000000000000000L) & 0xBFFFFFFFFFFFFFFFL

    new UUID(mostSigBits, leastSigBits)
  }

  def getNextUUIDUTF8String(): UTF8String = UTF8String.fromString(getNextUUID().toString())
}
