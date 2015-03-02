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

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.apache.spark.network.buffer.WrappedLargeByteBuffer
import org.apache.spark.util.{LargeByteBufferInputStream, LargeByteBufferOutputStream}
import org.scalatest.{Matchers, FunSuite}

class LargeByteBufferTest extends FunSuite with Matchers {

//  test("allocateOnHeap") {
//    val bufs = LargeByteBuffer.allocateOnHeap(10, 3).asInstanceOf[ChainedLargeByteBuffer]
//    bufs.underlying.foreach{buf => buf.capacity should be <= 3}
//    bufs.underlying.map{_.capacity}.sum should be (10)
//  }
//
//  test("allocate large") {
//    val size = Integer.MAX_VALUE.toLong + 10
//    val bufs = LargeByteBuffer.allocateOnHeap(size, 1e9.toInt).asInstanceOf[WrappedLargeByteBuffer]
//    bufs.capacity should be (size)
//    bufs.underlying.map{_.capacity.toLong}.sum should be (Integer.MAX_VALUE.toLong + 10)
//  }


  test("io stream roundtrip") {

    val rawOut = new LargeByteBufferOutputStream(128)
    val objOut = new ObjectOutputStream(rawOut)
    val someObject = (1 to 100).map{x => x -> scala.util.Random.nextInt(x)}.toMap
    objOut.writeObject(someObject)
    objOut.close()

    rawOut.largeBuffer.asInstanceOf[WrappedLargeByteBuffer].underlying.size should be > 1

    val rawIn = new LargeByteBufferInputStream(rawOut.largeBuffer)
    val objIn = new ObjectInputStream(rawIn)
    val deser = objIn.readObject()
    deser should be (someObject)
  }

}
