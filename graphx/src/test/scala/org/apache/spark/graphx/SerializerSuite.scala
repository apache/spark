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

package org.apache.spark.graphx

import java.io.{EOFException, ByteArrayInputStream, ByteArrayOutputStream}

import scala.util.Random
import scala.reflect.ClassTag

import org.scalatest.FunSuite

import org.apache.spark._
import org.apache.spark.graphx.impl._
import org.apache.spark.serializer.SerializationStream


class SerializerSuite extends FunSuite with LocalSparkContext {

  test("IntAggMsgSerializer") {
    val outMsg = (4: VertexId, 5)
    val bout = new ByteArrayOutputStream
    val outStrm = new IntAggMsgSerializer().newInstance().serializeStream(bout)
    outStrm.writeObject(outMsg)
    outStrm.writeObject(outMsg)
    bout.flush()
    val bin = new ByteArrayInputStream(bout.toByteArray)
    val inStrm = new IntAggMsgSerializer().newInstance().deserializeStream(bin)
    val inMsg1: (VertexId, Int) = inStrm.readObject()
    val inMsg2: (VertexId, Int) = inStrm.readObject()
    assert(outMsg === inMsg1)
    assert(outMsg === inMsg2)

    intercept[EOFException] {
      inStrm.readObject()
    }
  }

  test("LongAggMsgSerializer") {
    val outMsg = (4: VertexId, 1L << 32)
    val bout = new ByteArrayOutputStream
    val outStrm = new LongAggMsgSerializer().newInstance().serializeStream(bout)
    outStrm.writeObject(outMsg)
    outStrm.writeObject(outMsg)
    bout.flush()
    val bin = new ByteArrayInputStream(bout.toByteArray)
    val inStrm = new LongAggMsgSerializer().newInstance().deserializeStream(bin)
    val inMsg1: (VertexId, Long) = inStrm.readObject()
    val inMsg2: (VertexId, Long) = inStrm.readObject()
    assert(outMsg === inMsg1)
    assert(outMsg === inMsg2)

    intercept[EOFException] {
      inStrm.readObject()
    }
  }

  test("DoubleAggMsgSerializer") {
    val outMsg = (4: VertexId, 5.0)
    val bout = new ByteArrayOutputStream
    val outStrm = new DoubleAggMsgSerializer().newInstance().serializeStream(bout)
    outStrm.writeObject(outMsg)
    outStrm.writeObject(outMsg)
    bout.flush()
    val bin = new ByteArrayInputStream(bout.toByteArray)
    val inStrm = new DoubleAggMsgSerializer().newInstance().deserializeStream(bin)
    val inMsg1: (VertexId, Double) = inStrm.readObject()
    val inMsg2: (VertexId, Double) = inStrm.readObject()
    assert(outMsg === inMsg1)
    assert(outMsg === inMsg2)

    intercept[EOFException] {
      inStrm.readObject()
    }
  }

  test("variable long encoding") {
    def testVarLongEncoding(v: Long, optimizePositive: Boolean) {
      val bout = new ByteArrayOutputStream
      val stream = new ShuffleSerializationStream(bout) {
        def writeObject[T: ClassTag](t: T): SerializationStream = {
          writeVarLong(t.asInstanceOf[Long], optimizePositive = optimizePositive)
          this
        }
      }
      stream.writeObject(v)

      val bin = new ByteArrayInputStream(bout.toByteArray)
      val dstream = new ShuffleDeserializationStream(bin) {
        def readObject[T: ClassTag](): T = {
          readVarLong(optimizePositive).asInstanceOf[T]
        }
      }
      val read = dstream.readObject[Long]()
      assert(read === v)
    }

    // Test all variable encoding code path (each branch uses 7 bits, i.e. 1L << 7 difference)
    val d = Random.nextLong() % 128
    Seq[Long](0, 1L << 0 + d, 1L << 7 + d, 1L << 14 + d, 1L << 21 + d, 1L << 28 + d, 1L << 35 + d,
      1L << 42 + d, 1L << 49 + d, 1L << 56 + d, 1L << 63 + d).foreach { number =>
      testVarLongEncoding(number, optimizePositive = false)
      testVarLongEncoding(number, optimizePositive = true)
      testVarLongEncoding(-number, optimizePositive = false)
      testVarLongEncoding(-number, optimizePositive = true)
    }
  }
}
