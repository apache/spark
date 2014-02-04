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

import org.scalatest.FunSuite

import org.apache.spark._
import org.apache.spark.graphx.impl._
import org.apache.spark.graphx.impl.MsgRDDFunctions._
import org.apache.spark.serializer.SerializationStream


class SerializerSuite extends FunSuite with LocalSparkContext {

  test("IntVertexBroadcastMsgSerializer") {
    val conf = new SparkConf(false)
    val outMsg = new VertexBroadcastMsg[Int](3, 4, 5)
    val bout = new ByteArrayOutputStream
    val outStrm = new IntVertexBroadcastMsgSerializer(conf).newInstance().serializeStream(bout)
    outStrm.writeObject(outMsg)
    outStrm.writeObject(outMsg)
    bout.flush()
    val bin = new ByteArrayInputStream(bout.toByteArray)
    val inStrm = new IntVertexBroadcastMsgSerializer(conf).newInstance().deserializeStream(bin)
    val inMsg1: VertexBroadcastMsg[Int] = inStrm.readObject()
    val inMsg2: VertexBroadcastMsg[Int] = inStrm.readObject()
    assert(outMsg.vid === inMsg1.vid)
    assert(outMsg.vid === inMsg2.vid)
    assert(outMsg.data === inMsg1.data)
    assert(outMsg.data === inMsg2.data)

    intercept[EOFException] {
      inStrm.readObject()
    }
  }

  test("LongVertexBroadcastMsgSerializer") {
    val conf = new SparkConf(false)
    val outMsg = new VertexBroadcastMsg[Long](3, 4, 5)
    val bout = new ByteArrayOutputStream
    val outStrm = new LongVertexBroadcastMsgSerializer(conf).newInstance().serializeStream(bout)
    outStrm.writeObject(outMsg)
    outStrm.writeObject(outMsg)
    bout.flush()
    val bin = new ByteArrayInputStream(bout.toByteArray)
    val inStrm = new LongVertexBroadcastMsgSerializer(conf).newInstance().deserializeStream(bin)
    val inMsg1: VertexBroadcastMsg[Long] = inStrm.readObject()
    val inMsg2: VertexBroadcastMsg[Long] = inStrm.readObject()
    assert(outMsg.vid === inMsg1.vid)
    assert(outMsg.vid === inMsg2.vid)
    assert(outMsg.data === inMsg1.data)
    assert(outMsg.data === inMsg2.data)

    intercept[EOFException] {
      inStrm.readObject()
    }
  }

  test("DoubleVertexBroadcastMsgSerializer") {
    val conf = new SparkConf(false)
    val outMsg = new VertexBroadcastMsg[Double](3, 4, 5.0)
    val bout = new ByteArrayOutputStream
    val outStrm = new DoubleVertexBroadcastMsgSerializer(conf).newInstance().serializeStream(bout)
    outStrm.writeObject(outMsg)
    outStrm.writeObject(outMsg)
    bout.flush()
    val bin = new ByteArrayInputStream(bout.toByteArray)
    val inStrm = new DoubleVertexBroadcastMsgSerializer(conf).newInstance().deserializeStream(bin)
    val inMsg1: VertexBroadcastMsg[Double] = inStrm.readObject()
    val inMsg2: VertexBroadcastMsg[Double] = inStrm.readObject()
    assert(outMsg.vid === inMsg1.vid)
    assert(outMsg.vid === inMsg2.vid)
    assert(outMsg.data === inMsg1.data)
    assert(outMsg.data === inMsg2.data)

    intercept[EOFException] {
      inStrm.readObject()
    }
  }

  test("IntAggMsgSerializer") {
    val conf = new SparkConf(false)
    val outMsg = (4: VertexId, 5)
    val bout = new ByteArrayOutputStream
    val outStrm = new IntAggMsgSerializer(conf).newInstance().serializeStream(bout)
    outStrm.writeObject(outMsg)
    outStrm.writeObject(outMsg)
    bout.flush()
    val bin = new ByteArrayInputStream(bout.toByteArray)
    val inStrm = new IntAggMsgSerializer(conf).newInstance().deserializeStream(bin)
    val inMsg1: (VertexId, Int) = inStrm.readObject()
    val inMsg2: (VertexId, Int) = inStrm.readObject()
    assert(outMsg === inMsg1)
    assert(outMsg === inMsg2)

    intercept[EOFException] {
      inStrm.readObject()
    }
  }

  test("LongAggMsgSerializer") {
    val conf = new SparkConf(false)
    val outMsg = (4: VertexId, 1L << 32)
    val bout = new ByteArrayOutputStream
    val outStrm = new LongAggMsgSerializer(conf).newInstance().serializeStream(bout)
    outStrm.writeObject(outMsg)
    outStrm.writeObject(outMsg)
    bout.flush()
    val bin = new ByteArrayInputStream(bout.toByteArray)
    val inStrm = new LongAggMsgSerializer(conf).newInstance().deserializeStream(bin)
    val inMsg1: (VertexId, Long) = inStrm.readObject()
    val inMsg2: (VertexId, Long) = inStrm.readObject()
    assert(outMsg === inMsg1)
    assert(outMsg === inMsg2)

    intercept[EOFException] {
      inStrm.readObject()
    }
  }

  test("DoubleAggMsgSerializer") {
    val conf = new SparkConf(false)
    val outMsg = (4: VertexId, 5.0)
    val bout = new ByteArrayOutputStream
    val outStrm = new DoubleAggMsgSerializer(conf).newInstance().serializeStream(bout)
    outStrm.writeObject(outMsg)
    outStrm.writeObject(outMsg)
    bout.flush()
    val bin = new ByteArrayInputStream(bout.toByteArray)
    val inStrm = new DoubleAggMsgSerializer(conf).newInstance().deserializeStream(bin)
    val inMsg1: (VertexId, Double) = inStrm.readObject()
    val inMsg2: (VertexId, Double) = inStrm.readObject()
    assert(outMsg === inMsg1)
    assert(outMsg === inMsg2)

    intercept[EOFException] {
      inStrm.readObject()
    }
  }

  test("TestShuffleVertexBroadcastMsg") {
    withSpark { sc =>
      val bmsgs = sc.parallelize(0 until 100, 10).map { pid =>
        new VertexBroadcastMsg[Int](pid, pid, pid)
      }
      bmsgs.partitionBy(new HashPartitioner(3)).collect()
    }
  }

  test("variable long encoding") {
    def testVarLongEncoding(v: Long, optimizePositive: Boolean) {
      val bout = new ByteArrayOutputStream
      val stream = new ShuffleSerializationStream(bout) {
        def writeObject[T](t: T): SerializationStream = {
          writeVarLong(t.asInstanceOf[Long], optimizePositive = optimizePositive)
          this
        }
      }
      stream.writeObject(v)

      val bin = new ByteArrayInputStream(bout.toByteArray)
      val dstream = new ShuffleDeserializationStream(bin) {
        def readObject[T](): T = {
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
