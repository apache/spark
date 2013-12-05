package org.apache.spark.graph

import org.scalatest.FunSuite

import org.apache.spark.SparkContext
import org.apache.spark.graph.LocalSparkContext._
import java.io.{EOFException, ByteArrayInputStream, ByteArrayOutputStream}
import org.apache.spark.graph.impl._
import org.apache.spark.graph.impl.MsgRDDFunctions._
import org.apache.spark._


class SerializerSuite extends FunSuite with LocalSparkContext {

  System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  System.setProperty("spark.kryo.registrator", "org.apache.spark.graph.GraphKryoRegistrator")

  test("IntVertexBroadcastMsgSerializer") {
    val outMsg = new VertexBroadcastMsg[Int](3, 4, 5)
    val bout = new ByteArrayOutputStream
    val outStrm = new IntVertexBroadcastMsgSerializer().newInstance().serializeStream(bout)
    outStrm.writeObject(outMsg)
    outStrm.writeObject(outMsg)
    bout.flush()
    val bin = new ByteArrayInputStream(bout.toByteArray)
    val inStrm = new IntVertexBroadcastMsgSerializer().newInstance().deserializeStream(bin)
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
    val outMsg = new VertexBroadcastMsg[Long](3, 4, 5)
    val bout = new ByteArrayOutputStream
    val outStrm = new LongVertexBroadcastMsgSerializer().newInstance().serializeStream(bout)
    outStrm.writeObject(outMsg)
    outStrm.writeObject(outMsg)
    bout.flush()
    val bin = new ByteArrayInputStream(bout.toByteArray)
    val inStrm = new LongVertexBroadcastMsgSerializer().newInstance().deserializeStream(bin)
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
    val outMsg = new VertexBroadcastMsg[Double](3, 4, 5.0)
    val bout = new ByteArrayOutputStream
    val outStrm = new DoubleVertexBroadcastMsgSerializer().newInstance().serializeStream(bout)
    outStrm.writeObject(outMsg)
    outStrm.writeObject(outMsg)
    bout.flush()
    val bin = new ByteArrayInputStream(bout.toByteArray)
    val inStrm = new DoubleVertexBroadcastMsgSerializer().newInstance().deserializeStream(bin)
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
    val outMsg = (4, 5)
    val bout = new ByteArrayOutputStream
    val outStrm = new IntAggMsgSerializer().newInstance().serializeStream(bout)
    outStrm.writeObject(outMsg)
    outStrm.writeObject(outMsg)
    bout.flush()
    val bin = new ByteArrayInputStream(bout.toByteArray)
    val inStrm = new IntAggMsgSerializer().newInstance().deserializeStream(bin)
    val inMsg1: (Vid, Int) = inStrm.readObject()
    val inMsg2: (Vid, Int) = inStrm.readObject()
    assert(outMsg === inMsg1)
    assert(outMsg === inMsg2)

    intercept[EOFException] {
      inStrm.readObject()
    }
  }

  test("LongAggMsgSerializer") {
    val outMsg = (4, 1L << 32)
    val bout = new ByteArrayOutputStream
    val outStrm = new LongAggMsgSerializer().newInstance().serializeStream(bout)
    outStrm.writeObject(outMsg)
    outStrm.writeObject(outMsg)
    bout.flush()
    val bin = new ByteArrayInputStream(bout.toByteArray)
    val inStrm = new LongAggMsgSerializer().newInstance().deserializeStream(bin)
    val inMsg1: (Vid, Long) = inStrm.readObject()
    val inMsg2: (Vid, Long) = inStrm.readObject()
    assert(outMsg === inMsg1)
    assert(outMsg === inMsg2)

    intercept[EOFException] {
      inStrm.readObject()
    }
  }

  test("DoubleAggMsgSerializer") {
    val outMsg = (4, 5.0)
    val bout = new ByteArrayOutputStream
    val outStrm = new DoubleAggMsgSerializer().newInstance().serializeStream(bout)
    outStrm.writeObject(outMsg)
    outStrm.writeObject(outMsg)
    bout.flush()
    val bin = new ByteArrayInputStream(bout.toByteArray)
    val inStrm = new DoubleAggMsgSerializer().newInstance().deserializeStream(bin)
    val inMsg1: (Vid, Double) = inStrm.readObject()
    val inMsg2: (Vid, Double) = inStrm.readObject()
    assert(outMsg === inMsg1)
    assert(outMsg === inMsg2)

    intercept[EOFException] {
      inStrm.readObject()
    }
  }

  test("TestShuffleVertexBroadcastMsg") {
    withSpark(new SparkContext("local[2]", "test")) { sc =>
      val bmsgs = sc.parallelize(0 until 100, 10).map { pid =>
        new VertexBroadcastMsg[Int](pid, pid, pid)
      }
      bmsgs.partitionBy(new HashPartitioner(3)).collect()
    }
  }
}
