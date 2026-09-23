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
package org.apache.spark.sql.connect.common

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InvalidClassException}
import java.io.{ObjectInputStream, ObjectOutputStream, ObjectStreamClass, ObjectStreamConstants}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import com.google.protobuf.ByteString

import org.apache.spark.SparkFunSuite
import org.apache.spark.connect.proto
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.PrimitiveIntEncoder
import org.apache.spark.sql.connect.SparkConnectTestUtils
import org.apache.spark.sql.connect.common.UdfSerialization.SuidTransition
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{SuidCompatV1, SuidCompatV2, SuidCustomV1, SuidCustomV2}
import org.apache.spark.sql.types.{SuidExplicitV1, SuidExplicitV2, SuidLayoutV1, SuidLayoutV2}
import org.apache.spark.sql.types.{SuidProducerCustomV1, SuidProducerCustomV2}
import org.apache.spark.sql.types.{SuidProducerExplicitV1, SuidProducerExplicitV2}
import org.apache.spark.util.Utils

/**
 * Tests for [[UdfSerialization]]'s tolerance of audited `serialVersionUID` changes of
 * `org.apache.spark.sql.types` classes.
 *
 * The fixture pairs have same-length class names, so serializing a `*V1` instance and patching
 * the class name in the stream to `*V2` yields what a `*V2` consumer sees from a `*V1` producer
 * with a different `serialVersionUID`, without needing two builds.
 */
class UdfSerializationSuite extends SparkFunSuite with SharedSparkSession {

  private val loader = getClass.getClassLoader

  private def serialize(o: AnyRef): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
    bos.toByteArray
  }

  private def plainDeserialize(bytes: Array[Byte]): AnyRef =
    new ObjectInputStream(new ByteArrayInputStream(bytes)).readObject()

  private def suid(cls: Class[_]): Long = ObjectStreamClass.lookup(cls).getSerialVersionUID

  /** Serialize `producer` and rename its class in the stream to `consumer`. */
  private def drifted(producer: AnyRef, consumer: Class[_]): Array[Byte] = {
    val from = producer.getClass.getName.getBytes(StandardCharsets.UTF_8)
    val to = consumer.getName.getBytes(StandardCharsets.UTF_8)
    assert(from.length == to.length, "class names must have the same length")
    val bytes = serialize(producer)
    val idx = bytes.indexOfSlice(from)
    assert(idx >= 0, s"${producer.getClass.getName} not found in the stream")
    val patched = bytes.clone()
    System.arraycopy(to, 0, patched, idx, to.length)
    patched
  }

  /** The transition that lets a `consumer` reader accept a stream produced by `producer`. */
  private def transition(producer: Class[_], consumer: Class[_]): SuidTransition =
    SuidTransition(consumer.getName, suid(producer), suid(consumer))

  test("accepts an audited serialVersionUID change (bytes overload)") {
    val stream = drifted(SuidCompatV1(7, "hi"), classOf[SuidCompatV2])
    // A plain ObjectInputStream rejects the SUID mismatch: this is the failure being fixed.
    intercept[InvalidClassException](plainDeserialize(stream))
    val audited = Set(transition(classOf[SuidCompatV1], classOf[SuidCompatV2]))
    assert(
      UdfSerialization
        .deserialize[SuidCompatV2](stream, loader, audited) === SuidCompatV2(7, "hi"))
  }

  test("accepts an audited serialVersionUID change (InputStream overload)") {
    val stream = drifted(SuidCompatV1(9, "yo"), classOf[SuidCompatV2])
    val audited = Set(transition(classOf[SuidCompatV1], classOf[SuidCompatV2]))
    val result =
      UdfSerialization.deserialize[SuidCompatV2](new ByteArrayInputStream(stream), audited)
    assert(result === SuidCompatV2(9, "yo"))
  }

  test("rejects a serialVersionUID change that is not audited") {
    val stream = drifted(SuidCompatV1(1, "x"), classOf[SuidCompatV2])
    intercept[InvalidClassException](
      UdfSerialization.deserialize[AnyRef](stream, loader, Set.empty[SuidTransition]))
    intercept[InvalidClassException](UdfSerialization.deserialize[AnyRef](stream, loader))
  }

  test("rejects an audited change when the field layout differs") {
    val stream = drifted(SuidLayoutV1(1, "x"), classOf[SuidLayoutV2])
    val audited = Set(transition(classOf[SuidLayoutV1], classOf[SuidLayoutV2]))
    intercept[InvalidClassException](
      UdfSerialization.deserialize[AnyRef](stream, loader, audited))
  }

  test("rejects an audited change when the consumer declares a serialVersionUID") {
    val stream = drifted(SuidExplicitV1(1, "x"), classOf[SuidExplicitV2])
    val audited = Set(transition(classOf[SuidExplicitV1], classOf[SuidExplicitV2]))
    intercept[InvalidClassException](
      UdfSerialization.deserialize[AnyRef](stream, loader, audited))
  }

  test("rejects an audited change when the consumer has a custom readObject") {
    val stream = drifted(SuidCustomV1(1, "x"), classOf[SuidCustomV2])
    val audited = Set(transition(classOf[SuidCustomV1], classOf[SuidCustomV2]))
    intercept[InvalidClassException](
      UdfSerialization.deserialize[AnyRef](stream, loader, audited))
  }

  test("rejects an unaudited change from a producer with a custom writeObject") {
    val stream = drifted(SuidProducerCustomV1(1, "x"), classOf[SuidProducerCustomV2])
    intercept[InvalidClassException](UdfSerialization.deserialize[AnyRef](stream, loader))
  }

  test("rejects an unaudited change from a producer that declares a serialVersionUID") {
    val stream = drifted(SuidProducerExplicitV1(1, "x"), classOf[SuidProducerExplicitV2])
    intercept[InvalidClassException](UdfSerialization.deserialize[AnyRef](stream, loader))
  }

  test("rejects an audited change outside sql.types") {
    val stream = drifted(SuidUntolerantV1(1, "x"), classOf[SuidUntolerantV2])
    val audited = Set(transition(classOf[SuidUntolerantV1], classOf[SuidUntolerantV2]))
    intercept[InvalidClassException](
      UdfSerialization.deserialize[AnyRef](stream, loader, audited))
  }

  test("audited transitions target this build") {
    assert(UdfSerialization.auditedTransitions.nonEmpty)
    UdfSerialization.auditedTransitions.foreach { t =>
      assert(t.className.startsWith("org.apache.spark.sql.types."), t)
      assert(t.streamSuid != t.localSuid, t)
      val cls = Utils.classForName(t.className)
      assert(
        suid(cls) == t.localSuid,
        s"The serialVersionUID of ${t.className} is now ${suid(cls)}, not ${t.localSuid}. " +
          "Re-audit its entries in UdfSerialization.auditedTransitions against the released " +
          "versions before updating localSuid.")
      assert(UdfSerialization.isRebindSafe(cls), t)
    }
  }

  // An audited production transition of a Scala object. The object is serialized through its
  // ModuleSerializationProxy, whose Class field writes the object's class descriptor, and hence
  // its serialVersionUID, into the stream.
  private lazy val objectTransition: SuidTransition =
    UdfSerialization.auditedTransitions.toSeq
      .sortBy(_.className)
      .find(_.className.endsWith("$"))
      .getOrElse(fail("expected an audited transition of a Scala object"))

  /** A UdfPacket referencing `t`'s object, as produced by a build with `t.streamSuid`. */
  private def driftedUdfPacket(t: SuidTransition): (AnyRef, Array[Byte]) = {
    val obj = Utils.classForName(t.className).getField("MODULE$").get(null)
    val bytes = UdfPacket(obj, Seq.empty, PrimitiveIntEncoder).toByteString.toByteArray
    val name = t.className.getBytes(StandardCharsets.UTF_8)
    val header =
      Array(ObjectStreamConstants.TC_CLASSDESC, (name.length >> 8).toByte, name.length.toByte) ++
        name
    val idx = bytes.indexOfSlice(header)
    assert(idx >= 0, s"no class descriptor for ${t.className} in the stream")
    val patched = ByteBuffer.wrap(bytes.clone())
    val suidOffset = idx + header.length
    assert(patched.getLong(suidOffset) == t.localSuid)
    patched.putLong(suidOffset, t.streamSuid)
    (obj, patched.array())
  }

  test("UdfPacket.apply accepts an audited serialVersionUID change") {
    val (obj, bytes) = driftedUdfPacket(objectTransition)
    intercept[InvalidClassException](plainDeserialize(bytes))
    assert(UdfPacket(ByteString.copyFrom(bytes)).function eq obj)
  }

  test("SparkConnectPlanner.unpackScalaUDF accepts an audited serialVersionUID change") {
    val (obj, bytes) = driftedUdfPacket(objectTransition)
    val planner =
      new SparkConnectPlanner(SparkConnectTestUtils.createDummySessionHolder(spark))
    val udf = proto.ScalarScalaUDF.newBuilder().setPayload(ByteString.copyFrom(bytes)).build()
    assert(planner.unpackScalaUDF[UdfPacket](udf).function eq obj)
  }
}
