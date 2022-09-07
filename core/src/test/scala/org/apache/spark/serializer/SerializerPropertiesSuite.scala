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

package org.apache.spark.serializer

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import scala.util.Random

import org.scalatest.Assertions

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.Kryo._
import org.apache.spark.serializer.KryoTest.RegistratorWithoutAutoReset

/**
 * Tests to ensure that [[Serializer]] implementations obey the API contracts for methods that
 * describe properties of the serialized stream, such as
 * `Serializer.supportsRelocationOfSerializedObjects`.
 */
class SerializerPropertiesSuite extends SparkFunSuite {

  import SerializerPropertiesSuite._

  test("JavaSerializer does not support relocation") {
    // Per a comment on the SPARK-4550 JIRA ticket, Java serialization appears to write out the
    // full class name the first time an object is written to an output stream, but subsequent
    // references to the class write a more compact identifier; this prevents relocation.
    val ser = new JavaSerializer(new SparkConf())
    testSupportsRelocationOfSerializedObjects(ser, generateRandomItem)
  }

  test("KryoSerializer supports relocation when auto-reset is enabled") {
    val ser = new KryoSerializer(new SparkConf)
    assert(ser.newInstance().asInstanceOf[KryoSerializerInstance].getAutoReset())
    testSupportsRelocationOfSerializedObjects(ser, generateRandomItem)
  }

  test("KryoSerializer does not support relocation when auto-reset is disabled") {
    val conf = new SparkConf().set(KRYO_USER_REGISTRATORS,
      Seq(classOf[RegistratorWithoutAutoReset].getName))
    val ser = new KryoSerializer(conf)
    assert(!ser.newInstance().asInstanceOf[KryoSerializerInstance].getAutoReset())
    testSupportsRelocationOfSerializedObjects(ser, generateRandomItem)
  }

}

object SerializerPropertiesSuite extends Assertions {

  def generateRandomItem(rand: Random): Any = {
    val randomFunctions: Seq[() => Any] = Seq(
      () => rand.nextInt(),
      () => rand.nextString(rand.nextInt(10)),
      () => rand.nextDouble(),
      () => rand.nextBoolean(),
      () => (rand.nextInt(), rand.nextString(rand.nextInt(10))),
      () => MyCaseClass(rand.nextInt(), rand.nextString(rand.nextInt(10))),
      () => {
        val x = MyCaseClass(rand.nextInt(), rand.nextString(rand.nextInt(10)))
        (x, x)
      }
    )
    randomFunctions(rand.nextInt(randomFunctions.size)).apply()
  }

  def testSupportsRelocationOfSerializedObjects(
      serializer: Serializer,
      generateRandomItem: Random => Any): Unit = {
    if (!serializer.supportsRelocationOfSerializedObjects) {
      return
    }
    val NUM_TRIALS = 5
    val rand = new Random(42)
    for (_ <- 1 to NUM_TRIALS) {
      val items = {
        // Make sure that we have duplicate occurrences of the same object in the stream:
        val randomItems = Seq.fill(10)(generateRandomItem(rand))
        randomItems ++ randomItems.take(5)
      }
      val baos = new ByteArrayOutputStream()
      val serStream = serializer.newInstance().serializeStream(baos)
      def serializeItem(item: Any): Array[Byte] = {
        val itemStartOffset = baos.toByteArray.length
        serStream.writeObject(item)
        serStream.flush()
        val itemEndOffset = baos.toByteArray.length
        baos.toByteArray.slice(itemStartOffset, itemEndOffset).clone()
      }
      val itemsAndSerializedItems: Seq[(Any, Array[Byte])] = {
        val serItems = items.map {
          item => (item, serializeItem(item))
        }
        serStream.close()
        rand.shuffle(serItems)
      }
      val reorderedSerializedData: Array[Byte] = itemsAndSerializedItems.flatMap(_._2).toArray
      val deserializedItemsStream = serializer.newInstance().deserializeStream(
        new ByteArrayInputStream(reorderedSerializedData))
      assert(deserializedItemsStream.asIterator.toSeq === itemsAndSerializedItems.map(_._1))
      deserializedItemsStream.close()
    }
  }
}

private case class MyCaseClass(foo: Int, bar: String)
