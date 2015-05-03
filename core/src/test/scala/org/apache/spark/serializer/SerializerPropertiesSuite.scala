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

import org.scalatest.FunSuite

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoTest.RegistratorWithoutAutoReset

private case class MyCaseClass(foo: Int, bar: String)

class SerializerPropertiesSuite extends FunSuite {

  test("JavaSerializer does not support relocation") {
    testSupportsRelocationOfSerializedObjects(new JavaSerializer(new SparkConf()))
  }

  test("KryoSerializer supports relocation when auto-reset is enabled") {
    val ser = new KryoSerializer(new SparkConf)
    assert(ser.newInstance().asInstanceOf[KryoSerializerInstance].getAutoReset())
    testSupportsRelocationOfSerializedObjects(ser)
  }

  test("KryoSerializer does not support relocation when auto-reset is disabled") {
    val conf = new SparkConf().set("spark.kryo.registrator",
      classOf[RegistratorWithoutAutoReset].getName)
    val ser = new KryoSerializer(conf)
    assert(!ser.newInstance().asInstanceOf[KryoSerializerInstance].getAutoReset())
    testSupportsRelocationOfSerializedObjects(ser)
  }

  def testSupportsRelocationOfSerializedObjects(serializer: Serializer): Unit = {
    val NUM_TRIALS = 100
    if (!serializer.supportsRelocationOfSerializedObjects) {
      return
    }
    val rand = new Random(42)
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
    def generateRandomItem(): Any = {
      randomFunctions(rand.nextInt(randomFunctions.size)).apply()
    }

    for (_ <- 1 to NUM_TRIALS) {
      val items = {
        // Make sure that we have duplicate occurrences of the same object in the stream:
        val randomItems = Seq.fill(10)(generateRandomItem())
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
