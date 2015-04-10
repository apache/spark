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
package org.apache.spark.util

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import org.scalatest.{FunSuite, Matchers}

import org.apache.spark.SparkConf
import org.apache.spark.serializer.JavaSerializer

class ByteBufferInputStreamSuite extends FunSuite with Matchers {

  val ser = new JavaSerializer(new SparkConf()).newInstance()

  def makeInputStream: ByteBufferInputStream = {
    val out = new ByteArrayOutputStream()
    val serStream = ser.serializeStream(out)
    serStream.writeAll((1 to 10).iterator)

    val bytes = out.toByteArray

    new ByteBufferInputStream(ByteBuffer.wrap(bytes), dispose = true)
  }


  test("dispose when buffer is closed") {
    val in = makeInputStream
    val deser = ser.deserializeStream(in)
    try {
      deser.asIterator.map{x =>
        if (x.asInstanceOf[Int] > 4) throw new UserException()
      }
    } catch {
      case ue: UserException =>
    } finally {
      in.close()
    }

    in.disposed should be (true)
  }

  test("disposed when deserialization stream is read fully") {
    val in = makeInputStream
    val deser = ser.deserializeStream(in)
    deser.asIterator.toArray
    in.disposed should be (true)
  }
}

class UserException extends RuntimeException
