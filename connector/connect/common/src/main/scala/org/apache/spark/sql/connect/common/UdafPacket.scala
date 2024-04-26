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

import java.io.{InputStream, ObjectInputStream, ObjectOutputStream, OutputStream}

import com.google.protobuf.ByteString

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder

/**
 * A wrapper class around the UDF and it's Input/Output [[AgnosticEncoder]](s).
 *
 * This class is shared between the client and the server to allow for serialization and
 * deserialization of the JVM object.
 *
 * @param function
 *   The UDF
 * @param inputEncoders
 *   A list of [[AgnosticEncoder]](s) for all input arguments of the UDF
 * @param outputEncoder
 *   An [[AgnosticEncoder]] for the output of the UDF
 */
@SerialVersionUID(5082833725037298050L)
case class UdafPacket(
    aggregator: AnyRef,
    inputEncoder: AgnosticEncoder[_])
    extends Serializable {

  def writeTo(out: OutputStream): Unit = {
    val oos = new ObjectOutputStream(out)
    oos.writeObject(this)
    oos.flush()
  }

  def toByteString: ByteString = {
    val out = ByteString.newOutput()
    writeTo(out)
    out.toByteString
  }
}

object UdafPacket {
  def apply(in: InputStream): UdafPacket = {
    val ois = new ObjectInputStream(in)
    ois.readObject().asInstanceOf[UdafPacket]
  }

  def apply(bytes: ByteString): UdafPacket = {
    val in = bytes.newInput()
    try UdafPacket(in)
    finally {
      in.close()
    }
  }
}
