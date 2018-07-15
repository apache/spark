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

package org.apache.spark.sql.avro

import java.io._

import scala.util.control.NonFatal

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.avro.Schema
import org.slf4j.LoggerFactory

class SerializableSchema(@transient var value: Schema)
  extends Serializable with KryoSerializable {

  @transient private[avro] lazy val log = LoggerFactory.getLogger(getClass)

    private def writeObject(out: ObjectOutputStream): Unit = tryOrIOException {
      out.defaultWriteObject()
      out.writeUTF(value.toString())
      out.flush()
    }

    private def readObject(in: ObjectInputStream): Unit = tryOrIOException {
      val json = in.readUTF()
      value = new Schema.Parser().parse(json)
    }

    private def tryOrIOException[T](block: => T): T = {
      try {
        block
      } catch {
        case e: IOException =>
          log.error("Exception encountered", e)
          throw e
        case NonFatal(e) =>
          log.error("Exception encountered", e)
          throw new IOException(e)
      }
    }

    def write(kryo: Kryo, out: Output): Unit = {
      val dos = new DataOutputStream(out)
      dos.writeUTF(value.toString())
      dos.flush()
    }

    def read(kryo: Kryo, in: Input): Unit = {
      val dis = new DataInputStream(in)
      val json = dis.readUTF()
      value = new Schema.Parser().parse(json)
    }
}
