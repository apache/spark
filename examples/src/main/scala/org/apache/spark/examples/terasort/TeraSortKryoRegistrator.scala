/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.terasort

import java.io.{DataInputStream, DataOutputStream}

import com.esotericsoftware.kryo.{Kryo, Serializer => KSerializer}
import com.esotericsoftware.kryo.io.{Input => KryoInput, Output => KryoOutput}

import org.apache.hadoop.io.{BytesWritable, Writable}

import org.apache.spark.serializer.KryoRegistrator


class TeraSortKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[BytesWritable])
    kryo.setReferences(false)
  }
}


/** A Kryo serializer for Hadoop writables. */
class KryoWritableSerializer[T <: Writable] extends KSerializer[T] {
  override def write(kryo: Kryo, output: KryoOutput, writable: T) {
    val ouputStream = new DataOutputStream(output)
    writable.write(ouputStream)
  }

  override def read(kryo: Kryo, input: KryoInput, cls: java.lang.Class[T]): T = {
    val writable = cls.newInstance()
    val inputStream = new DataInputStream(input)
    writable.readFields(inputStream)
    writable
  }
}
