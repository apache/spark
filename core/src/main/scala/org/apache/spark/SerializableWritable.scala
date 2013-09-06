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

package org.apache.spark

import java.io._

import org.apache.hadoop.io.ObjectWritable
import org.apache.hadoop.io.Writable
import org.apache.hadoop.conf.Configuration

class SerializableWritable[T <: Writable](@transient var t: T) extends Serializable {
  def value = t
  override def toString = t.toString

  private def writeObject(out: ObjectOutputStream) {
    out.defaultWriteObject()
    new ObjectWritable(t).write(out)
  }

  private def readObject(in: ObjectInputStream) {
    in.defaultReadObject()
    val ow = new ObjectWritable()
    ow.setConf(new Configuration())
    ow.readFields(in)
    t = ow.get().asInstanceOf[T]
  }
}
