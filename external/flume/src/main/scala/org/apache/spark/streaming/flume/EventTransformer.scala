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

package org.apache.spark.streaming.flume

import java.io.{ObjectInput, ObjectOutput}

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * A simple object that provides the implementation of readExternal and writeExternal for both
 * the wrapper classes for Flume-style Events.
 */
private[streaming] object EventTransformer extends Logging {
  def readExternal(in: ObjectInput): (java.util.HashMap[CharSequence, CharSequence],
    Array[Byte]) = {
    val bodyLength = in.readInt()
    val bodyBuff = new Array[Byte](bodyLength)
    in.readFully(bodyBuff)

    val numHeaders = in.readInt()
    val headers = new java.util.HashMap[CharSequence, CharSequence]

    for (i <- 0 until numHeaders) {
      val keyLength = in.readInt()
      val keyBuff = new Array[Byte](keyLength)
      in.readFully(keyBuff)
      val key: String = Utils.deserialize(keyBuff)

      val valLength = in.readInt()
      val valBuff = new Array[Byte](valLength)
      in.readFully(valBuff)
      val value: String = Utils.deserialize(valBuff)

      headers.put(key, value)
    }
    (headers, bodyBuff)
  }

  def writeExternal(out: ObjectOutput, headers: java.util.Map[CharSequence, CharSequence],
    body: Array[Byte]) {
    out.writeInt(body.length)
    out.write(body)
    val numHeaders = headers.size()
    out.writeInt(numHeaders)
    for ((k, v) <- headers.asScala) {
      val keyBuff = Utils.serialize(k.toString)
      out.writeInt(keyBuff.length)
      out.write(keyBuff)
      val valBuff = Utils.serialize(v.toString)
      out.writeInt(valBuff.length)
      out.write(valBuff)
    }
  }
}
