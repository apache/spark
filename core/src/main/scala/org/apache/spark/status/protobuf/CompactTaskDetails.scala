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

package org.apache.spark.status.protobuf

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.zip.{Deflater, DeflaterOutputStream, InflaterInputStream}

import org.apache.spark.status.api.v1.AccumulableInfo

/** Immutable cold task payload; the default empty payload needs no allocation. */
private[spark] final class CompactTaskDetails private (
    bytes: Array[Byte],
    compressed: Boolean) {

  private def message(): StoreTypes.TaskDataWrapper = {
    val payload = if (compressed) {
      val in = new InflaterInputStream(new ByteArrayInputStream(bytes))
      try {
        in.readAllBytes()
      } finally {
        in.close()
      }
    } else {
      bytes
    }
    StoreTypes.TaskDataWrapper.parseFrom(payload)
  }

  def decode(): (collection.Seq[AccumulableInfo], Option[String]) = {
    val value = message()
    val error = if (value.hasErrorMessage) Some(value.getErrorMessage) else None
    (AccumulableInfoSerializer.deserialize(value.getAccumulatorUpdatesList).toSeq, error)
  }

  def accumulators(): collection.Seq[AccumulableInfo] = {
    AccumulableInfoSerializer.deserialize(message().getAccumulatorUpdatesList).toSeq
  }

  def error(): Option[String] = {
    val value = message()
    if (value.hasErrorMessage) Some(value.getErrorMessage) else None
  }
}

private[spark] object CompactTaskDetails {
  def apply(
      updates: collection.Seq[AccumulableInfo],
      error: Option[String]): CompactTaskDetails = {
    if (updates.isEmpty && error.isEmpty) {
      return null
    }
    val builder = StoreTypes.TaskDataWrapper.newBuilder()
    updates.foreach(a => builder.addAccumulatorUpdates(AccumulableInfoSerializer.serialize(a)))
    error.foreach(builder.setErrorMessage)
    val payload = builder.build().toByteArray
    if (payload.length >= 256) {
      val deflater = new Deflater(Deflater.BEST_SPEED)
      try {
        val bytes = new ByteArrayOutputStream()
        val out = new DeflaterOutputStream(bytes, deflater)
        try {
          out.write(payload)
        } finally {
          out.close()
        }
        if (bytes.size() + 16 < payload.length) {
          return new CompactTaskDetails(bytes.toByteArray, compressed = true)
        }
      } finally {
        deflater.end()
      }
    }
    new CompactTaskDetails(payload, compressed = false)
  }
}
