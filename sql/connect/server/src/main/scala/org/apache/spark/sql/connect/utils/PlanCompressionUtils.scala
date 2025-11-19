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

package org.apache.spark.sql.connect.utils

import java.io.IOException

import scala.util.control.NonFatal

import com.github.luben.zstd.{Zstd, ZstdInputStreamNoFinalizer}
import com.google.protobuf.{ByteString, CodedInputStream}
import org.apache.commons.io.input.BoundedInputStream

import org.apache.spark.{SparkEnv, SparkSQLException}
import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.planner.InvalidInputErrors

object PlanCompressionUtils {
  def decompressPlan(plan: proto.Plan): proto.Plan = {
    plan.getOpTypeCase match {
      case proto.Plan.OpTypeCase.COMPRESSED_OPERATION =>
        val (cis, closeStream) = decompressBytes(
          plan.getCompressedOperation.getData,
          plan.getCompressedOperation.getCompressionCodec)
        try {
          plan.getCompressedOperation.getOpType match {
            case proto.Plan.CompressedOperation.OpType.OP_TYPE_RELATION =>
              proto.Plan.newBuilder().setRoot(proto.Relation.parser().parseFrom(cis)).build()
            case proto.Plan.CompressedOperation.OpType.OP_TYPE_COMMAND =>
              proto.Plan.newBuilder().setCommand(proto.Command.parser().parseFrom(cis)).build()
            case other =>
              throw InvalidInputErrors.invalidOneOfField(
                other,
                plan.getCompressedOperation.getDescriptorForType)
          }
        } catch {
          case e: SparkSQLException =>
            throw e
          case NonFatal(e) =>
            throw new SparkSQLException(
              errorClass = "CONNECT_INVALID_PLAN.CANNOT_PARSE",
              messageParameters = Map("errorMsg" -> e.getMessage))
        } finally {
          try {
            closeStream()
          } catch {
            case NonFatal(_) =>
          }
        }
      case _ => plan
    }
  }

  private def getMaxPlanSize: Long = {
    SparkEnv.get.conf.get(Connect.CONNECT_MAX_PLAN_SIZE)
  }

  /**
   * Decompress the given bytes using the specified codec.
   * @return
   *   A tuple of decompressed CodedInputStream and a function to close the underlying stream.
   */
  private def decompressBytes(
      data: ByteString,
      compressionCodec: proto.CompressionCodec): (CodedInputStream, () => Unit) = {
    compressionCodec match {
      case proto.CompressionCodec.COMPRESSION_CODEC_ZSTD =>
        decompressBytesWithZstd(data, getMaxPlanSize)
      case other =>
        throw InvalidInputErrors.invalidEnum(other)
    }
  }

  private def decompressBytesWithZstd(
      input: ByteString,
      maxOutputSize: Long): (CodedInputStream, () => Unit) = {
    // Check the declared size in the header against the limit.
    val declaredSize = Zstd.getFrameContentSize(input.asReadOnlyByteBuffer())
    if (declaredSize > maxOutputSize) {
      throw new SparkSQLException(
        errorClass = "CONNECT_INVALID_PLAN.PLAN_SIZE_LARGER_THAN_MAX",
        messageParameters =
          Map("planSize" -> declaredSize.toString, "maxPlanSize" -> maxOutputSize.toString))
    }

    val zstdStream = new ZstdInputStreamNoFinalizer(input.newInput())

    // Create a bounded input stream to limit the decompressed output size to avoid decompression
    // bomb attacks.
    val boundedStream = new BoundedInputStream(zstdStream, maxOutputSize) {
      @throws[IOException]
      override protected def onMaxLength(maxBytes: Long, count: Long): Unit =
        throw new SparkSQLException(
          errorClass = "CONNECT_INVALID_PLAN.PLAN_SIZE_LARGER_THAN_MAX",
          messageParameters =
            Map("planSize" -> "unknown", "maxPlanSize" -> maxOutputSize.toString))
    }
    val cis = CodedInputStream.newInstance(boundedStream)
    cis.setSizeLimit(Integer.MAX_VALUE)
    cis.setRecursionLimit(SparkEnv.get.conf.get(Connect.CONNECT_GRPC_MARSHALLER_RECURSION_LIMIT))
    (cis, () => boundedStream.close())
  }
}
