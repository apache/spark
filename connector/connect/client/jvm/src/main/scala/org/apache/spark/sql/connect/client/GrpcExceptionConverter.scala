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
package org.apache.spark.sql.connect.client

import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto

import org.apache.spark.{SparkException, SparkThrowable}

private[client] object GrpcExceptionConverter {
  def convert[T](f: => T): T = {
    try {
      f
    } catch {
      case e: StatusRuntimeException =>
        throw toSparkThrowable(e)
    }
  }

  def convertIterator[T](iter: java.util.Iterator[T]): java.util.Iterator[T] = {
    new java.util.Iterator[T] {
      override def hasNext: Boolean = {
        convert {
          iter.hasNext
        }
      }

      override def next(): T = {
        convert {
          iter.next()
        }
      }
    }
  }

  private def toSparkThrowable(ex: StatusRuntimeException): SparkThrowable with Throwable = {
    val status = StatusProto.fromThrowable(ex)
    // TODO: Add finer grained error conversion
    new SparkException(status.getMessage, ex.getCause)
  }
}


