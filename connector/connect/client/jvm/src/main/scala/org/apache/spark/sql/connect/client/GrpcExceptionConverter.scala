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

import com.google.rpc.ErrorInfo
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.util.JsonUtils

private[client] object GrpcExceptionConverter extends JsonUtils {
  def convert[T](f: => T): T = {
    try {
      f
    } catch {
      case e: StatusRuntimeException =>
        throw toThrowable(e)
    }
  }

  def convertIterator[T](iter: CloseableIterator[T]): CloseableIterator[T] = {
    new CloseableIterator[T] {
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

      override def close(): Unit = {
        convert {
          iter.close()
        }
      }
    }
  }

  private def errorInfoToThrowable(info: ErrorInfo, message: String): Throwable = {
    val classes =
      mapper.readValue(info.getMetadataOrDefault("classes", "[]"), classOf[Array[String]])

    for (cls <- classes) {
      cls match {
        case "org.apache.spark.sql.catalyst.parser.ParseException" =>
          return new ParseException(None, message, Origin(), Origin())
        case "org.apache.spark.sql.AnalysisException" =>
          return new AnalysisException(message)
        case _ =>
        // Do nothing.
      }
    }

    new SparkException(message)
  }

  private def toThrowable(ex: StatusRuntimeException): Throwable = {
    val status = StatusProto.fromThrowable(ex)

    status.getDetailsList.forEach { d =>
      if (d.is(classOf[ErrorInfo])) {
        val errorInfo = d.unpack(classOf[ErrorInfo])
        return errorInfoToThrowable(errorInfo, status.getMessage)
      }
    }

    new SparkException(status.getMessage, ex.getCause)
  }
}
