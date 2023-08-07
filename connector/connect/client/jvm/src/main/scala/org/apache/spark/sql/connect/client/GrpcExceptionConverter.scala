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

import scala.jdk.CollectionConverters._

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

  private val errorFactory = new ErrorFactoryBuilder()
    .registerConstructor((message, _) => new ParseException(None, message, Origin(), Origin()))
    .registerConstructor((message, _) => new AnalysisException(message))
    .build()

  private def errorInfoToThrowable(info: ErrorInfo, message: String): Option[Throwable] = {
    val classes =
      mapper.readValue(info.getMetadataOrDefault("classes", "[]"), classOf[Array[String]])

    classes
      .find(errorFactory.contains(_))
      .map { cls =>
        errorFactory.get(cls).get(message, null)
      }
  }

  private def toThrowable(ex: StatusRuntimeException): Throwable = {
    val status = StatusProto.fromThrowable(ex)

    status
      .getDetailsList
      .asScala
      .find(_.is(classOf[ErrorInfo]))
      .flatMap { d =>
        val errorInfo = d.unpack(classOf[ErrorInfo])
        errorInfoToThrowable(errorInfo, status.getMessage)
      }
      .getOrElse(new SparkException(status.getMessage, ex.getCause))
  }
}
