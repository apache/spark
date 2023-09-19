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

import java.time.DateTimeException

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import com.google.rpc.ErrorInfo
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto

import org.apache.spark.{SparkArithmeticException, SparkArrayIndexOutOfBoundsException, SparkDateTimeException, SparkException, SparkIllegalArgumentException, SparkNumberFormatException, SparkRuntimeException, SparkUnsupportedOperationException, SparkUpgradeException}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NoSuchDatabaseException, NoSuchTableException, TableAlreadyExistsException, TempTableAlreadyExistsException}
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
    new WrappedCloseableIterator[T] {

      override def innerIterator: Iterator[T] = iter

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

  private def errorConstructor[T <: Throwable: ClassTag](
      throwableCtr: (String, Option[Throwable]) => T)
      : (String, (String, Option[Throwable]) => Throwable) = {
    val className = implicitly[reflect.ClassTag[T]].runtimeClass.getName
    (className, throwableCtr)
  }

  private val errorFactory = Map(
    errorConstructor((message, _) => new ParseException(None, message, Origin(), Origin())),
    errorConstructor((message, cause) => new AnalysisException(message, cause = cause)),
    errorConstructor((message, _) => new NamespaceAlreadyExistsException(message)),
    errorConstructor((message, cause) => new TableAlreadyExistsException(message, cause)),
    errorConstructor((message, cause) => new TempTableAlreadyExistsException(message, cause)),
    errorConstructor((message, cause) => new NoSuchDatabaseException(message, cause)),
    errorConstructor((message, cause) => new NoSuchTableException(message, cause)),
    errorConstructor[NumberFormatException]((message, _) =>
      new SparkNumberFormatException(message)),
    errorConstructor[IllegalArgumentException]((message, cause) =>
      new SparkIllegalArgumentException(message, cause)),
    errorConstructor[ArithmeticException]((message, _) => new SparkArithmeticException(message)),
    errorConstructor[UnsupportedOperationException]((message, _) =>
      new SparkUnsupportedOperationException(message)),
    errorConstructor[ArrayIndexOutOfBoundsException]((message, _) =>
      new SparkArrayIndexOutOfBoundsException(message)),
    errorConstructor[DateTimeException]((message, _) => new SparkDateTimeException(message)),
    errorConstructor((message, cause) => new SparkRuntimeException(message, cause)),
    errorConstructor((message, cause) => new SparkUpgradeException(message, cause)))

  private def errorInfoToThrowable(info: ErrorInfo, message: String): Option[Throwable] = {
    val classes =
      mapper.readValue(info.getMetadataOrDefault("classes", "[]"), classOf[Array[String]])

    classes
      .find(errorFactory.contains)
      .map { cls =>
        val constructor = errorFactory.get(cls).get
        constructor(message, None)
      }
  }

  private def toThrowable(ex: StatusRuntimeException): Throwable = {
    val status = StatusProto.fromThrowable(ex)

    val fallbackEx = new SparkException(ex.toString, ex.getCause)

    val errorInfoOpt = status.getDetailsList.asScala
      .find(_.is(classOf[ErrorInfo]))

    if (errorInfoOpt.isEmpty) {
      return fallbackEx
    }

    errorInfoToThrowable(errorInfoOpt.get.unpack(classOf[ErrorInfo]), status.getMessage)
      .getOrElse(fallbackEx)
  }
}
