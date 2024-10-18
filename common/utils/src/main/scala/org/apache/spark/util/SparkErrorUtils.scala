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
package org.apache.spark.util

import java.io.{Closeable, IOException, PrintWriter}
import java.nio.charset.StandardCharsets.UTF_8

import scala.util.control.NonFatal

import org.apache.spark.internal.{Logging, LogKeys, MDC}

private[spark] trait SparkErrorUtils extends Logging {
  /**
   * Execute a block of code that returns a value, re-throwing any non-fatal uncaught
   * exceptions as IOException. This is used when implementing Externalizable and Serializable's
   * read and write methods, since Java's serializer will not report non-IOExceptions properly;
   * see SPARK-4080 for more context.
   */
  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        logError("Exception encountered", e)
        throw e
      case NonFatal(e) =>
        logError("Exception encountered", e)
        throw new IOException(e)
    }
  }

  def tryWithResource[R <: Closeable, T](createResource: => R)(f: R => T): T = {
    val resource = createResource
    try f.apply(resource) finally resource.close()
  }

  /**
   * Try to initialize a resource. If an exception is throw during initialization, closes the
   * resource before propagating the error. Otherwise, the caller is responsible for closing
   * the resource. This means that [[T]] should provide some way to close the resource.
   */
  def tryInitializeResource[R <: Closeable, T](createResource: => R)(initialize: R => T): T = {
    val resource = createResource
    try {
      initialize(resource)
    } catch {
      case e: Throwable =>
        resource.close()
        throw e
    }
  }

  /**
   * Execute a block of code, then a finally block, but if exceptions happen in
   * the finally block, do not suppress the original exception.
   *
   * This is primarily an issue with `finally { out.close() }` blocks, where
   * close needs to be called to clean up `out`, but if an exception happened
   * in `out.write`, it's likely `out` may be corrupted and `out.close` will
   * fail as well. This would then suppress the original/likely more meaningful
   * exception from the original `out.write` call.
   */
  def tryWithSafeFinally[T](block: => T)(finallyBlock: => Unit): T = {
    var originalThrowable: Throwable = null
    try {
      block
    } catch {
      case t: Throwable =>
        // Purposefully not using NonFatal, because even fatal exceptions
        // we don't want to have our finallyBlock suppress
        originalThrowable = t
        throw originalThrowable
    } finally {
      try {
        finallyBlock
      } catch {
        case t: Throwable if (originalThrowable != null && originalThrowable != t) =>
          originalThrowable.addSuppressed(t)
          logWarning(
            log"Suppressing exception in finally: ${MDC(LogKeys.MESSAGE, t.getMessage)}", t)
          throw originalThrowable
      }
    }
  }

  def stackTraceToString(t: Throwable): String = {
    val out = new java.io.ByteArrayOutputStream
    SparkErrorUtils.tryWithResource(new PrintWriter(out)) { writer =>
      t.printStackTrace(writer)
      writer.flush()
    }
    new String(out.toByteArray, UTF_8)
  }
}

private[spark] object SparkErrorUtils extends SparkErrorUtils
