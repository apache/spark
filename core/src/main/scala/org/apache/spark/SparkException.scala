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

import java.io.{FileNotFoundException, IOException}
import java.nio.file.FileAlreadyExistsException
import java.sql.SQLFeatureNotSupportedException
import java.time.DateTimeException
import java.util.ConcurrentModificationException

import org.apache.spark.errors.{DEFAULT, ErrorCode, INTERNAL_ERROR}

class SparkException(
    message: String,
    cause: Throwable,
    val errorCode: ErrorCode = DEFAULT)
  extends Exception(message, cause) {

  def this(message: String) = this(message, null, DEFAULT)
  def this(message: String, cause: Throwable) = this(message, cause, DEFAULT)
}

/**
 * Exception thrown when execution of some user code in the driver process fails, e.g.
 * accumulator update fails or failure in takeOrdered (user supplies an Ordering implementation
 * that can be misbehaving.
 */
private[spark] class SparkDriverExecutionException(cause: Throwable)
  extends SparkException("Execution error", cause)

/**
 * Exception thrown when the main user code is run as a child process (e.g. pyspark) and we want
 * the parent SparkSubmit process to exit with the same exit code.
 */
private[spark] case class SparkUserAppException(exitCode: Int)
  extends SparkException(s"User application exited with $exitCode")

/**
 * Exception thrown when the relative executor to access is dead.
 */
private[spark] case class ExecutorDeadException(message: String)
  extends SparkException(message)

/**
 * Exception thrown when Spark returns different result after upgrading to a new version.
 */
private[spark] class SparkUpgradeException(version: String, message: String, cause: Throwable)
  extends SparkRuntimeException("You may get a different result due to the upgrading of Spark" +
    s" $version: $message", cause, INTERNAL_ERROR)


private[spark] class SparkRuntimeException(
    val reason: String,
    val cause: Throwable,
    val errorCode: ErrorCode = DEFAULT) extends RuntimeException(reason, cause) {
  def this() = this("unknown reason", null)
  def this(reason: String) = this(reason, null)
  def this(cause: Throwable) = this(cause.toString, cause)
}

private[spark] class SparkIllegalArgumentException(
    message: String,
    cause: Throwable,
    val errorCode: ErrorCode = DEFAULT) extends IllegalArgumentException(message, cause) {

  def this(message: String) = this(message, null)

  def this(cause: Throwable) = this(cause.toString, cause)
}

private[spark] class SparkIllegalStateException(
    message: String,
    val errorCode: ErrorCode = DEFAULT) extends IllegalStateException(message) {
}

private[spark] class SparkUnsupportedOperationException(
    message: String, cause: Throwable, val errorCode: ErrorCode = DEFAULT)
  extends UnsupportedOperationException(message, cause) {

  def this(message: String) = this(message, null)

  def this(cause: Throwable) = this(cause.toString, cause)
}

private[spark] class SparkNullPointerException(message: String, val errorCode: ErrorCode = DEFAULT)
  extends NullPointerException(message) {
}

private[spark] class SparkArrayIndexOutOfBoundsException(
    message: String, val errorCode: ErrorCode = DEFAULT)
  extends ArrayIndexOutOfBoundsException(message) {

  def this(message: String) = this(message, null)
}

private[spark] class SparkIndexOutOfBoundsException(
    message: String, val errorCode: ErrorCode = DEFAULT)
  extends IndexOutOfBoundsException(message) {

  def this(message: String) = this(message, null)
}

private[spark] class SparkDateTimeException(
    message: String, val errorCode: ErrorCode = DEFAULT)
  extends DateTimeException(message) {

  def this(message: String) = this(message, null)
}

private[spark] class SparkConcurrentModificationException(
    message: String, val errorCode: ErrorCode = DEFAULT)
  extends ConcurrentModificationException(message) {

  def this(message: String) = this(message, null)
}

private[spark] class SparkSecurityException(
    message: String, val errorCode: ErrorCode = DEFAULT)
  extends SecurityException(message) {

  def this(message: String) = this(message, null)
}

private[spark] class SparkFileNotFoundException(
    message: String, val errorCode: ErrorCode = DEFAULT)
  extends FileNotFoundException(message) {

  def this(message: String) = this(message, null)
}

private[spark] class SparkSQLFeatureNotSupportedException(
    message: String, val errorCode: ErrorCode = DEFAULT)
  extends SQLFeatureNotSupportedException(message) {

  def this(message: String) = this(message, null)
}

private[spark] class SparkIOException(
    message: String, val errorCode: ErrorCode = DEFAULT)
  extends IOException(message) {

  def this(message: String) = this(message, null)
}

private[spark] class SparkFileAlreadyExistsException(
    message: String, val errorCode: ErrorCode = DEFAULT)
  extends FileAlreadyExistsException(message) {

  def this(message: String) = this(message, null)
}

private[spark] class SparkArithmeticException(
    message: String, val errorCode: ErrorCode = DEFAULT)
  extends ArithmeticException(message) {

  def this(message: String) = this(message, null)
}

private[spark] class SparkCloneNotSupportedException(
    message: String, val errorCode: ErrorCode = DEFAULT)
  extends CloneNotSupportedException(message) {

  def this(message: String) = this(message, null)
}

private[spark] class SparkNumberFormatException(
    message: String, val errorCode: ErrorCode = DEFAULT)
  extends NumberFormatException(message) {

  def this(message: String) = this(message, null)
}

