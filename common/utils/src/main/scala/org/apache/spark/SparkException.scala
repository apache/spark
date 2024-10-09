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

import java.io.FileNotFoundException
import java.sql.{SQLException, SQLFeatureNotSupportedException}
import java.time.DateTimeException
import java.util.ConcurrentModificationException

import scala.jdk.CollectionConverters._

class SparkException(
    message: String,
    cause: Throwable,
    errorClass: Option[String],
    messageParameters: Map[String, String],
    context: Array[QueryContext] = Array.empty)
  extends Exception(message, cause) with SparkThrowable {

  def this(message: String, cause: Throwable) =
    this(message = message, cause = cause, errorClass = None, messageParameters = Map.empty)

  def this(message: String) =
    this(message = message, cause = null)

  def this(
      errorClass: String,
      messageParameters: Map[String, String],
      cause: Throwable,
      context: Array[QueryContext],
      summary: String) =
    this(
      message = SparkThrowableHelper.getMessage(errorClass, messageParameters, summary),
      cause = cause,
      errorClass = Some(errorClass),
      messageParameters = messageParameters,
      context)

  def this(errorClass: String, messageParameters: Map[String, String], cause: Throwable) =
    this(
      message = SparkThrowableHelper.getMessage(errorClass, messageParameters),
      cause = cause,
      errorClass = Some(errorClass),
      messageParameters = messageParameters)

  def this(errorClass: String, messageParameters: Map[String, String], cause: Throwable,
      context: Array[QueryContext]) =
    this(
      message = SparkThrowableHelper.getMessage(errorClass, messageParameters),
      cause = cause,
      errorClass = Some(errorClass),
      messageParameters = messageParameters,
      context = context)

  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava

  override def getCondition: String = errorClass.orNull

  override def getQueryContext: Array[QueryContext] = context
}

object SparkException {
  def internalError(msg: String, context: Array[QueryContext], summary: String): SparkException = {
    internalError(msg = msg, context = context, summary = summary, category = None)
  }

  def internalError(
      msg: String,
      context: Array[QueryContext],
      summary: String,
      category: Option[String]): SparkException = {
    new SparkException(
      errorClass = "INTERNAL_ERROR" + category.map("_" + _).getOrElse(""),
      messageParameters = Map("message" -> msg),
      cause = null,
      context,
      summary)
  }

  def internalError(msg: String): SparkException = {
    internalError(msg, context = Array.empty[QueryContext], summary = "", category = None)
  }

  def internalError(msg: String, category: String): SparkException = {
    internalError(msg, context = Array.empty[QueryContext], summary = "", category = Some(category))
  }

  def internalError(msg: String, cause: Throwable): SparkException = {
    new SparkException(
      errorClass = "INTERNAL_ERROR",
      messageParameters = Map("message" -> msg),
      cause = cause)
  }

  /**
   * This is like the Scala require precondition, except it uses SparkIllegalArgumentException.
   * @param requirement The requirement you want to check
   * @param errorClass The error class to type if the requirement isn't passed
   * @param messageParameters Message parameters to append to the message
   */
  def require(
      requirement: Boolean,
      errorClass: String,
      messageParameters: => Map[String, String]): Unit = {
    if (!requirement) {
      throw new SparkIllegalArgumentException(errorClass, messageParameters)
    }
  }

  /**
   * Utility method to construct message params from Java Map.
   * @param messageParameters The Java Map.
   * @return Scala collection that can be passed to SparkException constructor.
   */
  def constructMessageParams(
      messageParameters: java.util.Map[String, String]): Map[String, String] = {
    messageParameters.asScala.toMap
  }
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
  extends SparkException(errorClass = "INTERNAL_ERROR_NETWORK",
    messageParameters = Map("message" -> message), cause = null)

/**
 * Exception thrown when Spark returns different result after upgrading to a new version.
 */
private[spark] class SparkUpgradeException private(
    message: String,
    cause: Option[Throwable],
    errorClass: Option[String],
    messageParameters: Map[String, String])
  extends RuntimeException(message, cause.orNull) with SparkThrowable {

  def this(
    errorClass: String,
    messageParameters: Map[String, String],
    cause: Throwable) = {
    this(
      SparkThrowableHelper.getMessage(errorClass, messageParameters),
      Option(cause),
      Option(errorClass),
      messageParameters
    )
  }

  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava

  override def getCondition: String = errorClass.orNull
}

/**
 * Arithmetic exception thrown from Spark with an error class.
 */
private[spark] class SparkArithmeticException private(
    message: String,
    errorClass: Option[String],
    messageParameters: Map[String, String],
    context: Array[QueryContext])
  extends ArithmeticException(message) with SparkThrowable {

  def this(
    errorClass: String,
    messageParameters: Map[String, String],
    context: Array[QueryContext],
    summary: String) = {
    this(
      SparkThrowableHelper.getMessage(errorClass, messageParameters, summary),
      Option(errorClass),
      messageParameters,
      context
    )
  }

  def this(
    errorClass: String,
    messageParameters: Map[String, String],
    context: Array[QueryContext]) = this(errorClass, messageParameters, context, "")

  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava

  override def getCondition: String = errorClass.orNull
  override def getQueryContext: Array[QueryContext] = context
}

/**
 * Unsupported operation exception thrown from Spark with an error class.
 */
private[spark] class SparkUnsupportedOperationException private(
  message: String,
  errorClass: Option[String],
  messageParameters: Map[String, String])
  extends UnsupportedOperationException(message) with SparkThrowable {

  def this(
    errorClass: String,
    messageParameters: Map[String, String]) = {
    this(
      SparkThrowableHelper.getMessage(errorClass, messageParameters),
      Option(errorClass),
      messageParameters
    )
  }

  def this(
    errorClass: String,
    messageParameters: java.util.Map[String, String]) =
    this(errorClass, messageParameters.asScala.toMap)


  def this(errorClass: String) = {
    this(
      SparkThrowableHelper.getMessage(errorClass, Map.empty[String, String]),
      Option(errorClass),
      Map.empty)
  }

  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava

  override def getCondition: String = errorClass.orNull
}

private[spark] object SparkUnsupportedOperationException {
  def apply(): SparkUnsupportedOperationException = {
    val stackTrace = Thread.currentThread().getStackTrace
    val messageParameters = if (stackTrace.length >= 4) {
      val element = stackTrace(3)
      Map("className" -> element.getClassName, "methodName" -> element.getMethodName)
    } else {
      Map("className" -> "?", "methodName" -> "?")
    }
    new SparkUnsupportedOperationException(
      "UNSUPPORTED_CALL.WITHOUT_SUGGESTION", messageParameters)
  }
}

/**
 * Class not found exception thrown from Spark with an error class.
 */
private[spark] class SparkClassNotFoundException(
    errorClass: String,
    messageParameters: Map[String, String],
    cause: Throwable = null)
  extends ClassNotFoundException(
    SparkThrowableHelper.getMessage(errorClass, messageParameters), cause)
  with SparkThrowable {

  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava

  override def getCondition: String = errorClass
}

/**
 * Concurrent modification exception thrown from Spark with an error class.
 */
private[spark] class SparkConcurrentModificationException(
    errorClass: String,
    messageParameters: Map[String, String],
    cause: Throwable = null)
  extends ConcurrentModificationException(
    SparkThrowableHelper.getMessage(errorClass, messageParameters), cause)
  with SparkThrowable {

  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava

  override def getCondition: String = errorClass
}

/**
 * Datetime exception thrown from Spark with an error class.
 */
private[spark] class SparkDateTimeException private(
    message: String,
    errorClass: Option[String],
    messageParameters: Map[String, String],
    context: Array[QueryContext],
    cause: Option[Throwable])
  extends DateTimeException(message, cause.orNull) with SparkThrowable {

  def this(
    errorClass: String,
    messageParameters: Map[String, String],
    context: Array[QueryContext],
    summary: String) = {
    this(
      SparkThrowableHelper.getMessage(errorClass, messageParameters, summary),
      Option(errorClass),
      messageParameters,
      context,
      cause = None
    )
  }

  def this(
    errorClass: String,
    messageParameters: Map[String, String],
    context: Array[QueryContext],
    summary: String,
    cause: Option[Throwable]) = {
    this(
      SparkThrowableHelper.getMessage(errorClass, messageParameters, summary),
      Option(errorClass),
      messageParameters,
      context,
      cause.orElse(None)
    )
  }

  def this(
    errorClass: String,
    messageParameters: Map[String, String],
    context: Array[QueryContext]) = this(errorClass, messageParameters, context, "")

  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava

  override def getCondition: String = errorClass.orNull
  override def getQueryContext: Array[QueryContext] = context
}

/**
 * File not found exception thrown from Spark with an error class.
 */
private[spark] class SparkFileNotFoundException(
    errorClass: String,
    messageParameters: Map[String, String])
  extends FileNotFoundException(
    SparkThrowableHelper.getMessage(errorClass, messageParameters))
  with SparkThrowable {

  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava

  override def getCondition: String = errorClass
}

/**
 * Number format exception thrown from Spark with an error class.
 */
private[spark] class SparkNumberFormatException private(
    message: String,
    errorClass: Option[String],
    messageParameters: Map[String, String],
    context: Array[QueryContext])
  extends NumberFormatException(message)
  with SparkThrowable {

  def this(
    errorClass: String,
    messageParameters: Map[String, String],
    context: Array[QueryContext],
    summary: String) = {
    this(
      SparkThrowableHelper.getMessage(errorClass, messageParameters, summary),
      Option(errorClass),
      messageParameters,
      context
    )
  }

  def this(
    errorClass: String,
    messageParameters: Map[String, String],
    context: Array[QueryContext]) = this(errorClass, messageParameters, context, "")

  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava

  override def getCondition: String = errorClass.orNull
  override def getQueryContext: Array[QueryContext] = context
}

/**
 * Illegal argument exception thrown from Spark with an error class.
 */
private[spark] class SparkIllegalArgumentException private(
    message: String,
    cause: Option[Throwable],
    errorClass: Option[String],
    messageParameters: Map[String, String],
    context: Array[QueryContext])
  extends IllegalArgumentException(message, cause.orNull)
  with SparkThrowable {

  def this(
    errorClass: String,
    messageParameters: Map[String, String],
    context: Array[QueryContext],
    summary: String,
    cause: Throwable) = {
    this(
      SparkThrowableHelper.getMessage(errorClass, messageParameters, summary),
      Option(cause),
      Option(errorClass),
      messageParameters,
      context
    )
  }

  def this(
    errorClass: String,
    messageParameters: Map[String, String],
    cause: Throwable) =
    this(errorClass, messageParameters, Array.empty[QueryContext], "", cause)

  def this(
    errorClass: String,
    messageParameters: Map[String, String]) =
    this(errorClass, messageParameters, cause = null)

  def this(
    errorClass: String,
    messageParameters: java.util.Map[String, String]) =
    this(errorClass, messageParameters.asScala.toMap)

  def this(errorClass: String) =
    this(errorClass, messageParameters = Map.empty[String, String], cause = null)

  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava

  override def getCondition: String = errorClass.orNull
  override def getQueryContext: Array[QueryContext] = context
}

private[spark] class SparkRuntimeException private(
    message: String,
    cause: Option[Throwable],
    errorClass: Option[String],
    messageParameters: Map[String, String],
    context: Array[QueryContext])
  extends RuntimeException(message, cause.orNull) with SparkThrowable {

  def this(
    errorClass: String,
    messageParameters: Map[String, String],
    cause: Throwable = null,
    context: Array[QueryContext] = Array.empty,
    summary: String = "") = {
    this(
      SparkThrowableHelper.getMessage(errorClass, messageParameters, summary),
      Option(cause),
      Option(errorClass),
      messageParameters,
      context
    )
  }

  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava

  override def getCondition: String = errorClass.orNull
  override def getQueryContext: Array[QueryContext] = context
}

private[spark] class SparkPythonException private(
    message: String,
    cause: Option[Throwable],
    errorClass: Option[String],
    messageParameters: Map[String, String],
    context: Array[QueryContext])
  extends RuntimeException(message, cause.orNull) with SparkThrowable {

  def this(
      errorClass: String,
      messageParameters: Map[String, String],
      cause: Throwable = null,
      context: Array[QueryContext] = Array.empty,
      summary: String = "") = {
    this(
      SparkThrowableHelper.getMessage(errorClass, messageParameters, summary),
      Option(cause),
      Option(errorClass),
      messageParameters,
      context
    )
  }

  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava

  override def getCondition: String = errorClass.orNull
  override def getQueryContext: Array[QueryContext] = context
}

/**
 * No such element exception thrown from Spark with an error class.
 */
private[spark] class SparkNoSuchElementException(
    errorClass: String,
    messageParameters: Map[String, String],
    context: Array[QueryContext] = Array.empty,
    summary: String = "")
    extends NoSuchElementException(
      SparkThrowableHelper.getMessage(errorClass, messageParameters, summary))
    with SparkThrowable {

  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava

  override def getCondition: String = errorClass

  override def getQueryContext: Array[QueryContext] = context
}

/**
 * Security exception thrown from Spark with an error class.
 */
private[spark] class SparkSecurityException(
    errorClass: String,
    messageParameters: Map[String, String])
  extends SecurityException(
    SparkThrowableHelper.getMessage(errorClass, messageParameters))
  with SparkThrowable {

  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava

  override def getCondition: String = errorClass
}

/**
 * Array index out of bounds exception thrown from Spark with an error class.
 */
private[spark] class SparkArrayIndexOutOfBoundsException private(
  message: String,
  errorClass: Option[String],
  messageParameters: Map[String, String],
  context: Array[QueryContext])
  extends ArrayIndexOutOfBoundsException(message)
    with SparkThrowable {

  def this(
    errorClass: String,
    messageParameters: Map[String, String],
    context: Array[QueryContext],
    summary: String) = {
    this(
      SparkThrowableHelper.getMessage(errorClass, messageParameters, summary),
      Option(errorClass),
      messageParameters,
      context
    )
  }

  def this(
    errorClass: String,
    messageParameters: Map[String, String],
    context: Array[QueryContext]) = this(errorClass, messageParameters, context, "")

  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava

  override def getCondition: String = errorClass.orNull
  override def getQueryContext: Array[QueryContext] = context
}

/**
 * SQL exception thrown from Spark with an error class.
 */
private[spark] class SparkSQLException(
    errorClass: String,
    messageParameters: Map[String, String])
  extends SQLException(
    SparkThrowableHelper.getMessage(errorClass, messageParameters))
  with SparkThrowable {

  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava

  override def getCondition: String = errorClass
}

/**
 * SQL feature not supported exception thrown from Spark with an error class.
 */
private[spark] class SparkSQLFeatureNotSupportedException(
    errorClass: String,
    messageParameters: Map[String, String])
  extends SQLFeatureNotSupportedException(
    SparkThrowableHelper.getMessage(errorClass, messageParameters))
  with SparkThrowable {

  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava

  override def getCondition: String = errorClass
}
