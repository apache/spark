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

package org.apache.spark.ml.util

import java.io.{PrintWriter, StringWriter}
import java.util.UUID

import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.internal.{LogEntry, Logging}
import org.apache.spark.ml.{MLEvents, PipelineStage}
import org.apache.spark.ml.param.{Param, Params}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/**
 * A small wrapper that defines a training session for an estimator, some methods to log
 * useful information during this session, and some methods to send
 * [[org.apache.spark.ml.MLEvent]].
 */
private[spark] class Instrumentation private () extends Logging with MLEvents {

  private val id = UUID.randomUUID()
  private val shortId = id.toString.take(8)
  private[util] val prefix = s"[$shortId] "

  /**
   * Log some info about the pipeline stage being fit.
   */
  def logPipelineStage(stage: PipelineStage): Unit = {
    // estimator.getClass.getSimpleName can cause Malformed class name error,
    // call safer `Utils.getSimpleName` instead
    val className = Utils.getSimpleName(stage.getClass)
    logInfo(s"Stage class: $className")
    logInfo(s"Stage uid: ${stage.uid}")
  }

  /**
   * Log some data about the dataset being fit.
   */
  def logDataset(dataset: Dataset[_]): Unit = logDataset(dataset.rdd)

  /**
   * Log some data about the dataset being fit.
   */
  def logDataset(dataset: RDD[_]): Unit = {
    logInfo(s"training: numPartitions=${dataset.partitions.length}" +
      s" storageLevel=${dataset.getStorageLevel}")
  }

  /**
   * Logs a debug message with a prefix that uniquely identifies the training session.
   */
  override def logDebug(msg: => String): Unit = {
    super.logDebug(prefix + msg)
  }

  /**
   * Logs a warning message with a prefix that uniquely identifies the training session.
   */
  override def logWarning(msg: => String): Unit = {
    super.logWarning(prefix + msg)
  }

  /**
   * Logs a LogEntry which message with a prefix that uniquely identifies the training session.
   */
  override def logWarning(entry: LogEntry): Unit = {
    if (log.isWarnEnabled) {
      withLogContext(entry.context) {
        log.warn(prefix + entry.message)
      }
    }
  }

  /**
   * Logs a error message with a prefix that uniquely identifies the training session.
   */
  override def logError(msg: => String): Unit = {
    super.logError(prefix + msg)
  }

  /**
   * Logs a LogEntry which message with a prefix that uniquely identifies the training session.
   */
  override def logError(entry: LogEntry): Unit = {
    if (log.isErrorEnabled) {
      withLogContext(entry.context) {
        log.error(prefix + entry.message)
      }
    }
  }

  /**
   * Logs an info message with a prefix that uniquely identifies the training session.
   */
  override def logInfo(msg: => String): Unit = {
    super.logInfo(prefix + msg)
  }

  /**
   * Logs a LogEntry which message with a prefix that uniquely identifies the training session.
   */
  override def logInfo(entry: LogEntry): Unit = {
    if (log.isInfoEnabled) {
      withLogContext(entry.context) {
        log.info(prefix + entry.message)
      }
    }
  }

  /**
   * Logs the value of the given parameters for the estimator being used in this session.
   */
  def logParams(hasParams: Params, params: Param[_]*): Unit = {
    val pairs: Seq[(String, JValue)] = for {
      p <- params
      value <- hasParams.get(p)
    } yield {
      val cast = p.asInstanceOf[Param[Any]]
      p.name -> parse(cast.jsonEncode(value))
    }
    logInfo(compact(render(map2jvalue(pairs.toMap))))
  }

  def logNumFeatures(num: Long): Unit = {
    logNamedValue(Instrumentation.loggerTags.numFeatures, num)
  }

  def logNumClasses(num: Long): Unit = {
    logNamedValue(Instrumentation.loggerTags.numClasses, num)
  }

  def logNumExamples(num: Long): Unit = {
    logNamedValue(Instrumentation.loggerTags.numExamples, num)
  }

  def logSumOfWeights(num: Double): Unit = {
    logNamedValue(Instrumentation.loggerTags.sumOfWeights, num)
  }

  /**
   * Logs the value with customized name field.
   */
  def logNamedValue(name: String, value: String): Unit = {
    logInfo(compact(render(name -> value)))
  }

  def logNamedValue(name: String, value: Long): Unit = {
    logInfo(compact(render(name -> value)))
  }

  def logNamedValue(name: String, value: Double): Unit = {
    logInfo(compact(render(name -> value)))
  }

  def logNamedValue(name: String, value: Array[String]): Unit = {
    logInfo(compact(render(name -> compact(render(value.toImmutableArraySeq)))))
  }

  def logNamedValue(name: String, value: Array[Long]): Unit = {
    logInfo(compact(render(name -> compact(render(value.toImmutableArraySeq)))))
  }

  def logNamedValue(name: String, value: Array[Double]): Unit = {
    logInfo(compact(render(name -> compact(render(value.toImmutableArraySeq)))))
  }


  /**
   * Logs the successful completion of the training session.
   */
  def logSuccess(): Unit = {
    logInfo("training finished")
  }

  /**
   * Logs an exception raised during a training session.
   */
  def logFailure(e: Throwable): Unit = {
    val msg = new StringWriter()
    e.printStackTrace(new PrintWriter(msg))
    super.logError(msg.toString)
  }
}

/**
 * Some common methods for logging information about a training session.
 */
private[spark] object Instrumentation {

  object loggerTags {
    val numFeatures = "numFeatures"
    val numClasses = "numClasses"
    val numExamples = "numExamples"
    val meanOfLabels = "meanOfLabels"
    val varianceOfLabels = "varianceOfLabels"
    val sumOfWeights = "sumOfWeights"
  }

  def instrumented[T](body: (Instrumentation => T)): T = {
    val instr = new Instrumentation()
    Try(body(instr)) match {
      case Failure(NonFatal(e)) =>
        instr.logFailure(e)
        throw e
      case Failure(e) =>
        throw e
      case Success(result) =>
        instr.logSuccess()
        result
    }
  }
}

/**
 * A small wrapper that contains an optional `Instrumentation` object.
 * Provide some log methods, if the containing `Instrumentation` object is defined,
 * will log via it, otherwise will log via common logger.
 */
private[spark] class OptionalInstrumentation private(
    val instrumentation: Option[Instrumentation],
    val className: String) extends Logging {

  protected override def logName: String = className

  override def logInfo(msg: => String): Unit = {
    instrumentation match {
      case Some(instr) => instr.logInfo(msg)
      case None => super.logInfo(msg)
    }
  }

  override def logWarning(msg: => String): Unit = {
    instrumentation match {
      case Some(instr) => instr.logWarning(msg)
      case None => super.logWarning(msg)
    }
  }

  override def logError(msg: => String): Unit = {
    instrumentation match {
      case Some(instr) => instr.logError(msg)
      case None => super.logError(msg)
    }
  }
}

private[spark] object OptionalInstrumentation {

  /**
   * Creates an `OptionalInstrumentation` object from an existing `Instrumentation` object.
   */
  def create(instr: Instrumentation): OptionalInstrumentation = {
    new OptionalInstrumentation(Some(instr), instr.prefix)
  }

  /**
   * Creates an `OptionalInstrumentation` object from a `Class` object.
   * The created `OptionalInstrumentation` object will log messages via common logger and use the
   * specified class name as logger name.
   */
  def create(clazz: Class[_]): OptionalInstrumentation = {
    new OptionalInstrumentation(None, clazz.getName.stripSuffix("$"))
  }
}
