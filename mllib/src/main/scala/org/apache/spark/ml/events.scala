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

package org.apache.spark.ml

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Unstable
import org.apache.spark.internal.Logging
import org.apache.spark.ml.util.{MLReader, MLWriter}
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.sql.{DataFrame, Dataset}

/**
 * Event emitted by ML operations. Events are either fired before and/or
 * after each operation (the event should document this).
 *
 * @note This is supported via [[Pipeline]] and [[PipelineModel]].
 */
@Unstable
sealed trait MLEvent extends SparkListenerEvent

/**
 * Event fired before `Transformer.transform`.
 */
@Unstable
case class TransformStart(transformer: Transformer, input: Dataset[_]) extends MLEvent
/**
 * Event fired after `Transformer.transform`.
 */
@Unstable
case class TransformEnd(transformer: Transformer, output: Dataset[_]) extends MLEvent

/**
 * Event fired before `Estimator.fit`.
 */
@Unstable
case class FitStart[M <: Model[M]](estimator: Estimator[M], dataset: Dataset[_]) extends MLEvent
/**
 * Event fired after `Estimator.fit`.
 */
@Unstable
case class FitEnd[M <: Model[M]](estimator: Estimator[M], model: M) extends MLEvent

/**
 * Event fired before `MLReader.load`.
 */
@Unstable
case class LoadInstanceStart[T](reader: MLReader[T], path: String) extends MLEvent
/**
 * Event fired after `MLReader.load`.
 */
@Unstable
case class LoadInstanceEnd[T](reader: MLReader[T], instance: T) extends MLEvent

/**
 * Event fired before `MLWriter.save`.
 */
@Unstable
case class SaveInstanceStart(writer: MLWriter, path: String) extends MLEvent
/**
 * Event fired after `MLWriter.save`.
 */
@Unstable
case class SaveInstanceEnd(writer: MLWriter, path: String) extends MLEvent

/**
 * A small trait that defines some methods to send [[org.apache.spark.ml.MLEvent]].
 */
private[ml] trait MLEvents extends Logging {

  private def listenerBus = SparkContext.getOrCreate().listenerBus

  def withFitEvent[M <: Model[M]](
      estimator: Estimator[M], dataset: Dataset[_], logging: Boolean = false)(func: => M): M = {
    val startEvent = FitStart(estimator, dataset)
    if (logging) logDebug(s"Sending an MLEvent: $startEvent")
    listenerBus.post(startEvent)
    val model: M = func
    val endEvent = FitEnd(estimator, model)
    if (logging) logDebug(s"Sending an MLEvent: $endEvent")
    listenerBus.post(endEvent)
    model
  }

  def withTransformEvent(
      transformer: Transformer,
      input: Dataset[_],
      logging: Boolean = false)(func: => DataFrame): DataFrame = {
    val startEvent = TransformStart(transformer, input)
    if (logging) logDebug(s"Sending an MLEvent: $startEvent")
    listenerBus.post(startEvent)
    val output: DataFrame = func
    val endEvent = TransformEnd(transformer, output)
    if (logging) logDebug(s"Sending an MLEvent: $endEvent")
    listenerBus.post(endEvent)
    output
  }

  def withLoadInstanceEvent[T](
      reader: MLReader[T], path: String, logging: Boolean = false)(func: => T): T = {
    val startEvent = LoadInstanceStart(reader, path)
    if (logging) logDebug(s"Sending an MLEvent: $startEvent")
    listenerBus.post(startEvent)
    val instance: T = func
    val endEvent = LoadInstanceEnd(reader, instance)
    if (logging) logDebug(s"Sending an MLEvent: $endEvent")
    listenerBus.post(endEvent)
    instance
  }

  def withSaveInstanceEvent(
      writer: MLWriter, path: String, logging: Boolean = false)(func: => Unit): Unit = {
    listenerBus.post(SaveInstanceEnd(writer, path))
    val startEvent = SaveInstanceStart(writer, path)
    if (logging) logDebug(s"Sending an MLEvent: $startEvent")
    listenerBus.post(startEvent)
    func
    val endEvent = SaveInstanceEnd(writer, path)
    if (logging) logDebug(s"Sending an MLEvent: $endEvent")
    listenerBus.post(endEvent)
  }
}
