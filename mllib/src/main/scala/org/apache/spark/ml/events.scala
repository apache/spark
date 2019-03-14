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

import com.fasterxml.jackson.annotation.JsonIgnore

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
 * @note This is experimental and unstable. Do not use this unless you fully
 *   understand what `Unstable` means.
 */
@Unstable
sealed trait MLEvent extends SparkListenerEvent {
  // Do not log ML events in event log. It should be revisited to see
  // how it works with history server.
  protected[spark] override def logEvent: Boolean = false
}

/**
 * Event fired before `Transformer.transform`.
 */
@Unstable
case class TransformStart() extends MLEvent {
  @JsonIgnore var transformer: Transformer = _
  @JsonIgnore var input: Dataset[_] = _
}

/**
 * Event fired after `Transformer.transform`.
 */
@Unstable
case class TransformEnd() extends MLEvent {
  @JsonIgnore var transformer: Transformer = _
  @JsonIgnore var output: Dataset[_] = _
}

/**
 * Event fired before `Estimator.fit`.
 */
@Unstable
case class FitStart[M <: Model[M]]() extends MLEvent {
  @JsonIgnore var estimator: Estimator[M] = _
  @JsonIgnore var dataset: Dataset[_] = _
}

/**
 * Event fired after `Estimator.fit`.
 */
@Unstable
case class FitEnd[M <: Model[M]]() extends MLEvent {
  @JsonIgnore var estimator: Estimator[M] = _
  @JsonIgnore var model: M = _
}

/**
 * Event fired before `MLReader.load`.
 */
@Unstable
case class LoadInstanceStart[T](path: String) extends MLEvent {
  @JsonIgnore var reader: MLReader[T] = _
}

/**
 * Event fired after `MLReader.load`.
 */
@Unstable
case class LoadInstanceEnd[T]() extends MLEvent {
  @JsonIgnore var reader: MLReader[T] = _
  @JsonIgnore var instance: T = _
}

/**
 * Event fired before `MLWriter.save`.
 */
@Unstable
case class SaveInstanceStart(path: String) extends MLEvent {
  @JsonIgnore var writer: MLWriter = _
}

/**
 * Event fired after `MLWriter.save`.
 */
@Unstable
case class SaveInstanceEnd(path: String) extends MLEvent {
  @JsonIgnore var writer: MLWriter = _
}

/**
 * A small trait that defines some methods to send [[org.apache.spark.ml.MLEvent]].
 */
private[ml] trait MLEvents extends Logging {

  private def listenerBus = SparkContext.getOrCreate().listenerBus

  /**
   * Log [[MLEvent]] to send. By default, it emits a debug-level log.
   */
  def logEvent(event: MLEvent): Unit = logDebug(s"Sending an MLEvent: $event")

  def withFitEvent[M <: Model[M]](
      estimator: Estimator[M], dataset: Dataset[_])(func: => M): M = {
    val startEvent = FitStart[M]()
    startEvent.estimator = estimator
    startEvent.dataset = dataset
    logEvent(startEvent)
    listenerBus.post(startEvent)
    val model: M = func
    val endEvent = FitEnd[M]()
    endEvent.estimator = estimator
    endEvent.model = model
    logEvent(endEvent)
    listenerBus.post(endEvent)
    model
  }

  def withTransformEvent(
      transformer: Transformer, input: Dataset[_])(func: => DataFrame): DataFrame = {
    val startEvent = TransformStart()
    startEvent.transformer = transformer
    startEvent.input = input
    logEvent(startEvent)
    listenerBus.post(startEvent)
    val output: DataFrame = func
    val endEvent = TransformEnd()
    endEvent.transformer = transformer
    endEvent.output = output
    logEvent(endEvent)
    listenerBus.post(endEvent)
    output
  }

  def withLoadInstanceEvent[T](reader: MLReader[T], path: String)(func: => T): T = {
    val startEvent = LoadInstanceStart[T](path)
    startEvent.reader = reader
    logEvent(startEvent)
    listenerBus.post(startEvent)
    val instance: T = func
    val endEvent = LoadInstanceEnd[T]()
    endEvent.reader = reader
    endEvent.instance = instance
    logEvent(endEvent)
    listenerBus.post(endEvent)
    instance
  }

  def withSaveInstanceEvent(writer: MLWriter, path: String)(func: => Unit): Unit = {
    val startEvent = SaveInstanceStart(path)
    startEvent.writer = writer
    logEvent(startEvent)
    listenerBus.post(startEvent)
    func
    val endEvent = SaveInstanceEnd(path)
    endEvent.writer = writer
    logEvent(endEvent)
    listenerBus.post(endEvent)
  }
}
