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
package org.apache.spark.scheduler.cluster.k8s

import java.util.concurrent.{Callable, LinkedBlockingQueue, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import io.fabric8.kubernetes.api.model.Pod
import io.reactivex.Flowable
import io.reactivex.disposables.Disposable
import io.reactivex.functions.{BooleanSupplier, Consumer}
import io.reactivex.schedulers.Schedulers
import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.util.Utils

private[spark] class ExecutorPodsEventQueue(eventsProcessorExecutor: ScheduledExecutorService) {

  private val eventQueue = new LinkedBlockingQueue[Pod]()
  private val terminationSignal = new AtomicBoolean(false)
  private val eventsObservable =
    // Source is from the blocking queue
    Flowable.fromCallable(toCallable(eventQueue.take()))
      // Keep polling for items until we're told to stop. When the event queue is empty we'll
      // be able to stall, preventing overload of the downstream observables.
      .repeatUntil(toReactivexBooleanSupplier(() => terminationSignal.get()))
      // When the call to future.get() returns, halt the event stream
      // Forces every event to be shared amongst all observers. Will
      .publish()
  private val observedDisposables = mutable.Buffer.empty[Disposable]

  def addSubscriber(processBatchIntervalMillis: Long)(onNextBatch: Seq[Pod] => Unit): Unit = {
    observedDisposables += eventsObservable
      // Group events in the time window given by the caller. These buffers are then sent
      // to the caller's lambda at the given interval, with the pod updates that occurred
      // in that given interval.
      .buffer(
        processBatchIntervalMillis,
        TimeUnit.MILLISECONDS,
        // For testing - specifically use the given scheduled executor service to trigger
        // buffer boundaries. Allows us to inject a deterministic scheduler here.
        Schedulers.from(eventsProcessorExecutor))
      .subscribeOn(Schedulers.from(eventsProcessorExecutor))
      .subscribe(toReactivexConsumer { (pods: java.util.List[Pod]) =>
        Utils.tryLogNonFatalError {
          onNextBatch(pods.asScala)
        }
      })
  }

  def startProcessingEvents(): Unit = eventsObservable.connect()

  def stopProcessingEvents(): Unit = {
    terminationSignal.set(true)
    observedDisposables.foreach { disposable =>
      Utils.tryLogNonFatalError {
        disposable.dispose()
      }
    }
    eventsProcessorExecutor.shutdownNow()
  }

  def pushPodUpdate(updatedPod: Pod): Unit = eventQueue.add(updatedPod)

  private def toCallable[T](callable: => T): Callable[T] = {
    new Callable[T] {
      override def call(): T = callable
    }
  }

  private def toReactivexConsumer[T](consumer: T => Unit): Consumer[T] = {
    new Consumer[T] {
      override def accept(item: T): Unit = consumer(item)
    }
  }

  private def toReactivexBooleanSupplier(supplier: () => Boolean): BooleanSupplier = {
    new BooleanSupplier {
      override def getAsBoolean = supplier()
    }
  }
}
