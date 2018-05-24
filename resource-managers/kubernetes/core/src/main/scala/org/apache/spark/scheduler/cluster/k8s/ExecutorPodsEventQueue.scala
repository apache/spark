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

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import com.google.common.collect.Lists
import io.fabric8.kubernetes.api.model.Pod
import io.reactivex.disposables.Disposable
import io.reactivex.functions.Consumer
import io.reactivex.schedulers.Schedulers
import io.reactivex.subjects.PublishSubject
import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.util.Utils

private[spark] class ExecutorPodsEventQueue(eventsProcessorExecutor: ScheduledExecutorService) {

  private val eventsObservable = PublishSubject.create[Pod]()
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
      .startWith(Lists.newArrayList[Pod]())
      .subscribe(toReactivexConsumer { (pods: java.util.List[Pod]) =>
        Utils.tryLogNonFatalError {
          onNextBatch(pods.asScala)
        }
      })
  }

  def stopProcessingEvents(): Unit = {
    observedDisposables.foreach(_.dispose())
    eventsObservable.onComplete()
  }

  def pushPodUpdate(updatedPod: Pod): Unit = eventsObservable.onNext(updatedPod)

  private def toReactivexConsumer[T](consumer: T => Unit): Consumer[T] = {
    new Consumer[T] {
      override def accept(item: T): Unit = consumer(item)
    }
  }
}
