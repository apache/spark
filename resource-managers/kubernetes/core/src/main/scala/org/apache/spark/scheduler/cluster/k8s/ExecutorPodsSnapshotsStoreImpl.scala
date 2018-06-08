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

import java.util.concurrent.{ExecutorService, ScheduledExecutorService, TimeUnit}

import com.google.common.collect.Lists
import io.fabric8.kubernetes.api.model.Pod
import io.reactivex.disposables.Disposable
import io.reactivex.functions.Consumer
import io.reactivex.schedulers.Schedulers
import io.reactivex.subjects.PublishSubject
import javax.annotation.concurrent.GuardedBy
import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.util.{ThreadUtils, Utils}

private[spark] class ExecutorPodsSnapshotsStoreImpl(
    bufferSnapshotsExecutor: ScheduledExecutorService,
    executeSubscriptionsExecutor: ExecutorService)
  extends ExecutorPodsSnapshotsStore {

  private val SNAPSHOT_LOCK = new Object()

  private val snapshotsObservable = PublishSubject.create[ExecutorPodsSnapshot]()
  private val observedDisposables = mutable.Buffer.empty[Disposable]

  @GuardedBy("SNAPSHOT_LOCK")
  private var currentSnapshot = ExecutorPodsSnapshot()

  override def addSubscriber(
      processBatchIntervalMillis: Long)
      (onNewSnapshots: Seq[ExecutorPodsSnapshot] => Unit): Unit = {
    observedDisposables += snapshotsObservable
      // Group events in the time window given by the caller. These buffers are then sent
      // to the caller's lambda at the given interval, with the pod updates that occurred
      // in that given interval.
      .buffer(
        processBatchIntervalMillis,
        TimeUnit.MILLISECONDS,
        // For testing - specifically use the given scheduled executor service to trigger
        // buffer boundaries. Allows us to inject a deterministic scheduler here.
        Schedulers.from(bufferSnapshotsExecutor))
      // Trigger an event cycle immediately. Not strictly required to be fully correct, but
      // in particular the pod allocator should try to request executors immediately instead
      // of waiting for one pod allocation delay.
      .startWith(Lists.newArrayList(ExecutorPodsSnapshot()))
      // Force all triggered events - both the initial event above and the buffered ones in
      // the following time windows - to execute asynchronously to this call's thread.
      .observeOn(Schedulers.from(executeSubscriptionsExecutor))
      .subscribe(toReactivexConsumer { snapshots: java.util.List[ExecutorPodsSnapshot] =>
        Utils.tryLogNonFatalError {
          onNewSnapshots(snapshots.asScala)
        }
      })
  }

  override def stop(): Unit = {
    observedDisposables.foreach(_.dispose())
    snapshotsObservable.onComplete()
    ThreadUtils.shutdown(bufferSnapshotsExecutor)
    ThreadUtils.shutdown(executeSubscriptionsExecutor)
  }

  override def updatePod(updatedPod: Pod): Unit = SNAPSHOT_LOCK.synchronized {
    currentSnapshot = currentSnapshot.withUpdate(updatedPod)
    snapshotsObservable.onNext(currentSnapshot)
  }

  override def replaceSnapshot(newSnapshot: Seq[Pod]): Unit = SNAPSHOT_LOCK.synchronized {
    currentSnapshot = ExecutorPodsSnapshot(newSnapshot)
    snapshotsObservable.onNext(currentSnapshot)
  }

  private def toReactivexConsumer[T](consumer: T => Unit): Consumer[T] = {
    new Consumer[T] {
      override def accept(item: T): Unit = consumer(item)
    }
  }
}
