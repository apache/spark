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
package org.apache.spark.util.cleanup

import java.lang.ref.{ReferenceQueue, WeakReference}

/**
 * Classes that represent cleaning tasks.
 */
private[spark] sealed trait CleanupTask
private[spark] case class CleanRDD(rddId: Int) extends CleanupTask
private[spark] case class CleanShuffle(shuffleId: Int) extends CleanupTask
private[spark] case class CleanBroadcast(broadcastId: Long) extends CleanupTask
private[spark] case class CleanAccum(accId: Long) extends CleanupTask
private[spark] case class CleanCheckpoint(rddId: Int) extends CleanupTask
private[spark] case class CleanExternalList(pathsToClean: Iterable[String]) extends CleanupTask

/**
 * A WeakReference associated with a CleanupTask.
 *
 * When the referent object becomes only weakly reachable, the corresponding
 * CleanupTaskWeakReference is automatically added to the given reference queue.
 */
private[spark] class CleanupTaskWeakReference(
    val task: CleanupTask,
    referent: AnyRef,
    referenceQueue: ReferenceQueue[AnyRef])
  extends WeakReference(referent, referenceQueue)
