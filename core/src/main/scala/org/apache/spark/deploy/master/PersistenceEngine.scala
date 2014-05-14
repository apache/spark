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

package org.apache.spark.deploy.master

import org.apache.spark.annotation.DeveloperApi

import scala.reflect.ClassTag

/**
 * Allows Master to persist any state that is necessary in order to recover from a failure.
 * The following semantics are required:
 *   - addApplication and addWorker are called before completing registration of a new app/worker.
 *   - removeApplication and removeWorker are called at any time.
 * Given these two requirements, we will have all apps and workers persisted, but
 * we might not have yet deleted apps or workers that finished (so their liveness must be verified
 * during recovery).
 */
@DeveloperApi
trait PersistenceEngine {

  def persist(name: String, obj: Object)

  def unpersist(name: String)

  def read[T: ClassTag](name: String): Seq[T]

  def addApplication(app: ApplicationInfo): Unit = {
    persist("app_" + app.id, app)
  }

  def removeApplication(app: ApplicationInfo): Unit = {
    unpersist("app_" + app.id)
  }

  def addWorker(worker: WorkerInfo): Unit = {
    persist("worker_" + worker.id, worker)
  }

  def removeWorker(worker: WorkerInfo): Unit = {
    unpersist("worker_" + worker.id)
  }

  def addDriver(driver: DriverInfo): Unit = {
    persist("driver_" + driver.id, driver)
  }

  def removeDriver(driver: DriverInfo): Unit = {
    unpersist("driver_" + driver.id)
  }

  /**
   * Returns the persisted data sorted by their respective ids (which implies that they're
   * sorted by time of creation).
   */
  def readPersistedData(): (Seq[ApplicationInfo], Seq[DriverInfo], Seq[WorkerInfo]) = {
    (read[ApplicationInfo]("app_"), read[DriverInfo]("driver_"), read[WorkerInfo]("worker_"))
  }

  def close() {}
}

private[spark] class BlackHolePersistenceEngine extends PersistenceEngine {

  override def persist(name: String, obj: Object): Unit = {}

  override def readPersistedData() = (Nil, Nil, Nil)

  override def unpersist(name: String): Unit = {}

  override def read[T: ClassTag](name: String): Seq[T] = Nil

}
