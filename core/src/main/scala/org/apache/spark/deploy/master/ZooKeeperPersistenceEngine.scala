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

import scala.collection.JavaConversions._

import akka.serialization.Serialization
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode

import org.apache.spark.{Logging, SparkConf}

class ZooKeeperPersistenceEngine(serialization: Serialization, conf: SparkConf)
  extends PersistenceEngine
  with Logging
{
  val WORKING_DIR = conf.get("spark.deploy.zookeeper.dir", "/spark") + "/master_status"
  val zk: CuratorFramework = SparkCuratorUtil.newClient(conf)

  SparkCuratorUtil.mkdir(zk, WORKING_DIR)

  override def addApplication(app: ApplicationInfo) {
    serializeIntoFile(WORKING_DIR + "/app_" + app.id, app)
  }

  override def removeApplication(app: ApplicationInfo) {
    zk.delete().forPath(WORKING_DIR + "/app_" + app.id)
  }

  override def addDriver(driver: DriverInfo) {
    serializeIntoFile(WORKING_DIR + "/driver_" + driver.id, driver)
  }

  override def removeDriver(driver: DriverInfo) {
    zk.delete().forPath(WORKING_DIR + "/driver_" + driver.id)
  }

  override def addWorker(worker: WorkerInfo) {
    serializeIntoFile(WORKING_DIR + "/worker_" + worker.id, worker)
  }

  override def removeWorker(worker: WorkerInfo) {
    zk.delete().forPath(WORKING_DIR + "/worker_" + worker.id)
  }

  override def close() {
    zk.close()
  }

  override def readPersistedData(): (Seq[ApplicationInfo], Seq[DriverInfo], Seq[WorkerInfo]) = {
    val sortedFiles = zk.getChildren().forPath(WORKING_DIR).toList.sorted
    val appFiles = sortedFiles.filter(_.startsWith("app_"))
    val apps = appFiles.map(deserializeFromFile[ApplicationInfo]).flatten
    val driverFiles = sortedFiles.filter(_.startsWith("driver_"))
    val drivers = driverFiles.map(deserializeFromFile[DriverInfo]).flatten
    val workerFiles = sortedFiles.filter(_.startsWith("worker_"))
    val workers = workerFiles.map(deserializeFromFile[WorkerInfo]).flatten
    (apps, drivers, workers)
  }

  private def serializeIntoFile(path: String, value: AnyRef) {
    val serializer = serialization.findSerializerFor(value)
    val serialized = serializer.toBinary(value)
    zk.create().withMode(CreateMode.PERSISTENT).forPath(path, serialized)
  }

  def deserializeFromFile[T](filename: String)(implicit m: Manifest[T]): Option[T] = {
    val fileData = zk.getData().forPath(WORKING_DIR + "/" + filename)
    val clazz = m.runtimeClass.asInstanceOf[Class[T]]
    val serializer = serialization.serializerFor(clazz)
    try {
      Some(serializer.fromBinary(fileData).asInstanceOf[T])
    } catch {
      case e: Exception => {
        logWarning("Exception while reading persisted file, deleting", e)
        zk.delete().forPath(WORKING_DIR + "/" + filename)
        None
      }
    }
  }
}
