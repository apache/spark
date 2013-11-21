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

import org.apache.spark.Logging
import org.apache.zookeeper._

import akka.serialization.Serialization

class ZooKeeperPersistenceEngine(serialization: Serialization)
  extends PersistenceEngine
  with SparkZooKeeperWatcher
  with Logging
{
  val WORKING_DIR = System.getProperty("spark.deploy.zookeeper.dir", "/spark") + "/master_status"

  val zk = new SparkZooKeeperSession(this)

  zk.connect()

  override def zkSessionCreated() {
    zk.mkdirRecursive(WORKING_DIR)
  }

  override def zkDown() {
    logError("PersistenceEngine disconnected from ZooKeeper -- ZK looks down.")
  }

  override def addApplication(app: ApplicationInfo) {
    serializeIntoFile(WORKING_DIR + "/app_" + app.id, app)
  }

  override def removeApplication(app: ApplicationInfo) {
    zk.delete(WORKING_DIR + "/app_" + app.id)
  }

  override def addWorker(worker: WorkerInfo) {
    serializeIntoFile(WORKING_DIR + "/worker_" + worker.id, worker)
  }

  override def removeWorker(worker: WorkerInfo) {
    zk.delete(WORKING_DIR + "/worker_" + worker.id)
  }

  override def close() {
    zk.close()
  }

  override def readPersistedData(): (Seq[ApplicationInfo], Seq[WorkerInfo]) = {
    val sortedFiles = zk.getChildren(WORKING_DIR).toList.sorted
    val appFiles = sortedFiles.filter(_.startsWith("app_"))
    val apps = appFiles.map(deserializeFromFile[ApplicationInfo])
    val workerFiles = sortedFiles.filter(_.startsWith("worker_"))
    val workers = workerFiles.map(deserializeFromFile[WorkerInfo])
    (apps, workers)
  }

  private def serializeIntoFile(path: String, value: AnyRef) {
    val serializer = serialization.findSerializerFor(value)
    val serialized = serializer.toBinary(value)
    zk.create(path, serialized, CreateMode.PERSISTENT)
  }

  def deserializeFromFile[T](filename: String)(implicit m: Manifest[T]): T = {
    val fileData = zk.getData("/spark/master_status/" + filename)
    val clazz = m.runtimeClass.asInstanceOf[Class[T]]
    val serializer = serialization.serializerFor(clazz)
    serializer.fromBinary(fileData).asInstanceOf[T]
  }
}
