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

import java.io._

import akka.serialization.Serialization

import org.apache.spark.Logging

/**
 * Stores data in a single on-disk directory with one file per application and worker.
 * Files are deleted when applications and workers are removed.
 *
 * @param dir Directory to store files. Created if non-existent (but not recursively).
 * @param serialization Used to serialize our objects.
 */
private[spark] class FileSystemPersistenceEngine(
    val dir: String,
    val serialization: Serialization)
  extends PersistenceEngine with Logging {

  new File(dir).mkdir()

  override def addApplication(app: ApplicationInfo) {
    val appFile = new File(dir + File.separator + "app_" + app.id)
    serializeIntoFile(appFile, app)
  }

  override def removeApplication(app: ApplicationInfo) {
    new File(dir + File.separator + "app_" + app.id).delete()
  }

  override def addDriver(driver: DriverInfo) {
    val driverFile = new File(dir + File.separator + "driver_" + driver.id)
    serializeIntoFile(driverFile, driver)
  }

  override def removeDriver(driver: DriverInfo) {
    new File(dir + File.separator + "driver_" + driver.id).delete()
  }

  override def addWorker(worker: WorkerInfo) {
    val workerFile = new File(dir + File.separator + "worker_" + worker.id)
    serializeIntoFile(workerFile, worker)
  }

  override def removeWorker(worker: WorkerInfo) {
    new File(dir + File.separator + "worker_" + worker.id).delete()
  }

  override def readPersistedData(): (Seq[ApplicationInfo], Seq[DriverInfo], Seq[WorkerInfo]) = {
    val sortedFiles = new File(dir).listFiles().sortBy(_.getName)
    val appFiles = sortedFiles.filter(_.getName.startsWith("app_"))
    val apps = appFiles.map(deserializeFromFile[ApplicationInfo])
    val driverFiles = sortedFiles.filter(_.getName.startsWith("driver_"))
    val drivers = driverFiles.map(deserializeFromFile[DriverInfo])
    val workerFiles = sortedFiles.filter(_.getName.startsWith("worker_"))
    val workers = workerFiles.map(deserializeFromFile[WorkerInfo])
    (apps, drivers, workers)
  }

  private def serializeIntoFile(file: File, value: AnyRef) {
    val created = file.createNewFile()
    if (!created) { throw new IllegalStateException("Could not create file: " + file) }

    val serializer = serialization.findSerializerFor(value)
    val serialized = serializer.toBinary(value)

    val out = new FileOutputStream(file)
    try {
      out.write(serialized)
    } finally {
      out.close()
    }
  }

  def deserializeFromFile[T](file: File)(implicit m: Manifest[T]): T = {
    val fileData = new Array[Byte](file.length().asInstanceOf[Int])
    val dis = new DataInputStream(new FileInputStream(file))
    try {
      dis.readFully(fileData)
    } finally {
      dis.close()
    }

    val clazz = m.runtimeClass.asInstanceOf[Class[T]]
    val serializer = serialization.serializerFor(clazz)
    serializer.fromBinary(fileData).asInstanceOf[T]
  }
}
