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
import java.nio.file.{FileAlreadyExistsException, Files, Paths}

import scala.reflect.ClassTag

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer}
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils


/**
 * Stores data in a single on-disk directory with one file per application and worker.
 * Files are deleted when applications and workers are removed.
 *
 * @param dir Directory to store files. Created if non-existent.
 * @param serializer Used to serialize our objects.
 */
private[master] class FileSystemPersistenceEngine(
    val dir: String,
    val serializer: Serializer,
    val codec: Option[CompressionCodec] = None)
  extends PersistenceEngine with Logging {

  try {
    Files.createDirectories(Paths.get(dir))
  } catch {
    case _: FileAlreadyExistsException if Files.isSymbolicLink(Paths.get(dir)) =>
      Files.createDirectories(Paths.get(dir).toRealPath())
  }

  override def persist(name: String, obj: Object): Unit = {
    serializeIntoFile(new File(dir + File.separator + name), obj)
  }

  override def unpersist(name: String): Unit = {
    val f = new File(dir + File.separator + name)
    if (!f.delete()) {
      logWarning(log"Error deleting ${MDC(PATH, f.getPath())}")
    }
  }

  override def read[T: ClassTag](prefix: String): Seq[T] = {
    val files = new File(dir).listFiles().filter(_.getName.startsWith(prefix))
    files.map(deserializeFromFile[T]).toImmutableArraySeq
  }

  private def serializeIntoFile(file: File, value: AnyRef): Unit = {
    if (file.exists()) { throw new IllegalStateException("File already exists: " + file) }
    val created = file.createNewFile()
    if (!created) { throw new IllegalStateException("Could not create file: " + file) }
    var fileOut: OutputStream = new FileOutputStream(file)
    codec.foreach { c => fileOut = c.compressedOutputStream(fileOut) }
    var out: SerializationStream = null
    Utils.tryWithSafeFinally {
      out = serializer.newInstance().serializeStream(fileOut)
      out.writeObject(value)
    } {
      if (out != null) {
        out.close()
      }
      fileOut.close()
    }
  }

  private def deserializeFromFile[T](file: File)(implicit m: ClassTag[T]): T = {
    var fileIn: InputStream = new FileInputStream(file)
    codec.foreach { c => fileIn = c.compressedInputStream(new FileInputStream(file)) }
    var in: DeserializationStream = null
    try {
      in = serializer.newInstance().deserializeStream(fileIn)
      in.readObject[T]()
    } finally {
      fileIn.close()
      if (in != null) {
        in.close()
      }
    }
  }

}
