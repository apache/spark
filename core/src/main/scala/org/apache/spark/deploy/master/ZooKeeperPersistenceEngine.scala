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

import java.io.ObjectInputFilter
import java.nio.ByteBuffer

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkCuratorUtil
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.internal.config.Deploy._
import org.apache.spark.serializer.{JavaSerializerInstance, Serializer}
import org.apache.spark.util.ByteBufferInputStream


private[master] class ZooKeeperPersistenceEngine(conf: SparkConf, val serializer: Serializer)
  extends PersistenceEngine
  with Logging {

  private val workingDir = conf.get(ZOOKEEPER_DIRECTORY).getOrElse("/spark") + "/master_status"
  private val zk: CuratorFramework = SparkCuratorUtil.newClient(conf)

  // Only instantiate well-known classes while reading persisted state back, so corrupted
  // or unexpected znode contents are never instantiated in the newly elected master.
  private val serializationFilter: ObjectInputFilter =
    ObjectInputFilter.Config.createFilter(conf.get(RECOVERY_SERIALIZATION_FILTER))

  SparkCuratorUtil.mkdir(zk, workingDir)


  override def persist(name: String, obj: Object): Unit = {
    serializeIntoFile(workingDir + "/" + name, obj)
  }

  override def unpersist(name: String): Unit = {
    zk.delete().forPath(workingDir + "/" + name)
  }

  override def read[T: ClassTag](prefix: String): Seq[T] = {
    zk.getChildren.forPath(workingDir).asScala
      .filter(_.startsWith(prefix)).flatMap(deserializeFromFile[T]).toSeq
  }

  override def close(): Unit = {
    zk.close()
  }

  private def serializeIntoFile(path: String, value: AnyRef): Unit = {
    val serialized = serializer.newInstance().serialize(value)
    val bytes = new Array[Byte](serialized.remaining())
    serialized.get(bytes)
    zk.create().withMode(CreateMode.PERSISTENT).forPath(path, bytes)
  }

  private def deserializeFromFile[T](filename: String)(implicit m: ClassTag[T]): Option[T] = {
    val fileData = zk.getData().forPath(workingDir + "/" + filename)
    val recordingFilter = new RecordingFilter(serializationFilter)
    try {
      serializer.newInstance() match {
        case javaInstance: JavaSerializerInstance =>
          val in = javaInstance.deserializeStream(
            new ByteBufferInputStream(ByteBuffer.wrap(fileData)), recordingFilter)
          try {
            Some(in.readObject[T]())
          } finally {
            in.close()
          }
        case instance =>
          Some(instance.deserialize[T](ByteBuffer.wrap(fileData)))
      }
    } catch {
      case e: Exception if recordingFilter.rejected =>
        // Rejected by the serialization filter, not found corrupt. Skip the znode without
        // deleting it: an overly narrow filter pattern (e.g. "org.apache.spark.*", which
        // does not match subpackages) must not wipe the whole recovery state on failover.
        logError(log"Skipping persisted file ${MDC(LogKeys.FILE_NAME, filename)}, " +
          log"rejected by the recovery serialization filter " +
          log"(${MDC(LogKeys.CONFIG, RECOVERY_SERIALIZATION_FILTER.key)})", e)
        None
      case e: Exception =>
        logWarning("Exception while reading persisted file, deleting", e)
        zk.delete().forPath(workingDir + "/" + filename)
        None
    }
  }

  // Records whether the recovery serialization filter rejected anything during a read, since
  // the JDK reports a rejection only as a generic InvalidClassException. Only this filter's
  // rejections are recorded: a znode rejected solely by a JVM-wide jdk.serialFilter is
  // handled like any other unreadable znode.
  private class RecordingFilter(delegate: ObjectInputFilter) extends ObjectInputFilter {
    var rejected = false

    override def checkInput(info: ObjectInputFilter.FilterInfo): ObjectInputFilter.Status = {
      val status = delegate.checkInput(info)
      if (status == ObjectInputFilter.Status.REJECTED) {
        rejected = true
      }
      status
    }
  }
}
