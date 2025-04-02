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

import java.nio.ByteBuffer

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkCuratorUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Deploy._
import org.apache.spark.serializer.Serializer


private[master] class ZooKeeperPersistenceEngine(conf: SparkConf, val serializer: Serializer)
  extends PersistenceEngine
  with Logging {

  private val workingDir = conf.get(ZOOKEEPER_DIRECTORY).getOrElse("/spark") + "/master_status"
  private val zk: CuratorFramework = SparkCuratorUtil.newClient(conf)

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
    try {
      Some(serializer.newInstance().deserialize[T](ByteBuffer.wrap(fileData)))
    } catch {
      case e: Exception =>
        logWarning("Exception while reading persisted file, deleting", e)
        zk.delete().forPath(workingDir + "/" + filename)
        None
    }
  }
}
