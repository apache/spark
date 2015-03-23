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

import akka.serialization.Serialization

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode

import org.apache.spark.{Logging, SparkConf}


private[master] class ZooKeeperPersistenceEngine(conf: SparkConf, val serialization: Serialization)
  extends PersistenceEngine
  with Logging {
  
  private val WORKING_DIR = conf.get("spark.deploy.zookeeper.dir", "/spark") + "/master_status"
  private val zk: CuratorFramework = SparkCuratorUtil.newClient(conf)

  SparkCuratorUtil.mkdir(zk, WORKING_DIR)


  override def persist(name: String, obj: Object): Unit = {
    serializeIntoFile(WORKING_DIR + "/" + name, obj)
  }

  override def unpersist(name: String): Unit = {
    zk.delete().forPath(WORKING_DIR + "/" + name)
  }

  override def read[T: ClassTag](prefix: String) = {
    val file = zk.getChildren.forPath(WORKING_DIR).filter(_.startsWith(prefix))
    file.map(deserializeFromFile[T]).flatten
  }

  override def close() {
    zk.close()
  }

  private def serializeIntoFile(path: String, value: AnyRef) {
    val serializer = serialization.findSerializerFor(value)
    val serialized = serializer.toBinary(value)
    zk.create().withMode(CreateMode.PERSISTENT).forPath(path, serialized)
  }

  private def deserializeFromFile[T](filename: String)(implicit m: ClassTag[T]): Option[T] = {
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
