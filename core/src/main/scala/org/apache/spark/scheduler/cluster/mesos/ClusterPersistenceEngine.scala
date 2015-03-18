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

package org.apache.spark.scheduler.cluster.mesos

import scala.collection.JavaConversions._

import org.apache.curator.framework.CuratorFramework
import org.apache.spark.deploy.SparkCuratorUtil


import org.apache.zookeeper.CreateMode
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.util.Utils
import org.apache.zookeeper.KeeperException.NoNodeException

abstract class ClusterPersistenceEngineFactory(conf: SparkConf) {
  def createEngine(path: String): ClusterPersistenceEngine
}

private[spark] class ZookeeperClusterPersistenceEngineFactory(conf: SparkConf)
  extends ClusterPersistenceEngineFactory(conf) {

  lazy val zk = SparkCuratorUtil.newClient(conf)

  def createEngine(path: String): ClusterPersistenceEngine = {
    new ZookeeperClusterPersistenceEngine(path, zk, conf)
  }
}

private[spark] class BlackHolePersistenceEngineFactory
  extends ClusterPersistenceEngineFactory(null) {
  def createEngine(path: String): ClusterPersistenceEngine = {
    new BlackHoleClusterPersistenceEngine
  }
}

trait ClusterPersistenceEngine {
  def persist(name: String, obj: Object)
  def expunge(name: String)
  def fetch[T](name: String): Option[T]
  def fetchAll[T](): Iterable[T]
}

private[spark] class BlackHoleClusterPersistenceEngine extends ClusterPersistenceEngine {
  override def persist(name: String, obj: Object): Unit = {}

  override def fetch[T](name: String): Option[T] = None

  override def expunge(name: String): Unit = {}

  override def fetchAll[T](): Iterable[T] = Iterable.empty[T]
}

private[spark] class ZookeeperClusterPersistenceEngine(
    baseDir: String,
    zk: CuratorFramework,
    conf: SparkConf)
  extends ClusterPersistenceEngine with Logging {
  private val WORKING_DIR =
    conf.get("spark.deploy.zookeeper.dir", "/spark_mesos_dispatcher") + "/" + baseDir

  SparkCuratorUtil.mkdir(zk, WORKING_DIR)

  def path(name: String): String = {
    WORKING_DIR + "/" + name
  }

  override def expunge(name: String) {
    zk.delete().forPath(path(name))
  }

  override def persist(name: String, obj: Object) {
    val serialized = Utils.serialize(obj)
    val zkPath = path(name)
    zk.create().withMode(CreateMode.PERSISTENT).forPath(zkPath, serialized)
  }

  override def fetch[T](name: String): Option[T] = {
    val zkPath = path(name)

    try {
      val fileData = zk.getData().forPath(zkPath)
      Some(Utils.deserialize[T](fileData))
    } catch {
      case e: NoNodeException => None
      case e: Exception => {
        logWarning("Exception while reading persisted file, deleting", e)
        zk.delete().forPath(zkPath)
        None
      }
    }
  }

  override def fetchAll[T](): Iterable[T] = {
    zk.getChildren.forPath(WORKING_DIR).map(fetch[T]).flatten
  }
}
