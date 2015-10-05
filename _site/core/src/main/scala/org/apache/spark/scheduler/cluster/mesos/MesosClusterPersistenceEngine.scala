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

import scala.collection.JavaConverters._

import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.KeeperException.NoNodeException

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.deploy.SparkCuratorUtil
import org.apache.spark.util.Utils

/**
 * Persistence engine factory that is responsible for creating new persistence engines
 * to store Mesos cluster mode state.
 */
private[spark] abstract class MesosClusterPersistenceEngineFactory(conf: SparkConf) {
  def createEngine(path: String): MesosClusterPersistenceEngine
}

/**
 * Mesos cluster persistence engine is responsible for persisting Mesos cluster mode
 * specific state, so that on failover all the state can be recovered and the scheduler
 * can resume managing the drivers.
 */
private[spark] trait MesosClusterPersistenceEngine {
  def persist(name: String, obj: Object): Unit
  def expunge(name: String): Unit
  def fetch[T](name: String): Option[T]
  def fetchAll[T](): Iterable[T]
}

/**
 * Zookeeper backed persistence engine factory.
 * All Zk engines created from this factory shares the same Zookeeper client, so
 * all of them reuses the same connection pool.
 */
private[spark] class ZookeeperMesosClusterPersistenceEngineFactory(conf: SparkConf)
  extends MesosClusterPersistenceEngineFactory(conf) {

  lazy val zk = SparkCuratorUtil.newClient(conf, "spark.mesos.deploy.zookeeper.url")

  def createEngine(path: String): MesosClusterPersistenceEngine = {
    new ZookeeperMesosClusterPersistenceEngine(path, zk, conf)
  }
}

/**
 * Black hole persistence engine factory that creates black hole
 * persistence engines, which stores nothing.
 */
private[spark] class BlackHoleMesosClusterPersistenceEngineFactory
  extends MesosClusterPersistenceEngineFactory(null) {
  def createEngine(path: String): MesosClusterPersistenceEngine = {
    new BlackHoleMesosClusterPersistenceEngine
  }
}

/**
 * Black hole persistence engine that stores nothing.
 */
private[spark] class BlackHoleMesosClusterPersistenceEngine extends MesosClusterPersistenceEngine {
  override def persist(name: String, obj: Object): Unit = {}
  override def fetch[T](name: String): Option[T] = None
  override def expunge(name: String): Unit = {}
  override def fetchAll[T](): Iterable[T] = Iterable.empty[T]
}

/**
 * Zookeeper based Mesos cluster persistence engine, that stores cluster mode state
 * into Zookeeper. Each engine object is operating under one folder in Zookeeper, but
 * reuses a shared Zookeeper client.
 */
private[spark] class ZookeeperMesosClusterPersistenceEngine(
    baseDir: String,
    zk: CuratorFramework,
    conf: SparkConf)
  extends MesosClusterPersistenceEngine with Logging {
  private val WORKING_DIR =
    conf.get("spark.deploy.zookeeper.dir", "/spark_mesos_dispatcher") + "/" + baseDir

  SparkCuratorUtil.mkdir(zk, WORKING_DIR)

  def path(name: String): String = {
    WORKING_DIR + "/" + name
  }

  override def expunge(name: String): Unit = {
    zk.delete().forPath(path(name))
  }

  override def persist(name: String, obj: Object): Unit = {
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
    zk.getChildren.forPath(WORKING_DIR).asScala.flatMap(fetch[T])
  }
}
