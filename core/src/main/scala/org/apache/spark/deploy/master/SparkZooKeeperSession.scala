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
import scala.concurrent.ops._

import org.apache.spark.Logging
import org.apache.zookeeper._
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.Watcher.Event.KeeperState

/**
 * Provides a Scala-side interface to the standard ZooKeeper client, with the addition of retry
 * logic. If the ZooKeeper session expires or otherwise dies, a new ZooKeeper session will be
 * created. If ZooKeeper remains down after several retries, the given
 * [[org.apache.spark.deploy.master.SparkZooKeeperWatcher SparkZooKeeperWatcher]] will be
 * informed via zkDown().
 *
 * Additionally, all commands sent to ZooKeeper will be retried until they either fail too many
 * times or a semantic exception is thrown (e.g.., "node already exists").
 */
private[spark] class SparkZooKeeperSession(zkWatcher: SparkZooKeeperWatcher) extends Logging {
  val ZK_URL = System.getProperty("spark.deploy.zookeeper.url", "")

  val ZK_ACL = ZooDefs.Ids.OPEN_ACL_UNSAFE
  val ZK_TIMEOUT_MILLIS = 30000
  val RETRY_WAIT_MILLIS = 5000
  val ZK_CHECK_PERIOD_MILLIS = 10000
  val MAX_RECONNECT_ATTEMPTS = 3

  private var zk: ZooKeeper = _

  private val watcher = new ZooKeeperWatcher()
  private var reconnectAttempts = 0
  private var closed = false

  /** Connect to ZooKeeper to start the session. Must be called before anything else. */
  def connect() {
    connectToZooKeeper()

    new Thread() {
      override def run() = sessionMonitorThread()
    }.start()
  }

  def sessionMonitorThread(): Unit = {
    while (!closed) {
      Thread.sleep(ZK_CHECK_PERIOD_MILLIS)
      if (zk.getState != ZooKeeper.States.CONNECTED) {
        reconnectAttempts += 1
        val attemptsLeft = MAX_RECONNECT_ATTEMPTS - reconnectAttempts
        if (attemptsLeft <= 0) {
          logError("Could not connect to ZooKeeper: system failure")
          zkWatcher.zkDown()
          close()
        } else {
          logWarning("ZooKeeper connection failed, retrying " + attemptsLeft + " more times...")
          connectToZooKeeper()
        }
      }
    }
  }

  def close() {
    if (!closed && zk != null) { zk.close() }
    closed = true
  }

  private def connectToZooKeeper() {
    if (zk != null) zk.close()
    zk = new ZooKeeper(ZK_URL, ZK_TIMEOUT_MILLIS, watcher)
  }

  /**
   * Attempts to maintain a live ZooKeeper exception despite (very) transient failures.
   * Mainly useful for handling the natural ZooKeeper session expiration.
   */
  private class ZooKeeperWatcher extends Watcher {
    def process(event: WatchedEvent) {
      if (closed) { return }

      event.getState match {
        case KeeperState.SyncConnected =>
          reconnectAttempts = 0
          zkWatcher.zkSessionCreated()
        case KeeperState.Expired =>
          connectToZooKeeper()
        case KeeperState.Disconnected =>
          logWarning("ZooKeeper disconnected, will retry...")
      }
    }
  }

  def create(path: String, bytes: Array[Byte], createMode: CreateMode): String = {
    retry {
      zk.create(path, bytes, ZK_ACL, createMode)
    }
  }

  def exists(path: String, watcher: Watcher = null): Stat = {
    retry {
      zk.exists(path, watcher)
    }
  }

  def getChildren(path: String, watcher: Watcher = null): List[String] = {
    retry {
      zk.getChildren(path, watcher).toList
    }
  }

  def getData(path: String): Array[Byte] = {
    retry {
      zk.getData(path, false, null)
    }
  }

  def delete(path: String, version: Int = -1): Unit = {
    retry {
      zk.delete(path, version)
    }
  }

  /**
   * Creates the given directory (non-recursively) if it doesn't exist.
   * All znodes are created in PERSISTENT mode with no data.
   */
  def mkdir(path: String) {
    if (exists(path) == null) {
      try {
        create(path, "".getBytes, CreateMode.PERSISTENT)
      } catch {
        case e: Exception =>
          // If the exception caused the directory not to be created, bubble it up,
          // otherwise ignore it.
          if (exists(path) == null) { throw e }
      }
    }
  }

  /**
   * Recursively creates all directories up to the given one.
   * All znodes are created in PERSISTENT mode with no data.
   */
  def mkdirRecursive(path: String) {
    var fullDir = ""
    for (dentry <- path.split("/").tail) {
      fullDir += "/" + dentry
      mkdir(fullDir)
    }
  }

  /**
   * Retries the given function up to 3 times. The assumption is that failure is transient,
   * UNLESS it is a semantic exception (i.e., trying to get data from a node that doesn't exist),
   * in which case the exception will be thrown without retries.
   *
   * @param fn Block to execute, possibly multiple times.
   */
  def retry[T](fn: => T, n: Int = MAX_RECONNECT_ATTEMPTS): T = {
    try {
      fn
    } catch {
      case e: KeeperException.NoNodeException => throw e
      case e: KeeperException.NodeExistsException => throw e
      case e if n > 0 =>
        logError("ZooKeeper exception, " + n + " more retries...", e)
        Thread.sleep(RETRY_WAIT_MILLIS)
        retry(fn, n-1)
    }
  }
}

trait SparkZooKeeperWatcher {
  /**
   * Called whenever a ZK session is created --
   * this will occur when we create our first session as well as each time
   * the session expires or errors out.
   */
  def zkSessionCreated()

  /**
   * Called if ZK appears to be completely down (i.e., not just a transient error).
   * We will no longer attempt to reconnect to ZK, and the SparkZooKeeperSession is considered dead.
   */
  def zkDown()
}
