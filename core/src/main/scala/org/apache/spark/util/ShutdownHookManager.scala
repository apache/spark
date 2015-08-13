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

package org.apache.spark.util

import java.io.File
import java.util.PriorityQueue

import scala.util.{Failure, Success, Try}
import tachyon.client.TachyonFile

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.Logging

/**
 * Various utility methods used by Spark.
 */
private[spark] object ShutdownHookManager extends Logging {
  val DEFAULT_SHUTDOWN_PRIORITY = 100

  /**
   * The shutdown priority of the SparkContext instance. This is lower than the default
   * priority, so that by default hooks are run before the context is shut down.
   */
  val SPARK_CONTEXT_SHUTDOWN_PRIORITY = 50

  /**
   * The shutdown priority of temp directory must be lower than the SparkContext shutdown
   * priority. Otherwise cleaning the temp directories while Spark jobs are running can
   * throw undesirable errors at the time of shutdown.
   */
  val TEMP_DIR_SHUTDOWN_PRIORITY = 25

  private lazy val shutdownHooks = {
    val manager = new SparkShutdownHookManager()
    manager.install()
    manager
  }

  private val shutdownDeletePaths = new scala.collection.mutable.HashSet[String]()
  private val shutdownDeleteTachyonPaths = new scala.collection.mutable.HashSet[String]()

  // Add a shutdown hook to delete the temp dirs when the JVM exits
  addShutdownHook(TEMP_DIR_SHUTDOWN_PRIORITY) { () =>
    logInfo("Shutdown hook called")
    shutdownDeletePaths.foreach { dirPath =>
      try {
        logInfo("Deleting directory " + dirPath)
        Utils.deleteRecursively(new File(dirPath))
      } catch {
        case e: Exception => logError(s"Exception while deleting Spark temp dir: $dirPath", e)
      }
    }
  }

  // Register the path to be deleted via shutdown hook
  def registerShutdownDeleteDir(file: File) {
    val absolutePath = file.getAbsolutePath()
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths += absolutePath
    }
  }

  // Register the tachyon path to be deleted via shutdown hook
  def registerShutdownDeleteDir(tachyonfile: TachyonFile) {
    val absolutePath = tachyonfile.getPath()
    shutdownDeleteTachyonPaths.synchronized {
      shutdownDeleteTachyonPaths += absolutePath
    }
  }

  // Remove the path to be deleted via shutdown hook
  def removeShutdownDeleteDir(file: File) {
    val absolutePath = file.getAbsolutePath()
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths.remove(absolutePath)
    }
  }

  // Remove the tachyon path to be deleted via shutdown hook
  def removeShutdownDeleteDir(tachyonfile: TachyonFile) {
    val absolutePath = tachyonfile.getPath()
    shutdownDeleteTachyonPaths.synchronized {
      shutdownDeleteTachyonPaths.remove(absolutePath)
    }
  }

  // Is the path already registered to be deleted via a shutdown hook ?
  def hasShutdownDeleteDir(file: File): Boolean = {
    val absolutePath = file.getAbsolutePath()
    shutdownDeletePaths.synchronized {
      shutdownDeletePaths.contains(absolutePath)
    }
  }

  // Is the path already registered to be deleted via a shutdown hook ?
  def hasShutdownDeleteTachyonDir(file: TachyonFile): Boolean = {
    val absolutePath = file.getPath()
    shutdownDeleteTachyonPaths.synchronized {
      shutdownDeleteTachyonPaths.contains(absolutePath)
    }
  }

  // Note: if file is child of some registered path, while not equal to it, then return true;
  // else false. This is to ensure that two shutdown hooks do not try to delete each others
  // paths - resulting in IOException and incomplete cleanup.
  def hasRootAsShutdownDeleteDir(file: File): Boolean = {
    val absolutePath = file.getAbsolutePath()
    val retval = shutdownDeletePaths.synchronized {
      shutdownDeletePaths.exists { path =>
        !absolutePath.equals(path) && absolutePath.startsWith(path)
      }
    }
    if (retval) {
      logInfo("path = " + file + ", already present as root for deletion.")
    }
    retval
  }

  // Note: if file is child of some registered path, while not equal to it, then return true;
  // else false. This is to ensure that two shutdown hooks do not try to delete each others
  // paths - resulting in Exception and incomplete cleanup.
  def hasRootAsShutdownDeleteDir(file: TachyonFile): Boolean = {
    val absolutePath = file.getPath()
    val retval = shutdownDeleteTachyonPaths.synchronized {
      shutdownDeleteTachyonPaths.exists { path =>
        !absolutePath.equals(path) && absolutePath.startsWith(path)
      }
    }
    if (retval) {
      logInfo("path = " + file + ", already present as root for deletion.")
    }
    retval
  }

  /**
   * Detect whether this thread might be executing a shutdown hook. Will always return true if
   * the current thread is a running a shutdown hook but may spuriously return true otherwise (e.g.
   * if System.exit was just called by a concurrent thread).
   *
   * Currently, this detects whether the JVM is shutting down by Runtime#addShutdownHook throwing
   * an IllegalStateException.
   */
  def inShutdown(): Boolean = {
    try {
      val hook = new Thread {
        override def run() {}
      }
      Runtime.getRuntime.addShutdownHook(hook)
      Runtime.getRuntime.removeShutdownHook(hook)
    } catch {
      case ise: IllegalStateException => return true
    }
    false
  }

  /**
   * Adds a shutdown hook with default priority.
   *
   * @param hook The code to run during shutdown.
   * @return A handle that can be used to unregister the shutdown hook.
   */
  def addShutdownHook(hook: () => Unit): AnyRef = {
    addShutdownHook(DEFAULT_SHUTDOWN_PRIORITY)(hook)
  }

  /**
   * Adds a shutdown hook with the given priority. Hooks with lower priority values run
   * first.
   *
   * @param hook The code to run during shutdown.
   * @return A handle that can be used to unregister the shutdown hook.
   */
  def addShutdownHook(priority: Int)(hook: () => Unit): AnyRef = {
    shutdownHooks.add(priority, hook)
  }

  /**
   * Remove a previously installed shutdown hook.
   *
   * @param ref A handle returned by `addShutdownHook`.
   * @return Whether the hook was removed.
   */
  def removeShutdownHook(ref: AnyRef): Boolean = {
    shutdownHooks.remove(ref)
  }

}

private [util] class SparkShutdownHookManager {

  private val hooks = new PriorityQueue[SparkShutdownHook]()
  private var shuttingDown = false

  /**
   * Install a hook to run at shutdown and run all registered hooks in order. Hadoop 1.x does not
   * have `ShutdownHookManager`, so in that case we just use the JVM's `Runtime` object and hope for
   * the best.
   */
  def install(): Unit = {
    val hookTask = new Runnable() {
      override def run(): Unit = runAll()
    }
    Try(Utils.classForName("org.apache.hadoop.util.ShutdownHookManager")) match {
      case Success(shmClass) =>
        val fsPriority = classOf[FileSystem].getField("SHUTDOWN_HOOK_PRIORITY").get()
          .asInstanceOf[Int]
        val shm = shmClass.getMethod("get").invoke(null)
        shm.getClass().getMethod("addShutdownHook", classOf[Runnable], classOf[Int])
          .invoke(shm, hookTask, Integer.valueOf(fsPriority + 30))

      case Failure(_) =>
        Runtime.getRuntime.addShutdownHook(new Thread(hookTask, "Spark Shutdown Hook"));
    }
  }

  def runAll(): Unit = synchronized {
    shuttingDown = true
    while (!hooks.isEmpty()) {
      Try(Utils.logUncaughtExceptions(hooks.poll().run()))
    }
  }

  def add(priority: Int, hook: () => Unit): AnyRef = synchronized {
    checkState()
    val hookRef = new SparkShutdownHook(priority, hook)
    hooks.add(hookRef)
    hookRef
  }

  def remove(ref: AnyRef): Boolean = synchronized {
    hooks.remove(ref)
  }

  private def checkState(): Unit = {
    if (shuttingDown) {
      throw new IllegalStateException("Shutdown hooks cannot be modified during shutdown.")
    }
  }

}

private class SparkShutdownHook(private val priority: Int, hook: () => Unit)
  extends Comparable[SparkShutdownHook] {

  override def compareTo(other: SparkShutdownHook): Int = {
    other.priority - priority
  }

  def run(): Unit = hook()

}
