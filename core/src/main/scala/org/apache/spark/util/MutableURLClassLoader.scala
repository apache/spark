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

import java.net.{URLClassLoader, URL}
import java.util.Enumeration
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConversions._

/**
 * URL class loader that exposes the `addURL` and `getURLs` methods in URLClassLoader.
 */
private[spark] class MutableURLClassLoader(urls: Array[URL], parent: ClassLoader)
  extends URLClassLoader(urls, parent) {

  override def addURL(url: URL): Unit = {
    super.addURL(url)
  }

  override def getURLs(): Array[URL] = {
    super.getURLs()
  }

}

/**
 * A mutable class loader that gives preference to its own URLs over the parent class loader
 * when loading classes and resources.
 */
private[spark] class ChildFirstURLClassLoader(urls: Array[URL], parent: ClassLoader)
  extends MutableURLClassLoader(urls, null) {

  private val parentClassLoader = new ParentClassLoader(parent)

  /**
   * Used to implement fine-grained class loading locks similar to what is done by Java 7. This
   * prevents deadlock issues when using non-hierarchical class loaders.
   *
   * Note that due to some issues with implementing class loaders in
   * Scala, Java 7's `ClassLoader.registerAsParallelCapable` method is not called.
   */
  private val locks = new ConcurrentHashMap[String, Object]()

  override def loadClass(name: String, resolve: Boolean): Class[_] = {
    var lock = locks.get(name)
    if (lock == null) {
      val newLock = new Object()
      lock = locks.putIfAbsent(name, newLock)
      if (lock == null) {
        lock = newLock
      }
    }

    lock.synchronized {
      try {
        super.loadClass(name, resolve)
      } catch {
        case e: ClassNotFoundException =>
          parentClassLoader.loadClass(name, resolve)
      }
    }
  }

  override def getResource(name: String): URL = {
    val url = super.findResource(name)
    val res = if (url != null) url else parentClassLoader.getResource(name)
    res
  }

  override def getResources(name: String): Enumeration[URL] = {
    val urls = super.findResources(name)
    val res =
      if (urls != null && urls.hasMoreElements()) {
        urls
      } else {
        parentClassLoader.getResources(name)
      }
    res
  }

  override def addURL(url: URL) {
    super.addURL(url)
  }

}
