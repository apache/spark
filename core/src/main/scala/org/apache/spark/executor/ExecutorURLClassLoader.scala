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

package org.apache.spark.executor

import java.net.{URLClassLoader, URL}

/**
 * The addURL method in URLClassLoader is protected. We subclass it to make this accessible.
 * We also make changes so user classes can come before the default classes.
 */

private[spark] class ExecutorURLClassLoader(urls: Array[URL], parent: ClassLoader, userFirst: Boolean)
  extends ClassLoader {

  object userClassLoader extends URLClassLoader(urls, null){
    override def addURL(url: URL) {
      super.addURL(url)
    }
    override def findClass(name: String): Class[_] = { 
      super.findClass(name)
    }
  }

  object parentClassLoader extends ClassLoader(parent) {
    override def findClass(name: String): Class[_] = {
      super.findClass(name)
    }
  }

  override def findClass(name: String): Class[_] = {
    if (!userFirst) {
      try {
        val c = parentClassLoader.findClass(name)
        c
      } catch {
        case e: ClassNotFoundException => {
          userClassLoader.findClass(name)
        }
      }
    } else {
      try {
        userClassLoader.findClass(name)
      } catch {
        case e: ClassNotFoundException => {
          parentClassLoader.findClass(name)
        }
      }
    }
  }

  def addURL(url: URL) {
    userClassLoader.addURL(url)
  }

  def getURLs() = {
    userClassLoader.getURLs()
  }
}
