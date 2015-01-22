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
 * A ClassLoader trait that changes the normal delegation scheme.
 * Normally, it's (cache, parent, self).  GreedyClassLoader uses
 * (cache, self, parent).  This lets this CL "hide" or "override"
 * class defs that also exist in the parent loader.
 */
private[spark] trait GreedyClassLoader extends ClassLoader {
  override def loadClass(name: String, resolve: Boolean): Class[_] = {
    this.synchronized {
      // Check the cache
      var c: Class[_] = findLoadedClass(name)
      if (c == null) {
        try {
          // Try loading it ourselves
          c = findClass(name)
        } catch {
          case ignored: ClassNotFoundException =>
        }

        if (c == null) {
          // Finally, try delegating to the parent
          c = getParent.loadClass(name)
        }
      }
      if (resolve) {
        resolveClass(c)
      }
      c
    }
  }
}

/**
 * The addURL method in URLClassLoader is protected. We subclass it to make this accessible.
 * We also make changes so user classes can come before the default classes.
 */
private[spark] trait MutableURLClassLoader extends ClassLoader {
  def addURL(url: URL) //XXX: this is sketchy and dangerous.  ClassLoader instances aren't designed to be mutable.
  def getURLs: Array[URL]
}

private[spark] class ChildExecutorURLClassLoader(urls: Array[URL], parent: ClassLoader)
  extends URLClassLoader(urls, parent) with GreedyClassLoader with MutableURLClassLoader {
}

private[spark] class ExecutorURLClassLoader(urls: Array[URL], parent: ClassLoader)
  extends URLClassLoader(urls, parent) with MutableURLClassLoader {
}
