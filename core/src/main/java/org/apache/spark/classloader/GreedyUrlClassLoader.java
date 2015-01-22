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

package org.apache.spark.classloader;

import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLStreamHandlerFactory;

/**
 * <p>
 * Instead of the usual delegate-first strategy employed by all the built-in classloaders, this one
 * calls findClass first (after checking the cache), then delegates.  The net effect is to allow
 * the classes that this classloader knows how to find and load itself (the URLs that are
 * registered with it) to shadow class defs of the same name from parent classloaders.
 * </p>
 * <p>
 * This shouldn't cause JVM class loader violations, because the world is still internally
 * consistent from the perspective of classes loaded by this CL.
 * </p>
 */
public class GreedyUrlClassLoader extends URLClassLoader {
  /*
  * It would be great to make this a Scala Trait, but I don't know of a way to write a static
  * initializer into a trait.  It's a class loader; timing matters.
  */
  static {
    //This spooky Java magic declares that we're smart enough to avoid class loading deadlocks.
    //Requires 1.7
    registerAsParallelCapable();
  }

  public GreedyUrlClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, parent);
  }

  public GreedyUrlClassLoader(URL[] urls, ClassLoader parent,
      URLStreamHandlerFactory factory) {
    super(urls, parent, factory);
  }

  @Override
  protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    synchronized (getClassLoadingLock(name)) {
      // First, check if the class has already been loaded
      Class c = findLoadedClass(name);
      if (c == null) {
        try {
          c = findClass(name);
        } catch (ClassNotFoundException ignored) {
        }

        if (c == null) {
          // Couldn't load it ourselves; delegate to the parent
          c = getParent().loadClass(name);
        }
      }
      if (resolve) {
        resolveClass(c);
      }
      return c;
    }
  }
}
