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

package org.apache.spark.util;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;

/**
 * A mutable class loader that gives preference to its own URLs over the parent class loader
 * when loading classes and resources.
 */
public class ChildFirstURLClassLoader extends MutableURLClassLoader {

  static {
    ClassLoader.registerAsParallelCapable();
  }

  private ParentClassLoader parent;

  public ChildFirstURLClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, null);
    this.parent = new ParentClassLoader(parent);
  }

  /**
   * Specify the grandparent if there is a need to load in the order of
   * `grandparent -&gt; urls (child) -&gt; parent`.
   */
  public ChildFirstURLClassLoader(URL[] urls, ClassLoader parent, ClassLoader grandparent) {
    super(urls, grandparent);
    this.parent = new ParentClassLoader(parent);
  }

  @Override
  public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    try {
      return super.loadClass(name, resolve);
    } catch (ClassNotFoundException cnf) {
      return parent.loadClass(name, resolve);
    }
  }

  @Override
  public Enumeration<URL> getResources(String name) throws IOException {
    ArrayList<URL> urls = Collections.list(super.getResources(name));
    urls.addAll(Collections.list(parent.getResources(name)));
    return Collections.enumeration(urls);
  }

  @Override
  public URL getResource(String name) {
    URL url = super.getResource(name);
    if (url != null) {
      return url;
    } else {
      return parent.getResource(name);
    }
  }
}
