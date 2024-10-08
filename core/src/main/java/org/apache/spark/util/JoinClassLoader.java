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

import java.net.URL;

/**
 * A class loader that first tries one class loader and then another.
 */
public class JoinClassLoader extends ChildFirstURLClassLoader {

  static {
    ClassLoader.registerAsParallelCapable();
  }

  /**
   * Initialize a JoinClassLoader, Load order is `first -&gt; last`.
   */
  public JoinClassLoader(ClassLoader first, ClassLoader last) {
    super(new URL[0], last, first);
  }
}
