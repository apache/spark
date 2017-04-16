/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.io;

import java.util.HashMap;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

/** Utility to permit renaming of Writable implementation classes without
 * invalidiating files that contain their class name.
 */
public class WritableName {
  private static HashMap<String, Class<?>> NAME_TO_CLASS =
    new HashMap<String, Class<?>>();
  private static HashMap<Class<?>, String> CLASS_TO_NAME =
    new HashMap<Class<?>, String>();

  static {                                        // define important types
    WritableName.setName(NullWritable.class, "null");
    WritableName.setName(LongWritable.class, "long");
    WritableName.setName(UTF8.class, "UTF8");
    WritableName.setName(MD5Hash.class, "MD5Hash");
  }

  private WritableName() {}                      // no public ctor

  /** Set the name that a class should be known as to something other than the
   * class name. */
  public static synchronized void setName(Class writableClass, String name) {
    CLASS_TO_NAME.put(writableClass, name);
    NAME_TO_CLASS.put(name, writableClass);
  }

  /** Add an alternate name for a class. */
  public static synchronized void addName(Class writableClass, String name) {
    NAME_TO_CLASS.put(name, writableClass);
  }

  /** Return the name for a class.  Default is {@link Class#getName()}. */
  public static synchronized String getName(Class writableClass) {
    String name = CLASS_TO_NAME.get(writableClass);
    if (name != null)
      return name;
    return writableClass.getName();
  }

  /** Return the class for a name.  Default is {@link Class#forName(String)}.*/
  public static synchronized Class<?> getClass(String name, Configuration conf
                                            ) throws IOException {
    Class<?> writableClass = NAME_TO_CLASS.get(name);
    if (writableClass != null)
      return writableClass.asSubclass(Writable.class);
    try {
      return conf.getClassByName(name);
    } catch (ClassNotFoundException e) {
      IOException newE = new IOException("WritableName can't load class: " + name);
      newE.initCause(e);
      throw newE;
    }
  }

}
