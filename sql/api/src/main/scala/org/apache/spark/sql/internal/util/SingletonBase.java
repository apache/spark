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

package org.apache.spark.sql.internal.util;

/**
 * Generic abstract base class for implementing the singleton design pattern. This class is lazily
 * initialized, thread-safe, and generic (it can be used to create singletons for any type `T`).
 */
public abstract class SingletonBase<T> {
  // The singleton `instance` is created lazily, meaning that it is not instantiated until the
  // `getInstance()` method is called for the first time. Note that this solution is thread-safe.
  private static volatile Object instance = null;

  // The `createInstance` method is abstract, so all subclasses must implement it. Note that
  // this method defines how the singleton instance is created for the specific subclass.
  protected abstract T createInstance();

  // The `getInstance` method uses double-checked locking to ensure efficient and safe instance
  // creation. The singleton instance is created only once, even in a multithreaded environment.
  @SuppressWarnings("unchecked")
  public static <T> T getInstance(SingletonBase<T> singletonBase) {
    if (instance == null) {
      synchronized (SingletonBase.class) {
        if (instance == null) {
          instance = singletonBase.createInstance();
        }
      }
    }
    return (T) instance;
  }
}
