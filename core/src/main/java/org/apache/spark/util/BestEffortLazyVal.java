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

import scala.Function0;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * A lock-free implementation of a lazily-initialized variable.
 * If there are concurrent initializations then the `compute()` function may be invoked
 * multiple times. However, only a single `compute()` result will be stored and all readers
 * will receive the same result object instance.
 *
 * This may be helpful for avoiding deadlocks in certain scenarios where exactly-once
 * value computation is not a hard requirement.
 *
 * @note
 * This helper class has additional requirements on the compute function:
 *   1) The compute function MUST not return null;
 *   2) The computation failure is not cached.
 *
 * @note
 *   Scala 3 uses a different implementation of lazy vals which doesn't have this problem.
 *   Please refer to <a
 *   href="https://docs.scala-lang.org/scala3/reference/changed-features/lazy-vals-init.html">Lazy
 *   Vals Initialization</a> for more details.
 */
public class BestEffortLazyVal<T> implements Serializable {
  private volatile Function0<T> compute;
  protected volatile T cached;

  private static final VarHandle HANDLE;
  static {
    try {
      HANDLE = MethodHandles.lookup()
        .in(BestEffortLazyVal.class)
        .findVarHandle(BestEffortLazyVal.class, "cached", Object.class);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Failed to initialize VarHandle", e);
    }
  }

  public BestEffortLazyVal(Function0<T> compute) {
    this.compute = compute;
  }

  public T apply() {
    T value = cached;
    if (value != null) {
      return value;
    }
    Function0<T> f = compute;
    if (f != null) {
      T newValue = f.apply();
      assert newValue != null: "compute function cannot return null.";
      HANDLE.compareAndSet(this, null, newValue);
      compute = null; // allow closure to be GC'd
    }
    return cached;
  }
}
