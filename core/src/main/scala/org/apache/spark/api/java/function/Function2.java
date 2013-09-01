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

package org.apache.spark.api.java.function;

import scala.reflect.ClassManifest;
import scala.reflect.ClassManifest$;
import scala.runtime.AbstractFunction2;

import java.io.Serializable;

/**
 * A two-argument function that takes arguments of type T1 and T2 and returns an R.
 */
public abstract class Function2<T1, T2, R> extends WrappedFunction2<T1, T2, R>
  implements Serializable {

  public abstract R call(T1 t1, T2 t2) throws Exception;

  public ClassManifest<R> returnType() {
    return (ClassManifest<R>) ClassManifest$.MODULE$.fromClass(Object.class);
  }
}

