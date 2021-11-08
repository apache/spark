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

package org.apache.spark.serializer;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

class ContainsProxyClass implements Serializable {
  final MyInterface proxy = (MyInterface) Proxy.newProxyInstance(
    MyInterface.class.getClassLoader(),
    new Class[]{MyInterface.class},
    new MyInvocationHandler());

  // Interface needs to be public as classloaders will mismatch.
  // See ObjectInputStream#resolveProxyClass for details.
  public interface MyInterface {
    void myMethod();
  }

  static class MyClass implements MyInterface, Serializable {
    @Override
    public void myMethod() {}
  }

  class MyInvocationHandler implements InvocationHandler, Serializable {
    private final MyClass real = new MyClass();

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      return method.invoke(real, args);
    }
  }
}
