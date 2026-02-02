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

package org.apache.spark.sql.catalyst;

public class JavaTypeInferenceBeans {

  static class JavaBeanWithGenericsA<T> {
    public T getPropertyA() {
      return null;
    }

    public void setPropertyA(T a) {

    }
  }

  static class JavaBeanWithGenericsAB<T> extends JavaBeanWithGenericsA<String> {
    public T getPropertyB() {
      return null;
    }

    public void setPropertyB(T a) {

    }
  }

  static class JavaBeanWithGenericsABC<T> extends JavaBeanWithGenericsAB<Long> {
    public T getPropertyC() {
      return null;
    }

    public void setPropertyC(T a) {

    }
  }

  static class JavaBeanWithGenerics<T, A> {
    private A attribute;

    private T value;

    public A getAttribute() {
      return attribute;
    }

    public void setAttribute(A attribute) {
      this.attribute = attribute;
    }

    public T getValue() {
      return value;
    }

    public void setValue(T value) {
      this.value = value;
    }
  }

  static class JavaBeanWithGenericBase extends JavaBeanWithGenerics<String, String> {

  }

  static class JavaBeanWithGenericHierarchy extends JavaBeanWithGenericsABC<Integer> {

  }

  // SPARK-46679: Test classes for nested parameterized types with multi-level inheritance
  static class Foo<T> {
    private T t;

    public T getT() {
      return t;
    }

    public void setT(T t) {
      this.t = t;
    }
  }

  static class FooWrapper<U> {
    private Foo<U> foo;

    public Foo<U> getFoo() {
      return foo;
    }

    public void setFoo(Foo<U> foo) {
      this.foo = foo;
    }
  }

  static class StringFooWrapper extends FooWrapper<String> {
  }

  // SPARK-46679: Additional test classes for same type variable names at different levels
  static class StringBarWrapper extends BarWrapper<String> {
  }

  static class BarWrapper<T> {
    private Bar<T> bar;

    public Bar<T> getBar() {
      return bar;
    }

    public void setBar(Bar<T> bar) {
      this.bar = bar;
    }
  }

  static class Bar<T> {
    private T t;

    public T getT() {
      return t;
    }

    public void setT(T t) {
      this.t = t;
    }
  }
}

