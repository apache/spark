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
}

