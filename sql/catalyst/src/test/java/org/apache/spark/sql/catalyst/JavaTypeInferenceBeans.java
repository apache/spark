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

import java.io.Serializable;

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
  static class FooT<T> {
    private T t;

    public T getT() {
      return t;
    }

    public void setT(T t) {
      this.t = t;
    }
  }

  static class InnerWrapperU<U> {
    private FooT<U> foo;

    public FooT<U> getFoo() {
      return foo;
    }

    public void setFoo(FooT<U> foo) {
      this.foo = foo;
    }
  }

  static class OuterWrapper extends InnerWrapperU<String> {
  }

  // Additional test classes for same type variable names at different levels
  static class CompanyWrapperT extends CompanyWrapperGenericT<String> {
  }

  static class CompanyWrapperGenericT<T> {
    private CompanyInfoGenericT<T> companyInfo;

    public CompanyInfoGenericT<T> getCompanyInfo() {
      return companyInfo;
    }

    public void setCompanyInfo(CompanyInfoGenericT<T> companyInfo) {
      this.companyInfo = companyInfo;
    }
  }

  static class CompanyInfoGenericT<T> {
    private T companyId;

    public T getCompanyId() {
      return companyId;
    }

    public void setCompanyId(T companyId) {
      this.companyId = companyId;
    }
  }
}

