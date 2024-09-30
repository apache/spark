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

  static class PersonData {
    private String id;
    private String firstName;

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public String getFirstName() {
      return firstName;
    }

    public void setFirstName(String firstName) {
      this.firstName = firstName;
    }
  }

  static class PersonDataSerializable implements Serializable {
    private String id;
    private String firstName;

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public String getFirstName() {
      return firstName;
    }

    public void setFirstName(String firstName) {
      this.firstName = firstName;
    }
  }

  static class Team<P> {
    String name;
    P person;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public P getPerson() {
      return person;
    }

    public void setPerson(P person) {
      this.person = person;
    }
  }

  static class TeamT<T extends Serializable> {
    String name;
    T person;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public T getPerson() {
      return person;
    }

    public void setPerson(T person) {
      this.person = person;
    }
  }

  static class Company<T> {
    String name;
    Team<T> team;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public Team<T> getTeam() {
      return team;
    }

    public void setTeam(Team<T> team) {
      this.team = team;
    }
  }

  static class CompanyT<T extends Serializable> {
    String name;
    TeamT<T> team;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public TeamT<T> getTeam() {
      return team;
    }

    public void setTeam(TeamT<T> team) {
      this.team = team;
    }
  }

  static class CompanyWrapperT extends CompanyT<PersonDataSerializable> {
  }
  static class CompanyWrapper extends Company<PersonData> {
  }
}

