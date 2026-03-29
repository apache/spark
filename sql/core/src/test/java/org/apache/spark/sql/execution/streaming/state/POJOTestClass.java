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

package org.apache.spark.sql.execution.streaming.state;

/**
 * A POJO class used for tests of arbitrary state SQL encoder.
 */
public class POJOTestClass {
  // Fields
  private String name;
  private int id;

  // Constructors
  public POJOTestClass() {
    // Default constructor
  }

  public POJOTestClass(String name, int id) {
    this.name = name;
    this.id = id;
  }

  // Getter methods
  public String getName() {
    return name;
  }

  public int getId() {
    return id;
  }

  // Setter methods
  public void setName(String name) {
    this.name = name;
  }

  public void setId(int id) {
    this.id = id;
  }

  // Additional methods if needed
  public void incrementId() {
    id++;
    System.out.println(name + " is now " + id + "!");
  }

  // Override toString for better representation
  @Override
  public String toString() {
    return "POJOTestClass{" +
      "name='" + name + '\'' +
      ", age=" + id +
      '}';
  }

  // Override equals and hashCode for custom equality
  @Override
  public boolean equals(Object obj) {
    POJOTestClass testObj = (POJOTestClass) obj;
    return id == testObj.id && name.equals(testObj.name);
  }
}

