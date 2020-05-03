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

package org.apache.spark.util.kvstore;

public class CustomType1 {

  @KVIndex
  public String key;

  @KVIndex("id")
  public String id;

  @KVIndex(value = "name", copy = true)
  public String name;

  @KVIndex("int")
  public int num;

  @KVIndex(value = "child", parent = "id")
  public String child;

  @Override
  public boolean equals(Object o) {
    if (o instanceof CustomType1) {
      CustomType1 other = (CustomType1) o;
      return id.equals(other.id) && name.equals(other.name);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public String toString() {
    return "CustomType1[key=" + key + ",id=" + id + ",name=" + name + ",num=" + num;
  }

}
