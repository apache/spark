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

import java.util.List;

public class IntKeyType {

  @KVIndex
  public int key;

  @KVIndex("id")
  public String id;

  public List<String> values;

  @Override
  public boolean equals(Object o) {
    if (o instanceof IntKeyType other) {
      return key == other.key && id.equals(other.id) && values.equals(other.values);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

}

