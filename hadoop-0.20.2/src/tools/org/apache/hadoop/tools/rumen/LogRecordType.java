/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.tools.rumen;

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

class LogRecordType {
  static Map<String, LogRecordType> internees = new HashMap<String, LogRecordType>();

  final String name;

  final int index;

  private LogRecordType(String name) {
    super();

    this.name = name;

    index = internees.size();
  }

  static LogRecordType intern(String typeName) {
    LogRecordType result = internees.get(typeName);

    if (result == null) {
      result = new LogRecordType(typeName);

      internees.put(typeName, result);
    }

    return result;
  }

  static LogRecordType internSoft(String typeName) {
    return internees.get(typeName);
  }

  @Override
  public String toString() {
    return name;
  }

  static String[] lineTypes() {
    Iterator<Map.Entry<String, LogRecordType>> iter = internees.entrySet()
        .iterator();

    String[] result = new String[internees.size()];

    for (int i = 0; i < internees.size(); ++i) {
      result[i] = iter.next().getKey();
    }

    return result;
  }
}
