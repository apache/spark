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

package org.apache.spark.status.api.v1;

import org.apache.spark.util.EnumUtil;

import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public enum TaskSorting {
  ID,
  INCREASING_RUNTIME("runtime"),
  DECREASING_RUNTIME("-runtime");

  private final Set<String> alternateNames;
  TaskSorting(String... names) {
    alternateNames = new HashSet<>();
    Collections.addAll(alternateNames, names);
  }

  public static TaskSorting fromString(String str) {
    String lower = str.toLowerCase(Locale.ROOT);
    for (TaskSorting t: values()) {
      if (t.alternateNames.contains(lower)) {
        return t;
      }
    }
    return EnumUtil.parseIgnoreCase(TaskSorting.class, str);
  }

}
