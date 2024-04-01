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

package org.apache.spark.sql;

import org.apache.spark.unsafe.types.UTF8String;

public class StaticInvokeMethod {

  public static Integer parseInt1(UTF8String input) throws StaticInvokeException1 {
    if (input == null) {
      throw new StaticInvokeException1("input cannot be null");
    }
    return Integer.parseInt(input.toString());
  }

  public static Integer parseInt2(UTF8String input) throws
      StaticInvokeException1, StaticInvokeException2 {
    if (input == null) {
      throw new StaticInvokeException1("input cannot be null");
    }
    long longValue = Long.parseLong(input.toString());
    if (longValue > Integer.MAX_VALUE || longValue < Integer.MIN_VALUE) {
      throw new StaticInvokeException2("input must <= " + Integer.MAX_VALUE +
          " and " + " >= " + Integer.MIN_VALUE);
    }
    return Integer.parseInt(input.toString());
  }
}
