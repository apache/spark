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
package org.apache.hadoop.http;

import java.util.Map;

/**
 * A container class for javax.servlet.Filter. 
 */
public interface FilterContainer {
  /**
   * Add a filter to the container.
   * @param name Filter name
   * @param classname Filter class name
   * @param parameters a map from parameter names to initial values
   */
  void addFilter(String name, String classname, Map<String, String> parameters);
  /**
   * Add a global filter to the container.
   * @param name filter name
   * @param classname filter class name
   * @param parameters a map from parameter names to initial values
   */
  void addGlobalFilter(String name, String classname, Map<String, String> parameters);
}
