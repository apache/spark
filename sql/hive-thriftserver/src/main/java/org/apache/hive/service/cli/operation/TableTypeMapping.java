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

package org.apache.hive.service.cli.operation;

import java.util.Set;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface TableTypeMapping {
  /**
   * Map client's table type name to hive's table type
   * @param clientTypeName
   * @return
   */
  String[] mapToHiveType(String clientTypeName);

  /**
   * Map hive's table type name to client's table type
   * @param hiveTypeName
   * @return
   */
  String mapToClientType(String hiveTypeName);

  /**
   * Get all the table types of this mapping
   * @return
   */
  Set<String> getTableTypeNames();
}
