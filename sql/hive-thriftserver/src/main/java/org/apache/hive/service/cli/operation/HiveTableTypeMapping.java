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

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.metastore.TableType;

/**
 * HiveTableTypeMapping.
 * Default table type mapping
 *
 */
public class HiveTableTypeMapping implements TableTypeMapping {

  @Override
  public String[] mapToHiveType(String clientTypeName) {
    return new String[] {mapToClientType(clientTypeName)};
  }

  @Override
  public String mapToClientType(String hiveTypeName) {
    return hiveTypeName;
  }

  @Override
  public Set<String> getTableTypeNames() {
    Set<String> typeNameSet = new HashSet<String>();
    for (TableType typeNames : TableType.values()) {
      typeNameSet.add(typeNames.name());
    }
    return typeNameSet;
  }
}
