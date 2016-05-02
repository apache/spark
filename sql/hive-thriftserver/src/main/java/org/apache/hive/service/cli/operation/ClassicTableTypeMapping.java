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

package org.apache.hive.service.cli.operation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.metastore.TableType;

/**
 * ClassicTableTypeMapping.
 * Classic table type mapping :
 *  Managed Table ==> Table
 *  External Table ==> Table
 *  Virtual View ==> View
 */
public class ClassicTableTypeMapping implements TableTypeMapping {

  public enum ClassicTableTypes {
    TABLE,
    VIEW,
  }

  private final Map<String, String> hiveToClientMap = new HashMap<String, String>();
  private final Map<String, String> clientToHiveMap = new HashMap<String, String>();

  public ClassicTableTypeMapping () {
    hiveToClientMap.put(TableType.MANAGED_TABLE.toString(),
        ClassicTableTypes.TABLE.toString());
    hiveToClientMap.put(TableType.EXTERNAL_TABLE.toString(),
        ClassicTableTypes.TABLE.toString());
    hiveToClientMap.put(TableType.VIRTUAL_VIEW.toString(),
        ClassicTableTypes.VIEW.toString());

    clientToHiveMap.put(ClassicTableTypes.TABLE.toString(),
        TableType.MANAGED_TABLE.toString());
    clientToHiveMap.put(ClassicTableTypes.VIEW.toString(),
        TableType.VIRTUAL_VIEW.toString());
  }

  @Override
  public String mapToHiveType(String clientTypeName) {
    if (clientToHiveMap.containsKey(clientTypeName)) {
      return clientToHiveMap.get(clientTypeName);
    } else {
      return clientTypeName;
    }
  }

  @Override
  public String mapToClientType(String hiveTypeName) {
    if (hiveToClientMap.containsKey(hiveTypeName)) {
      return hiveToClientMap.get(hiveTypeName);
    } else {
      return hiveTypeName;
    }
  }

  @Override
  public Set<String> getTableTypeNames() {
    Set<String> typeNameSet = new HashSet<String>();
    for (ClassicTableTypes typeNames : ClassicTableTypes.values()) {
      typeNameSet.add(typeNames.toString());
    }
    return typeNameSet;
  }

}
