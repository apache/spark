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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import org.apache.hadoop.hive.metastore.TableType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ClassicTableTypeMapping.
 * Classic table type mapping :
 *  Managed Table to Table
 *  External Table to Table
 *  Virtual View to View
 */
public class ClassicTableTypeMapping implements TableTypeMapping {

  private static final Logger LOG = LoggerFactory.getLogger(ClassicTableTypeMapping.class);

  public enum ClassicTableTypes {
    TABLE,
    VIEW,
    MATERIALIZED_VIEW,
  }

  private final Map<String, String> hiveToClientMap = new HashMap<String, String>();
  private final Multimap<String, String> clientToHiveMap = ArrayListMultimap.create();

  public ClassicTableTypeMapping() {
    hiveToClientMap.put(TableType.MANAGED_TABLE.name(), ClassicTableTypes.TABLE.name());
    hiveToClientMap.put(TableType.EXTERNAL_TABLE.name(), ClassicTableTypes.TABLE.name());
    hiveToClientMap.put(TableType.VIRTUAL_VIEW.name(), ClassicTableTypes.VIEW.name());
    hiveToClientMap.put(TableType.MATERIALIZED_VIEW.toString(),
            ClassicTableTypes.MATERIALIZED_VIEW.toString());

    clientToHiveMap.putAll(ClassicTableTypes.TABLE.name(), Arrays.asList(
        TableType.MANAGED_TABLE.name(), TableType.EXTERNAL_TABLE.name()));
    clientToHiveMap.put(ClassicTableTypes.VIEW.name(), TableType.VIRTUAL_VIEW.name());
    clientToHiveMap.put(ClassicTableTypes.MATERIALIZED_VIEW.toString(),
            TableType.MATERIALIZED_VIEW.toString());
  }

  @Override
  public String[] mapToHiveType(String clientTypeName) {
    Collection<String> hiveTableType = clientToHiveMap.get(clientTypeName.toUpperCase());
    if (hiveTableType == null) {
      LOG.warn("Not supported client table type " + clientTypeName);
      return new String[] {clientTypeName};
    }
    return Iterables.toArray(hiveTableType, String.class);
  }

  @Override
  public String mapToClientType(String hiveTypeName) {
    String clientTypeName = hiveToClientMap.get(hiveTypeName);
    if (clientTypeName == null) {
      LOG.warn("Invalid hive table type " + hiveTypeName);
      return hiveTypeName;
    }
    return clientTypeName;
  }

  @Override
  public Set<String> getTableTypeNames() {
    Set<String> typeNameSet = new HashSet<String>();
    for (ClassicTableTypes typeNames : ClassicTableTypes.values()) {
      typeNameSet.add(typeNames.name());
    }
    return typeNameSet;
  }

}
