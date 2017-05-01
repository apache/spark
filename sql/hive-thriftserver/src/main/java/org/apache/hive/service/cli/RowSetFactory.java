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

package org.apache.hive.service.cli;

import org.apache.hive.service.cli.thrift.TProtocolVersion;
import org.apache.hive.service.cli.thrift.TRowSet;

import static org.apache.hive.service.cli.thrift.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6;

public class RowSetFactory {

  public static RowSet create(TableSchema schema, TProtocolVersion version) {
    if (version.getValue() >= HIVE_CLI_SERVICE_PROTOCOL_V6.getValue()) {
      return new ColumnBasedSet(schema);
    }
    return new RowBasedSet(schema);
  }

  public static RowSet create(TRowSet results, TProtocolVersion version) {
    if (version.getValue() >= HIVE_CLI_SERVICE_PROTOCOL_V6.getValue()) {
      return new ColumnBasedSet(results);
    }
    return new RowBasedSet(results);
  }
}
