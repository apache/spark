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

package org.apache.hadoop.mapreduce.lib.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;

/**
 * A RecordReader that reads records from a Oracle table via DataDrivenDBRecordReader
 */
public class OracleDataDrivenDBRecordReader<T extends DBWritable>
    extends DataDrivenDBRecordReader<T> {

  public OracleDataDrivenDBRecordReader(DBInputFormat.DBInputSplit split,
      Class<T> inputClass, Configuration conf, Connection conn,
      DBConfiguration dbConfig, String cond, String [] fields,
      String table) throws SQLException {

    super(split, inputClass, conf, conn, dbConfig, cond, fields, table,
        "ORACLE");

    // Must initialize the tz used by the connection for Oracle.
    OracleDBRecordReader.setSessionTimeZone(conf, conn);
  }
}
