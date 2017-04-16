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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

/**
 * A RecordReader that reads records from a SQL table,
 * using data-driven WHERE clause splits.
 * Emits LongWritables containing the record number as
 * key and DBWritables as value.
 */
public class DataDrivenDBRecordReader<T extends DBWritable> extends DBRecordReader<T> {

  private static final Log LOG = LogFactory.getLog(DataDrivenDBRecordReader.class);

  private String dbProductName; // database manufacturer string.

  /**
   * @param split The InputSplit to read data for
   * @throws SQLException 
   */
  public DataDrivenDBRecordReader(DBInputFormat.DBInputSplit split,
      Class<T> inputClass, Configuration conf, Connection conn, DBConfiguration dbConfig,
      String cond, String [] fields, String table, String dbProduct)
      throws SQLException {
    super(split, inputClass, conf, conn, dbConfig, cond, fields, table);
    this.dbProductName = dbProduct;
  }

  /** Returns the query for selecting the records,
   * subclasses can override this for custom behaviour.*/
  @SuppressWarnings("unchecked")
  protected String getSelectQuery() {
    StringBuilder query = new StringBuilder();
    DataDrivenDBInputFormat.DataDrivenDBInputSplit dataSplit =
        (DataDrivenDBInputFormat.DataDrivenDBInputSplit) getSplit();
    DBConfiguration dbConf = getDBConf();
    String [] fieldNames = getFieldNames();
    String tableName = getTableName();
    String conditions = getConditions();

    // Build the WHERE clauses associated with the data split first.
    // We need them in both branches of this function.
    StringBuilder conditionClauses = new StringBuilder();
    conditionClauses.append("( ").append(dataSplit.getLowerClause());
    conditionClauses.append(" ) AND ( ").append(dataSplit.getUpperClause());
    conditionClauses.append(" )");

    if(dbConf.getInputQuery() == null) {
      // We need to generate the entire query.
      query.append("SELECT ");

      for (int i = 0; i < fieldNames.length; i++) {
        query.append(fieldNames[i]);
        if (i != fieldNames.length -1) {
          query.append(", ");
        }
      }

      query.append(" FROM ").append(tableName);
      if (!dbProductName.startsWith("ORACLE")) {
        // Seems to be necessary for hsqldb? Oracle explicitly does *not*
        // use this clause.
        query.append(" AS ").append(tableName);
      }
      query.append(" WHERE ");
      if (conditions != null && conditions.length() > 0) {
        // Put the user's conditions first.
        query.append("( ").append(conditions).append(" ) AND ");
      }

      // Now append the conditions associated with our split.
      query.append(conditionClauses.toString());

    } else {
      // User provided the query. We replace the special token with our WHERE clause.
      String inputQuery = dbConf.getInputQuery();
      if (inputQuery.indexOf(DataDrivenDBInputFormat.SUBSTITUTE_TOKEN) == -1) {
        LOG.error("Could not find the clause substitution token "
            + DataDrivenDBInputFormat.SUBSTITUTE_TOKEN + " in the query: ["
            + inputQuery + "]. Parallel splits may not work correctly.");
      }

      query.append(inputQuery.replace(DataDrivenDBInputFormat.SUBSTITUTE_TOKEN,
          conditionClauses.toString()));
    }

    LOG.debug("Using query: " + query.toString());

    return query.toString();
  }
}
