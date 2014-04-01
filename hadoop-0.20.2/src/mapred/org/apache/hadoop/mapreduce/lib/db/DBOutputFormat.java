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

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

/**
 * A OutputFormat that sends the reduce output to a SQL table.
 * <p> 
 * {@link DBOutputFormat} accepts &lt;key,value&gt; pairs, where 
 * key has a type extending DBWritable. Returned {@link RecordWriter} 
 * writes <b>only the key</b> to the database with a batch SQL query.  
 * 
 */
public class DBOutputFormat<K  extends DBWritable, V> 
extends OutputFormat<K,V> {

  private static final Log LOG = LogFactory.getLog(DBOutputFormat.class);
  public void checkOutputSpecs(JobContext context) 
      throws IOException, InterruptedException {}

  public OutputCommitter getOutputCommitter(TaskAttemptContext context) 
      throws IOException, InterruptedException {
    return new FileOutputCommitter(FileOutputFormat.getOutputPath(context),
                                   context);
  }

  /**
   * A RecordWriter that writes the reduce output to a SQL table
   */
  public class DBRecordWriter 
      extends RecordWriter<K, V> {

    private Connection connection;
    private PreparedStatement statement;

    public DBRecordWriter() throws SQLException {
    }

    public DBRecordWriter(Connection connection
        , PreparedStatement statement) throws SQLException {
      this.connection = connection;
      this.statement = statement;
      this.connection.setAutoCommit(false);
    }

    public Connection getConnection() {
      return connection;
    }
    
    public PreparedStatement getStatement() {
      return statement;
    }
    
    /** {@inheritDoc} */
    public void close(TaskAttemptContext context) throws IOException {
      try {
        statement.executeBatch();
        connection.commit();
      } catch (SQLException e) {
        try {
          connection.rollback();
        }
        catch (SQLException ex) {
          LOG.warn(StringUtils.stringifyException(ex));
        }
        throw new IOException(e.getMessage());
      } finally {
        try {
          statement.close();
          connection.close();
        }
        catch (SQLException ex) {
          throw new IOException(ex.getMessage());
        }
      }
    }

    /** {@inheritDoc} */
    public void write(K key, V value) throws IOException {
      try {
        key.write(statement);
        statement.addBatch();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Constructs the query used as the prepared statement to insert data.
   * 
   * @param table
   *          the table to insert into
   * @param fieldNames
   *          the fields to insert into. If field names are unknown, supply an
   *          array of nulls.
   */
  public String constructQuery(String table, String[] fieldNames) {
    if(fieldNames == null) {
      throw new IllegalArgumentException("Field names may not be null");
    }

    StringBuilder query = new StringBuilder();
    query.append("INSERT INTO ").append(table);

    if (fieldNames.length > 0 && fieldNames[0] != null) {
      query.append(" (");
      for (int i = 0; i < fieldNames.length; i++) {
        query.append(fieldNames[i]);
        if (i != fieldNames.length - 1) {
          query.append(",");
        }
      }
      query.append(")");
    }
    query.append(" VALUES (");

    for (int i = 0; i < fieldNames.length; i++) {
      query.append("?");
      if(i != fieldNames.length - 1) {
        query.append(",");
      }
    }
    query.append(");");

    return query.toString();
  }

  /** {@inheritDoc} */
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) 
      throws IOException {
    DBConfiguration dbConf = new DBConfiguration(context.getConfiguration());
    String tableName = dbConf.getOutputTableName();
    String[] fieldNames = dbConf.getOutputFieldNames();
    
    if(fieldNames == null) {
      fieldNames = new String[dbConf.getOutputFieldCount()];
    }
    
    try {
      Connection connection = dbConf.getConnection();
      PreparedStatement statement = null;
  
      statement = connection.prepareStatement(
                    constructQuery(tableName, fieldNames));
      return new DBRecordWriter(connection, statement);
    } catch (Exception ex) {
      throw new IOException(ex.getMessage());
    }
  }

  /**
   * Initializes the reduce-part of the job with 
   * the appropriate output settings
   * 
   * @param job The job
   * @param tableName The table to insert data into
   * @param fieldNames The field names in the table.
   */
  public static void setOutput(Job job, String tableName, 
      String... fieldNames) throws IOException {
    if(fieldNames.length > 0 && fieldNames[0] != null) {
      DBConfiguration dbConf = setOutput(job, tableName);
      dbConf.setOutputFieldNames(fieldNames);
    } else {
      if (fieldNames.length > 0) {
        setOutput(job, tableName, fieldNames.length);
      }
      else { 
        throw new IllegalArgumentException(
          "Field names must be greater than 0");
      }
    }
  }
  
  /**
   * Initializes the reduce-part of the job 
   * with the appropriate output settings
   * 
   * @param job The job
   * @param tableName The table to insert data into
   * @param fieldCount the number of fields in the table.
   */
  public static void setOutput(Job job, String tableName, 
      int fieldCount) throws IOException {
    DBConfiguration dbConf = setOutput(job, tableName);
    dbConf.setOutputFieldCount(fieldCount);
  }
  
  private static DBConfiguration setOutput(Job job,
      String tableName) throws IOException {
    job.setOutputFormatClass(DBOutputFormat.class);
    job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);

    DBConfiguration dbConf = new DBConfiguration(job.getConfiguration());
    
    dbConf.setOutputTableName(tableName);
    return dbConf;
  }
}
