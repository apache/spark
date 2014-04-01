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

package org.apache.hadoop.mapred.lib.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Progressable;

public class DBOutputFormat<K  extends DBWritable, V> 
    extends org.apache.hadoop.mapreduce.lib.db.DBOutputFormat<K, V>
    implements OutputFormat<K, V> {

  /**
   * A RecordWriter that writes the reduce output to a SQL table
   */
  protected class DBRecordWriter extends 
      org.apache.hadoop.mapreduce.lib.db.DBOutputFormat<K, V>.DBRecordWriter
      implements RecordWriter<K, V> {

    protected DBRecordWriter(Connection connection, 
      PreparedStatement statement) throws SQLException {
      super(connection, statement);
    }

    /** {@inheritDoc} */
    public void close(Reporter reporter) throws IOException {
      super.close(null);
    }
  }

  /** {@inheritDoc} */
  public void checkOutputSpecs(FileSystem filesystem, JobConf job)
  throws IOException {
  }


  /** {@inheritDoc} */
  public RecordWriter<K, V> getRecordWriter(FileSystem filesystem,
      JobConf job, String name, Progressable progress) throws IOException {
    org.apache.hadoop.mapreduce.RecordWriter<K, V> w = super.getRecordWriter(
      new TaskAttemptContext(job, 
            TaskAttemptID.forName(job.get("mapred.task.id"))));
    org.apache.hadoop.mapreduce.lib.db.DBOutputFormat.DBRecordWriter writer = 
     (org.apache.hadoop.mapreduce.lib.db.DBOutputFormat.DBRecordWriter) w;
    try {
      return new DBRecordWriter(writer.getConnection(), writer.getStatement());
    } catch(SQLException se) {
      throw new IOException(se);
    }
  }

  /**
   * Initializes the reduce-part of the job with the appropriate output settings
   * 
   * @param job The job
   * @param tableName The table to insert data into
   * @param fieldNames The field names in the table.
   */
  public static void setOutput(JobConf job, String tableName, String... fieldNames) {
    if(fieldNames.length > 0 && fieldNames[0] != null) {
      DBConfiguration dbConf = setOutput(job, tableName);
      dbConf.setOutputFieldNames(fieldNames);
    } else {
      if(fieldNames.length > 0)
        setOutput(job, tableName, fieldNames.length);
      else 
        throw new IllegalArgumentException("Field names must be greater than 0");
    }
  }
  
  /**
   * Initializes the reduce-part of the job with the appropriate output settings
   * 
   * @param job The job
   * @param tableName The table to insert data into
   * @param fieldCount the number of fields in the table.
   */
  public static void setOutput(JobConf job, String tableName, int fieldCount) {
    DBConfiguration dbConf = setOutput(job, tableName);
    dbConf.setOutputFieldCount(fieldCount);
  }
  
  private static DBConfiguration setOutput(JobConf job, String tableName) {
    job.setOutputFormat(DBOutputFormat.class);
    job.setReduceSpeculativeExecution(false);

    DBConfiguration dbConf = new DBConfiguration(job);
    
    dbConf.setOutputTableName(tableName);
    return dbConf;
  }
  
}
