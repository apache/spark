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
 * A InputFormat that reads input data from an SQL table.
 * <p>
 * DBInputFormat emits LongWritables containing the record number as 
 * key and DBWritables as value. 
 * 
 * The SQL query, and input class can be using one of the two 
 * setInput methods.
 */
public class DBInputFormat<T extends DBWritable>
    extends InputFormat<LongWritable, T> implements Configurable {

  private String dbProductName = "DEFAULT";

  /**
   * A Class that does nothing, implementing DBWritable
   */
  public static class NullDBWritable implements DBWritable, Writable {
    @Override
    public void readFields(DataInput in) throws IOException { }
    @Override
    public void readFields(ResultSet arg0) throws SQLException { }
    @Override
    public void write(DataOutput out) throws IOException { }
    @Override
    public void write(PreparedStatement arg0) throws SQLException { }
  }
  
  /**
   * A InputSplit that spans a set of rows
   */
  public static class DBInputSplit extends InputSplit implements Writable {

    private long end = 0;
    private long start = 0;

    /**
     * Default Constructor
     */
    public DBInputSplit() {
    }

    /**
     * Convenience Constructor
     * @param start the index of the first row to select
     * @param end the index of the last row to select
     */
    public DBInputSplit(long start, long end) {
      this.start = start;
      this.end = end;
    }

    /** {@inheritDoc} */
    public String[] getLocations() throws IOException {
      // TODO Add a layer to enable SQL "sharding" and support locality
      return new String[] {};
    }

    /**
     * @return The index of the first row to select
     */
    public long getStart() {
      return start;
    }

    /**
     * @return The index of the last row to select
     */
    public long getEnd() {
      return end;
    }

    /**
     * @return The total row count in this split
     */
    public long getLength() throws IOException {
      return end - start;
    }

    /** {@inheritDoc} */
    public void readFields(DataInput input) throws IOException {
      start = input.readLong();
      end = input.readLong();
    }

    /** {@inheritDoc} */
    public void write(DataOutput output) throws IOException {
      output.writeLong(start);
      output.writeLong(end);
    }
  }

  private String conditions;

  private Connection connection;

  private String tableName;

  private String[] fieldNames;

  private DBConfiguration dbConf;

  /** {@inheritDoc} */
  public void setConf(Configuration conf) {

    dbConf = new DBConfiguration(conf);

    try {
      getConnection();

      DatabaseMetaData dbMeta = connection.getMetaData();
      this.dbProductName = dbMeta.getDatabaseProductName().toUpperCase();
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }

    tableName = dbConf.getInputTableName();
    fieldNames = dbConf.getInputFieldNames();
    conditions = dbConf.getInputConditions();
  }

  public Configuration getConf() {
    return dbConf.getConf();
  }
  
  public DBConfiguration getDBConf() {
    return dbConf;
  }

  public Connection getConnection() {
    try {
      if (null == this.connection) {
        // The connection was closed; reinstantiate it.
        this.connection = dbConf.getConnection();
        this.connection.setAutoCommit(false);
        this.connection.setTransactionIsolation(
            Connection.TRANSACTION_SERIALIZABLE);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return connection;
  }

  public String getDBProductName() {
    return dbProductName;
  }

  protected RecordReader<LongWritable, T> createDBRecordReader(DBInputSplit split,
      Configuration conf) throws IOException {

    @SuppressWarnings("unchecked")
    Class<T> inputClass = (Class<T>) (dbConf.getInputClass());
    try {
      // use database product name to determine appropriate record reader.
      if (dbProductName.startsWith("ORACLE")) {
        // use Oracle-specific db reader.
        return new OracleDBRecordReader<T>(split, inputClass,
            conf, getConnection(), getDBConf(), conditions, fieldNames,
            tableName);
      } else if (dbProductName.startsWith("MYSQL")) {
        // use MySQL-specific db reader.
        return new MySQLDBRecordReader<T>(split, inputClass,
            conf, getConnection(), getDBConf(), conditions, fieldNames,
            tableName);
      } else {
        // Generic reader.
        return new DBRecordReader<T>(split, inputClass,
            conf, getConnection(), getDBConf(), conditions, fieldNames,
            tableName);
      }
    } catch (SQLException ex) {
      throw new IOException(ex.getMessage());
    }
  }

  /** {@inheritDoc} */
  @SuppressWarnings("unchecked")
  public RecordReader<LongWritable, T> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {  

    return createDBRecordReader((DBInputSplit) split, context.getConfiguration());
  }

  /** {@inheritDoc} */
  public List<InputSplit> getSplits(JobContext job) throws IOException {

    ResultSet results = null;  
    Statement statement = null;
    try {
      statement = connection.createStatement();

      results = statement.executeQuery(getCountQuery());
      results.next();

      long count = results.getLong(1);
      int chunks = job.getConfiguration().getInt("mapred.map.tasks", 1);
      long chunkSize = (count / chunks);

      results.close();
      statement.close();

      List<InputSplit> splits = new ArrayList<InputSplit>();

      // Split the rows into n-number of chunks and adjust the last chunk
      // accordingly
      for (int i = 0; i < chunks; i++) {
        DBInputSplit split;

        if ((i + 1) == chunks)
          split = new DBInputSplit(i * chunkSize, count);
        else
          split = new DBInputSplit(i * chunkSize, (i * chunkSize)
              + chunkSize);

        splits.add(split);
      }

      connection.commit();
      return splits;
    } catch (SQLException e) {
      throw new IOException("Got SQLException", e);
    } finally {
      try {
        if (results != null) { results.close(); }
      } catch (SQLException e1) {}
      try {
        if (statement != null) { statement.close(); }
      } catch (SQLException e1) {}

      closeConnection();
    }
  }

  /** Returns the query for getting the total number of rows, 
   * subclasses can override this for custom behaviour.*/
  protected String getCountQuery() {
    
    if(dbConf.getInputCountQuery() != null) {
      return dbConf.getInputCountQuery();
    }
    
    StringBuilder query = new StringBuilder();
    query.append("SELECT COUNT(*) FROM " + tableName);

    if (conditions != null && conditions.length() > 0)
      query.append(" WHERE " + conditions);
    return query.toString();
  }

  /**
   * Initializes the map-part of the job with the appropriate input settings.
   * 
   * @param job The map-reduce job
   * @param inputClass the class object implementing DBWritable, which is the 
   * Java object holding tuple fields.
   * @param tableName The table to read data from
   * @param conditions The condition which to select data with, 
   * eg. '(updated > 20070101 AND length > 0)'
   * @param orderBy the fieldNames in the orderBy clause.
   * @param fieldNames The field names in the table
   * @see #setInput(Job, Class, String, String)
   */
  public static void setInput(Job job, 
      Class<? extends DBWritable> inputClass,
      String tableName,String conditions, 
      String orderBy, String... fieldNames) {
    job.setInputFormatClass(DBInputFormat.class);
    DBConfiguration dbConf = new DBConfiguration(job.getConfiguration());
    dbConf.setInputClass(inputClass);
    dbConf.setInputTableName(tableName);
    dbConf.setInputFieldNames(fieldNames);
    dbConf.setInputConditions(conditions);
    dbConf.setInputOrderBy(orderBy);
  }
  
  /**
   * Initializes the map-part of the job with the appropriate input settings.
   * 
   * @param job The map-reduce job
   * @param inputClass the class object implementing DBWritable, which is the 
   * Java object holding tuple fields.
   * @param inputQuery the input query to select fields. Example : 
   * "SELECT f1, f2, f3 FROM Mytable ORDER BY f1"
   * @param inputCountQuery the input query that returns 
   * the number of records in the table. 
   * Example : "SELECT COUNT(f1) FROM Mytable"
   * @see #setInput(Job, Class, String, String, String, String...)
   */
  public static void setInput(Job job,
      Class<? extends DBWritable> inputClass,
      String inputQuery, String inputCountQuery) {
    job.setInputFormatClass(DBInputFormat.class);
    DBConfiguration dbConf = new DBConfiguration(job.getConfiguration());
    dbConf.setInputClass(inputClass);
    dbConf.setInputQuery(inputQuery);
    dbConf.setInputCountQuery(inputCountQuery);
  }

  protected void closeConnection() {
    try {
      if (null != this.connection) {
        this.connection.close();
        this.connection = null;
      }
    } catch (SQLException sqlE) { } // ignore exception on close.
  }
}
