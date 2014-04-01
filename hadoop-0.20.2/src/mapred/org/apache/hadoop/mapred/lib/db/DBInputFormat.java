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
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;

public class DBInputFormat<T  extends DBWritable>
    extends org.apache.hadoop.mapreduce.lib.db.DBInputFormat<T> 
    implements InputFormat<LongWritable, T>, JobConfigurable {
  /**
   * A RecordReader that reads records from a SQL table.
   * Emits LongWritables containing the record number as 
   * key and DBWritables as value.  
   */
  protected class DBRecordReader extends
      org.apache.hadoop.mapreduce.lib.db.DBRecordReader<T>
      implements RecordReader<LongWritable, T> {
    /**
     * @param split The InputSplit to read data for
     * @throws SQLException 
     */
    protected DBRecordReader(DBInputSplit split, Class<T> inputClass, 
        JobConf job, Connection conn, DBConfiguration dbConfig, String cond,
        String [] fields, String table) throws SQLException {
      super(split, inputClass, job, conn, dbConfig, cond, fields, table);
    }

    /** {@inheritDoc} */
    public LongWritable createKey() {
      return new LongWritable();  
    }

    /** {@inheritDoc} */
    public T createValue() {
      return super.createValue();
    }

    public long getPos() throws IOException {
      return super.getPos();
    }

    /** {@inheritDoc} */
    public boolean next(LongWritable key, T value) throws IOException {
      return super.next(key, value);
    }
  }

  /**
   * A RecordReader implementation that just passes through to a wrapped
   * RecordReader built with the new API.
   */
  private static class DBRecordReaderWrapper<T extends DBWritable>
      implements RecordReader<LongWritable, T> {

    private org.apache.hadoop.mapreduce.lib.db.DBRecordReader<T> rr;
    
    public DBRecordReaderWrapper(
        org.apache.hadoop.mapreduce.lib.db.DBRecordReader<T> inner) {
      this.rr = inner;
    }

    public void close() throws IOException {
      rr.close();
    }

    public LongWritable createKey() {
      return new LongWritable();
    }

    public T createValue() {
      return rr.createValue();
    }

    public float getProgress() throws IOException {
      return rr.getProgress();
    }
    
    public long getPos() throws IOException {
      return rr.getPos();
    }

    public boolean next(LongWritable key, T value) throws IOException {
      return rr.next(key, value);
    }
  }

  /**
   * A Class that does nothing, implementing DBWritable
   */
  public static class NullDBWritable extends 
      org.apache.hadoop.mapreduce.lib.db.DBInputFormat.NullDBWritable 
      implements DBWritable, Writable {
  }
  /**
   * A InputSplit that spans a set of rows
   */
  protected static class DBInputSplit extends 
      org.apache.hadoop.mapreduce.lib.db.DBInputFormat.DBInputSplit 
      implements InputSplit {
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
      super(start, end);
    }
  }

  /** {@inheritDoc} */
  public void configure(JobConf job) {
    super.setConf(job);
  }

  /** {@inheritDoc} */
  @SuppressWarnings("unchecked")
  public RecordReader<LongWritable, T> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException {

    // wrap the DBRR in a shim class to deal with API differences.
    return new DBRecordReaderWrapper<T>(
        (org.apache.hadoop.mapreduce.lib.db.DBRecordReader<T>) 
        createDBRecordReader(
          (org.apache.hadoop.mapreduce.lib.db.DBInputFormat.DBInputSplit) split, job));
  }

  /** {@inheritDoc} */
  public InputSplit[] getSplits(JobConf job, int chunks) throws IOException {
    List<org.apache.hadoop.mapreduce.InputSplit> newSplits = 
      super.getSplits(new Job(job));
    InputSplit[] ret = new InputSplit[newSplits.size()];
    int i = 0;
    for (org.apache.hadoop.mapreduce.InputSplit s : newSplits) {
      org.apache.hadoop.mapreduce.lib.db.DBInputFormat.DBInputSplit split = 
    	(org.apache.hadoop.mapreduce.lib.db.DBInputFormat.DBInputSplit)s;
      ret[i++] = new DBInputSplit(split.getStart(), split.getEnd());
    }
    return ret;
  }

  /**
   * Initializes the map-part of the job with the appropriate input settings.
   * 
   * @param job The job
   * @param inputClass the class object implementing DBWritable, which is the 
   * Java object holding tuple fields.
   * @param tableName The table to read data from
   * @param conditions The condition which to select data with, eg. '(updated >
   * 20070101 AND length > 0)'
   * @param orderBy the fieldNames in the orderBy clause.
   * @param fieldNames The field names in the table
   * @see #setInput(JobConf, Class, String, String)
   */
  public static void setInput(JobConf job, Class<? extends DBWritable> inputClass,
      String tableName,String conditions, String orderBy, String... fieldNames) {
    job.setInputFormat(DBInputFormat.class);

    DBConfiguration dbConf = new DBConfiguration(job);
    dbConf.setInputClass(inputClass);
    dbConf.setInputTableName(tableName);
    dbConf.setInputFieldNames(fieldNames);
    dbConf.setInputConditions(conditions);
    dbConf.setInputOrderBy(orderBy);
  }
  
  /**
   * Initializes the map-part of the job with the appropriate input settings.
   * 
   * @param job The job
   * @param inputClass the class object implementing DBWritable, which is the 
   * Java object holding tuple fields.
   * @param inputQuery the input query to select fields. Example : 
   * "SELECT f1, f2, f3 FROM Mytable ORDER BY f1"
   * @param inputCountQuery the input query that returns the number of records in
   * the table. 
   * Example : "SELECT COUNT(f1) FROM Mytable"
   * @see #setInput(JobConf, Class, String, String, String, String...)
   */
  public static void setInput(JobConf job, Class<? extends DBWritable> inputClass,
      String inputQuery, String inputCountQuery) {
    job.setInputFormat(DBInputFormat.class);
    
    DBConfiguration dbConf = new DBConfiguration(job);
    dbConf.setInputClass(inputClass);
    dbConf.setInputQuery(inputQuery);
    dbConf.setInputCountQuery(inputCountQuery);
    
  }
}
