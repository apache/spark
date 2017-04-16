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

import org.apache.hadoop.mapred.JobConf;

public class DBConfiguration extends 
    org.apache.hadoop.mapreduce.lib.db.DBConfiguration {
  /** The JDBC Driver class name */
  public static final String DRIVER_CLASS_PROPERTY = 
    org.apache.hadoop.mapreduce.lib.db.DBConfiguration.DRIVER_CLASS_PROPERTY;
  
  /** JDBC Database access URL */
  public static final String URL_PROPERTY = 
    org.apache.hadoop.mapreduce.lib.db.DBConfiguration.URL_PROPERTY;

  /** User name to access the database */
  public static final String USERNAME_PROPERTY = 
    org.apache.hadoop.mapreduce.lib.db.DBConfiguration.USERNAME_PROPERTY;
  
  /** Password to access the database */
  public static final String PASSWORD_PROPERTY = 
    org.apache.hadoop.mapreduce.lib.db.DBConfiguration.PASSWORD_PROPERTY;

  /** Input table name */
  public static final String INPUT_TABLE_NAME_PROPERTY = org.apache.hadoop.
    mapreduce.lib.db.DBConfiguration.INPUT_TABLE_NAME_PROPERTY;

  /** Field names in the Input table */
  public static final String INPUT_FIELD_NAMES_PROPERTY = org.apache.hadoop.
    mapreduce.lib.db.DBConfiguration.INPUT_FIELD_NAMES_PROPERTY;

  /** WHERE clause in the input SELECT statement */
  public static final String INPUT_CONDITIONS_PROPERTY = org.apache.hadoop.
    mapreduce.lib.db.DBConfiguration.INPUT_CONDITIONS_PROPERTY;
  
  /** ORDER BY clause in the input SELECT statement */
  public static final String INPUT_ORDER_BY_PROPERTY = org.apache.hadoop.
    mapreduce.lib.db.DBConfiguration.INPUT_ORDER_BY_PROPERTY;
  
  /** Whole input query, exluding LIMIT...OFFSET */
  public static final String INPUT_QUERY = 
    org.apache.hadoop.mapreduce.lib.db.DBConfiguration.INPUT_QUERY;
  
  /** Input query to get the count of records */
  public static final String INPUT_COUNT_QUERY = 
    org.apache.hadoop.mapreduce.lib.db.DBConfiguration.INPUT_COUNT_QUERY;
  
  /** Class name implementing DBWritable which will hold input tuples */
  public static final String INPUT_CLASS_PROPERTY = 
    org.apache.hadoop.mapreduce.lib.db.DBConfiguration.INPUT_CLASS_PROPERTY;

  /** Output table name */
  public static final String OUTPUT_TABLE_NAME_PROPERTY = org.apache.hadoop.
    mapreduce.lib.db.DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY;

  /** Field names in the Output table */
  public static final String OUTPUT_FIELD_NAMES_PROPERTY = org.apache.hadoop.
    mapreduce.lib.db.DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY;  

  /** Number of fields in the Output table */
  public static final String OUTPUT_FIELD_COUNT_PROPERTY = org.apache.hadoop.
    mapreduce.lib.db.DBConfiguration.OUTPUT_FIELD_COUNT_PROPERTY;

  
  /**
   * Sets the DB access related fields in the JobConf.  
   * @param job the job
   * @param driverClass JDBC Driver class name
   * @param dbUrl JDBC DB access URL. 
   * @param userName DB access username 
   * @param passwd DB access passwd
   */
  public static void configureDB(JobConf job, String driverClass, String dbUrl
      , String userName, String passwd) {

    job.set(DRIVER_CLASS_PROPERTY, driverClass);
    job.set(URL_PROPERTY, dbUrl);
    if(userName != null)
      job.set(USERNAME_PROPERTY, userName);
    if(passwd != null)
      job.set(PASSWORD_PROPERTY, passwd);    
  }

  /**
   * Sets the DB access related fields in the JobConf.  
   * @param job the job
   * @param driverClass JDBC Driver class name
   * @param dbUrl JDBC DB access URL. 
   */
  public static void configureDB(JobConf job, String driverClass, String dbUrl) {
    configureDB(job, driverClass, dbUrl, null, null);
  }

  DBConfiguration(JobConf job) {
    super(job);
  }
  
}

