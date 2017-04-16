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
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat.NullDBWritable;

/**
 * A container for configuration property names for jobs with DB input/output.
 *  
 * The job can be configured using the static methods in this class, 
 * {@link DBInputFormat}, and {@link DBOutputFormat}. 
 * Alternatively, the properties can be set in the configuration with proper
 * values. 
 *   
 * @see DBConfiguration#configureDB(Configuration, String, String, String, String)
 * @see DBInputFormat#setInput(Job, Class, String, String)
 * @see DBInputFormat#setInput(Job, Class, String, String, String, String...)
 * @see DBOutputFormat#setOutput(Job, String, String...)
 */
public class DBConfiguration {

  /** The JDBC Driver class name */
  public static final String DRIVER_CLASS_PROPERTY = 
      "mapred.jdbc.driver.class";
  
  /** JDBC Database access URL */
  public static final String URL_PROPERTY = "mapred.jdbc.url";

  /** User name to access the database */
  public static final String USERNAME_PROPERTY = "mapred.jdbc.username";
  
  /** Password to access the database */
  public static final String PASSWORD_PROPERTY = "mapred.jdbc.password";

  /** Input table name */
  public static final String INPUT_TABLE_NAME_PROPERTY = 
      "mapred.jdbc.input.table.name";

  /** Field names in the Input table */
  public static final String INPUT_FIELD_NAMES_PROPERTY = 
      "mapred.jdbc.input.field.names";

  /** WHERE clause in the input SELECT statement */
  public static final String INPUT_CONDITIONS_PROPERTY = 
      "mapred.jdbc.input.conditions";
  
  /** ORDER BY clause in the input SELECT statement */
  public static final String INPUT_ORDER_BY_PROPERTY = 
      "mapred.jdbc.input.orderby";
  
  /** Whole input query, exluding LIMIT...OFFSET */
  public static final String INPUT_QUERY = "mapred.jdbc.input.query";
  
  /** Input query to get the count of records */
  public static final String INPUT_COUNT_QUERY = 
      "mapred.jdbc.input.count.query";
  
  /** Input query to get the max and min values of the jdbc.input.query */
  public static final String INPUT_BOUNDING_QUERY =
      "mapred.jdbc.input.bounding.query";
  
  /** Class name implementing DBWritable which will hold input tuples */
  public static final String INPUT_CLASS_PROPERTY = "mapred.jdbc.input.class";

  /** Output table name */
  public static final String OUTPUT_TABLE_NAME_PROPERTY = 
      "mapred.jdbc.output.table.name";

  /** Field names in the Output table */
  public static final String OUTPUT_FIELD_NAMES_PROPERTY = 
      "mapred.jdbc.output.field.names";  

  /** Number of fields in the Output table */
  public static final String OUTPUT_FIELD_COUNT_PROPERTY = 
      "mapred.jdbc.output.field.count";  
  
  /**
   * Sets the DB access related fields in the {@link Configuration}.  
   * @param conf the configuration
   * @param driverClass JDBC Driver class name
   * @param dbUrl JDBC DB access URL. 
   * @param userName DB access username 
   * @param passwd DB access passwd
   */
  public static void configureDB(Configuration conf, String driverClass, 
      String dbUrl, String userName, String passwd) {

    conf.set(DRIVER_CLASS_PROPERTY, driverClass);
    conf.set(URL_PROPERTY, dbUrl);
    if (userName != null) {
      conf.set(USERNAME_PROPERTY, userName);
    }
    if (passwd != null) {
      conf.set(PASSWORD_PROPERTY, passwd);
    }
  }

  /**
   * Sets the DB access related fields in the JobConf.  
   * @param job the job
   * @param driverClass JDBC Driver class name
   * @param dbUrl JDBC DB access URL. 
   */
  public static void configureDB(Configuration job, String driverClass,
      String dbUrl) {
    configureDB(job, driverClass, dbUrl, null, null);
  }

  private Configuration conf;

  public DBConfiguration(Configuration job) {
    this.conf = job;
  }

  /** Returns a connection object o the DB 
   * @throws ClassNotFoundException 
   * @throws SQLException */
  public Connection getConnection() 
      throws ClassNotFoundException, SQLException {

    Class.forName(conf.get(DBConfiguration.DRIVER_CLASS_PROPERTY));

    if(conf.get(DBConfiguration.USERNAME_PROPERTY) == null) {
      return DriverManager.getConnection(
               conf.get(DBConfiguration.URL_PROPERTY));
    } else {
      return DriverManager.getConnection(
          conf.get(DBConfiguration.URL_PROPERTY), 
          conf.get(DBConfiguration.USERNAME_PROPERTY), 
          conf.get(DBConfiguration.PASSWORD_PROPERTY));
    }
  }

  public Configuration getConf() {
    return conf;
  }
  
  public String getInputTableName() {
    return conf.get(DBConfiguration.INPUT_TABLE_NAME_PROPERTY);
  }

  public void setInputTableName(String tableName) {
    conf.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, tableName);
  }

  public String[] getInputFieldNames() {
    return conf.getStrings(DBConfiguration.INPUT_FIELD_NAMES_PROPERTY);
  }

  public void setInputFieldNames(String... fieldNames) {
    conf.setStrings(DBConfiguration.INPUT_FIELD_NAMES_PROPERTY, fieldNames);
  }

  public String getInputConditions() {
    return conf.get(DBConfiguration.INPUT_CONDITIONS_PROPERTY);
  }

  public void setInputConditions(String conditions) {
    if (conditions != null && conditions.length() > 0)
      conf.set(DBConfiguration.INPUT_CONDITIONS_PROPERTY, conditions);
  }

  public String getInputOrderBy() {
    return conf.get(DBConfiguration.INPUT_ORDER_BY_PROPERTY);
  }
  
  public void setInputOrderBy(String orderby) {
    if(orderby != null && orderby.length() >0) {
      conf.set(DBConfiguration.INPUT_ORDER_BY_PROPERTY, orderby);
    }
  }
  
  public String getInputQuery() {
    return conf.get(DBConfiguration.INPUT_QUERY);
  }
  
  public void setInputQuery(String query) {
    if(query != null && query.length() >0) {
      conf.set(DBConfiguration.INPUT_QUERY, query);
    }
  }
  
  public String getInputCountQuery() {
    return conf.get(DBConfiguration.INPUT_COUNT_QUERY);
  }
  
  public void setInputCountQuery(String query) {
    if(query != null && query.length() > 0) {
      conf.set(DBConfiguration.INPUT_COUNT_QUERY, query);
    }
  }

  public void setInputBoundingQuery(String query) {
    if (query != null && query.length() > 0) {
      conf.set(DBConfiguration.INPUT_BOUNDING_QUERY, query);
    }
  }

  public String getInputBoundingQuery() {
    return conf.get(DBConfiguration.INPUT_BOUNDING_QUERY);
  }

  public Class<?> getInputClass() {
    return conf.getClass(DBConfiguration.INPUT_CLASS_PROPERTY,
                         NullDBWritable.class);
  }

  public void setInputClass(Class<? extends DBWritable> inputClass) {
    conf.setClass(DBConfiguration.INPUT_CLASS_PROPERTY, inputClass,
                  DBWritable.class);
  }

  public String getOutputTableName() {
    return conf.get(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY);
  }

  public void setOutputTableName(String tableName) {
    conf.set(DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY, tableName);
  }

  public String[] getOutputFieldNames() {
    return conf.getStrings(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY);
  }

  public void setOutputFieldNames(String... fieldNames) {
    conf.setStrings(DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY, fieldNames);
  }

  public void setOutputFieldCount(int fieldCount) {
    conf.setInt(DBConfiguration.OUTPUT_FIELD_COUNT_PROPERTY, fieldCount);
  }
  
  public int getOutputFieldCount() {
    return conf.getInt(OUTPUT_FIELD_COUNT_PROPERTY, 0);
  }
  
}

