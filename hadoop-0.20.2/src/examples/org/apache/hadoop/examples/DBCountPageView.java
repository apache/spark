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

package org.apache.hadoop.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hsqldb.Server;

/**
 * This is a demonstrative program, which uses DBInputFormat for reading
 * the input data from a database, and DBOutputFormat for writing the data 
 * to the database. 
 * <br>
 * The Program first creates the necessary tables, populates the input table 
 * and runs the mapred job. 
 * <br> 
 * The input data is a mini access log, with a <code>&lt;url,referrer,time&gt;
 * </code> schema.The output is the number of pageviews of each url in the log, 
 * having the schema <code>&lt;url,pageview&gt;</code>.  
 * 
 * When called with no arguments the program starts a local HSQLDB server, and 
 * uses this database for storing/retrieving the data. 
 */
public class DBCountPageView extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(DBCountPageView.class);
  
  private Connection connection;
  private boolean initialized = false;

  private static final String[] AccessFieldNames = {"url", "referrer", "time"};
  private static final String[] PageviewFieldNames = {"url", "pageview"};
  
  private static final String DB_URL = "jdbc:hsqldb:hsql://localhost/URLAccess";
  private static final String DRIVER_CLASS = "org.hsqldb.jdbcDriver";
  
  private Server server;
  
  private void startHsqldbServer() {
    server = new Server();
    server.setDatabasePath(0, 
        System.getProperty("test.build.data",".") + "/URLAccess");
    server.setDatabaseName(0, "URLAccess");
    server.start();
  }
  
  private void createConnection(String driverClassName
      , String url) throws Exception {
    
    Class.forName(driverClassName);
    connection = DriverManager.getConnection(url);
    connection.setAutoCommit(false);
  }

  private void shutdown() {
    try {
      connection.commit();
      connection.close();
    }catch (Throwable ex) {
      LOG.warn("Exception occurred while closing connection :"
          + StringUtils.stringifyException(ex));
    } finally {
      try {
        if(server != null) {
          server.shutdown();
        }
      }catch (Throwable ex) {
        LOG.warn("Exception occurred while shutting down HSQLDB :"
            + StringUtils.stringifyException(ex));
      }
    }
  }

  private void initialize(String driverClassName, String url) 
    throws Exception {
    if(!this.initialized) {
      if(driverClassName.equals(DRIVER_CLASS)) {
        startHsqldbServer();
      }
      createConnection(driverClassName, url);
      dropTables();
      createTables();
      populateAccess();
      this.initialized = true;  
    }
  }
  
  private void dropTables() {
    String dropAccess = "DROP TABLE Access";
    String dropPageview = "DROP TABLE Pageview";
    
    try {
      Statement st = connection.createStatement();
      st.executeUpdate(dropAccess);
      st.executeUpdate(dropPageview);
      connection.commit();
      st.close();
    }catch (SQLException ex) {
      //ignore
    }
  }
  
  private void createTables() throws SQLException {

    String createAccess = 
      "CREATE TABLE " +
      "Access(url      VARCHAR(100) NOT NULL," +
            " referrer VARCHAR(100)," +
            " time     BIGINT NOT NULL, " +
            " PRIMARY KEY (url, time))";

    String createPageview = 
      "CREATE TABLE " +
      "Pageview(url      VARCHAR(100) NOT NULL," +
              " pageview     BIGINT NOT NULL, " +
               " PRIMARY KEY (url))";
    
    Statement st = connection.createStatement();
    try {
      st.executeUpdate(createAccess);
      st.executeUpdate(createPageview);
      connection.commit();
    } finally {
      st.close();
    }
  }

  /**
   * Populates the Access table with generated records.
   */
  private void populateAccess() throws SQLException {

    PreparedStatement statement = null ;
    try {
      statement = connection.prepareStatement(
          "INSERT INTO Access(url, referrer, time)" +
          " VALUES (?, ?, ?)");

      Random random = new Random();

      int time = random.nextInt(50) + 50;

      final int PROBABILITY_PRECISION = 100; //  1 / 100 
      final int NEW_PAGE_PROBABILITY  = 15;  //  15 / 100


      //Pages in the site :
      String[] pages = {"/a", "/b", "/c", "/d", "/e", "/f", "/g", "/h", "/i", "/j"};
      //linkMatrix[i] is the array of pages(indexes) that page_i links to.  
      int[][] linkMatrix = {{1,5,7}, {0,7,4,6,}, {0,1,7,8}, {0,2,4,6,7,9}, {0,1},
          {0,3,5,9}, {0}, {0,1,3}, {0,2,6}, {0,2,6}};

      //a mini model of user browsing a la pagerank
      int currentPage = random.nextInt(pages.length); 
      String referrer = null;

      for(int i=0; i<time; i++) {

        statement.setString(1, pages[currentPage]);
        statement.setString(2, referrer);
        statement.setLong(3, i);
        statement.execute();

        int action = random.nextInt(PROBABILITY_PRECISION);

        //go to a new page with probability NEW_PAGE_PROBABILITY / PROBABILITY_PRECISION
        if(action < NEW_PAGE_PROBABILITY) { 
          currentPage = random.nextInt(pages.length); // a random page
          referrer = null;
        }
        else {
          referrer = pages[currentPage];
          action = random.nextInt(linkMatrix[currentPage].length);
          currentPage = linkMatrix[currentPage][action];
        }
      }
      
      connection.commit();
      
    }catch (SQLException ex) {
      connection.rollback();
      throw ex;
    } finally {
      if(statement != null) {
        statement.close();
      }
    }
  }
  
  /**Verifies the results are correct */
  private boolean verify() throws SQLException {
    //check total num pageview
    String countAccessQuery = "SELECT COUNT(*) FROM Access";
    String sumPageviewQuery = "SELECT SUM(pageview) FROM Pageview";
    Statement st = null;
    ResultSet rs = null;
    try {
      st = connection.createStatement();
      rs = st.executeQuery(countAccessQuery);
      rs.next();
      long totalPageview = rs.getLong(1);

      rs = st.executeQuery(sumPageviewQuery);
      rs.next();
      long sumPageview = rs.getLong(1);

      LOG.info("totalPageview=" + totalPageview);
      LOG.info("sumPageview=" + sumPageview);

      return totalPageview == sumPageview && totalPageview != 0;
    }finally {
      if(st != null)
        st.close();
      if(rs != null)
        rs.close();
    }
  }
  
  /** Holds a &lt;url, referrer, time &gt; tuple */
  static class AccessRecord implements Writable, DBWritable {
    String url;
    String referrer;
    long time;
    
    @Override
    public void readFields(DataInput in) throws IOException {
      this.url = Text.readString(in);
      this.referrer = Text.readString(in);
      this.time = in.readLong();
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
      Text.writeString(out, url);
      Text.writeString(out, referrer);
      out.writeLong(time);
    }
    
    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
      this.url = resultSet.getString(1);
      this.referrer = resultSet.getString(2);
      this.time = resultSet.getLong(3);
    }
    @Override
    public void write(PreparedStatement statement) throws SQLException {
      statement.setString(1, url);
      statement.setString(2, referrer);
      statement.setLong(3, time);
    }
  }
  /** Holds a &lt;url, pageview &gt; tuple */
  static class PageviewRecord implements Writable, DBWritable {
    String url;
    long pageview;
   
    public PageviewRecord(String url, long pageview) {
      this.url = url;
      this.pageview = pageview;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
      this.url = Text.readString(in);
      this.pageview = in.readLong();
    }
    @Override
    public void write(DataOutput out) throws IOException {
      Text.writeString(out, url);
      out.writeLong(pageview);
    }
    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
      this.url = resultSet.getString(1);
      this.pageview = resultSet.getLong(2);
    }
    @Override
    public void write(PreparedStatement statement) throws SQLException {
      statement.setString(1, url);
      statement.setLong(2, pageview);
    }
    @Override
    public String toString() {
      return url + " " + pageview;
    }
  }
  
  /**
   * Mapper extracts URLs from the AccessRecord (tuples from db), 
   * and emits a &lt;url,1&gt; pair for each access record. 
   */
  static class PageviewMapper extends MapReduceBase 
    implements Mapper<LongWritable, AccessRecord, Text, LongWritable> {
    
    LongWritable ONE = new LongWritable(1L);
    @Override
    public void map(LongWritable key, AccessRecord value,
        OutputCollector<Text, LongWritable> output, Reporter reporter)
        throws IOException {
      
      Text oKey = new Text(value.url);
      output.collect(oKey, ONE);
    }
  }
  
  /**
   * Reducer sums up the pageviews and emits a PageviewRecord, 
   * which will correspond to one tuple in the db.
   */
  static class PageviewReducer extends MapReduceBase 
    implements Reducer<Text, LongWritable, PageviewRecord, NullWritable> {
    
    NullWritable n = NullWritable.get();
    @Override
    public void reduce(Text key, Iterator<LongWritable> values,
        OutputCollector<PageviewRecord, NullWritable> output, Reporter reporter)
        throws IOException {
      
      long sum = 0L;
      while(values.hasNext()) {
        sum += values.next().get();
      }
      output.collect(new PageviewRecord(key.toString(), sum), n);
    }
  }
  
  @Override
  //Usage DBCountPageView [driverClass dburl]
  public int run(String[] args) throws Exception {
    
    String driverClassName = DRIVER_CLASS;
    String url = DB_URL;
    
    if(args.length > 1) {
      driverClassName = args[0];
      url = args[1];
    }
    
    initialize(driverClassName, url);

    JobConf job = new JobConf(getConf(), DBCountPageView.class);
        
    job.setJobName("Count Pageviews of URLs");

    job.setMapperClass(PageviewMapper.class);
    job.setCombinerClass(LongSumReducer.class);
    job.setReducerClass(PageviewReducer.class);

    DBConfiguration.configureDB(job, driverClassName, url);
    
    DBInputFormat.setInput(job, AccessRecord.class, "Access"
        , null, "url", AccessFieldNames);

    DBOutputFormat.setOutput(job, "Pageview", PageviewFieldNames);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);

    job.setOutputKeyClass(PageviewRecord.class);
    job.setOutputValueClass(NullWritable.class);

    try {
      JobClient.runJob(job);
      
      boolean correct = verify();
      if(!correct) {
        throw new RuntimeException("Evaluation was not correct!");
      }
    } finally {
      shutdown();    
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new DBCountPageView(), args);
    System.exit(ret);
  }

}
