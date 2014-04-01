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
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
 * Operates like DBInputFormat, but instead of using LIMIT and OFFSET to demarcate
 * splits, it tries to generate WHERE clauses which separate the data into roughly
 * equivalent shards.
 */
public class DataDrivenDBInputFormat<T extends DBWritable>
    extends DBInputFormat<T> implements Configurable {

  private static final Log LOG = LogFactory.getLog(DataDrivenDBInputFormat.class);

  /** If users are providing their own query, the following string is expected to
      appear in the WHERE clause, which will be substituted with a pair of conditions
      on the input to allow input splits to parallelise the import. */
  public static final String SUBSTITUTE_TOKEN = "$CONDITIONS";

  /**
   * A InputSplit that spans a set of rows
   */
  public static class DataDrivenDBInputSplit extends DBInputFormat.DBInputSplit {

    private String lowerBoundClause;
    private String upperBoundClause;

    /**
     * Default Constructor
     */
    public DataDrivenDBInputSplit() {
    }

    /**
     * Convenience Constructor
     * @param lower the string to be put in the WHERE clause to guard on the 'lower' end
     * @param upper the string to be put in the WHERE clause to guard on the 'upper' end
     */
    public DataDrivenDBInputSplit(final String lower, final String upper) {
      this.lowerBoundClause = lower;
      this.upperBoundClause = upper;
    }


    /**
     * @return The total row count in this split
     */
    public long getLength() throws IOException {
      return 0; // unfortunately, we don't know this.
    }

    /** {@inheritDoc} */
    public void readFields(DataInput input) throws IOException {
      this.lowerBoundClause = Text.readString(input);
      this.upperBoundClause = Text.readString(input);
    }

    /** {@inheritDoc} */
    public void write(DataOutput output) throws IOException {
      Text.writeString(output, this.lowerBoundClause);
      Text.writeString(output, this.upperBoundClause);
    }

    public String getLowerClause() {
      return lowerBoundClause;
    }

    public String getUpperClause() {
      return upperBoundClause;
    }
  }

  /**
   * @return the DBSplitter implementation to use to divide the table/query into InputSplits.
   */
  protected DBSplitter getSplitter(int sqlDataType) {
    switch (sqlDataType) {
    case Types.NUMERIC:
    case Types.DECIMAL:
      return new BigDecimalSplitter();

    case Types.BIT:
    case Types.BOOLEAN:
      return new BooleanSplitter();

    case Types.INTEGER:
    case Types.TINYINT:
    case Types.SMALLINT:
    case Types.BIGINT:
      return new IntegerSplitter();

    case Types.REAL:
    case Types.FLOAT:
    case Types.DOUBLE:
      return new FloatSplitter();

    case Types.CHAR:
    case Types.VARCHAR:
    case Types.LONGVARCHAR:
      return new TextSplitter();

    case Types.DATE:
    case Types.TIME:
    case Types.TIMESTAMP:
      return new DateSplitter();

    default:
      // TODO: Support BINARY, VARBINARY, LONGVARBINARY, DISTINCT, CLOB, BLOB, ARRAY
      // STRUCT, REF, DATALINK, and JAVA_OBJECT.
      return null;
    }
  }

  /** {@inheritDoc} */
  public List<InputSplit> getSplits(JobContext job) throws IOException {

    int targetNumTasks = job.getConfiguration().getInt("mapred.map.tasks", 1);
    if (1 == targetNumTasks) {
      // There's no need to run a bounding vals query; just return a split
      // that separates nothing. This can be considerably more optimal for a
      // large table with no index.
      List<InputSplit> singletonSplit = new ArrayList<InputSplit>();
      singletonSplit.add(new DataDrivenDBInputSplit("1=1", "1=1"));
      return singletonSplit;
    }

    ResultSet results = null;
    Statement statement = null;
    Connection connection = getConnection();
    try {
      statement = connection.createStatement();

      results = statement.executeQuery(getBoundingValsQuery());
      results.next();

      // Based on the type of the results, use a different mechanism
      // for interpolating split points (i.e., numeric splits, text splits,
      // dates, etc.)
      int sqlDataType = results.getMetaData().getColumnType(1);
      DBSplitter splitter = getSplitter(sqlDataType);
      if (null == splitter) {
        throw new IOException("Unknown SQL data type: " + sqlDataType);
      }

      return splitter.split(job.getConfiguration(), results, getDBConf().getInputOrderBy());
    } catch (SQLException e) {
      throw new IOException(e.getMessage());
    } finally {
      // More-or-less ignore SQL exceptions here, but log in case we need it.
      try {
        if (null != results) {
          results.close();
        }
      } catch (SQLException se) {
        LOG.debug("SQLException closing resultset: " + se.toString());
      }

      try {
        if (null != statement) {
          statement.close();
        }
      } catch (SQLException se) {
        LOG.debug("SQLException closing statement: " + se.toString());
      }

      try {
        connection.commit();
        closeConnection();
      } catch (SQLException se) {
        LOG.debug("SQLException committing split transaction: " + se.toString());
      }
    }
  }

  /**
   * @return a query which returns the minimum and maximum values for
   * the order-by column.
   *
   * The min value should be in the first column, and the
   * max value should be in the second column of the results.
   */
  protected String getBoundingValsQuery() {
    // If the user has provided a query, use that instead.
    String userQuery = getDBConf().getInputBoundingQuery();
    if (null != userQuery) {
      return userQuery;
    }

    // Auto-generate one based on the table name we've been provided with.
    StringBuilder query = new StringBuilder();

    String splitCol = getDBConf().getInputOrderBy();
    query.append("SELECT MIN(").append(splitCol).append("), ");
    query.append("MAX(").append(splitCol).append(") FROM ");
    query.append(getDBConf().getInputTableName());
    String conditions = getDBConf().getInputConditions();
    if (null != conditions) {
      query.append(" WHERE ( " + conditions + " )");
    }

    return query.toString();
  }

  /** Set the user-defined bounding query to use with a user-defined query.
      This *must* include the substring "$CONDITIONS"
      (DataDrivenDBInputFormat.SUBSTITUTE_TOKEN) inside the WHERE clause,
      so that DataDrivenDBInputFormat knows where to insert split clauses.
      e.g., "SELECT foo FROM mytable WHERE $CONDITIONS"
      This will be expanded to something like:
        SELECT foo FROM mytable WHERE (id &gt; 100) AND (id &lt; 250)
      inside each split.
    */
  public static void setBoundingQuery(Configuration conf, String query) {
    if (null != query) {
      // If the user's settng a query, warn if they don't allow conditions.
      if (query.indexOf(SUBSTITUTE_TOKEN) == -1) {
        LOG.warn("Could not find " + SUBSTITUTE_TOKEN + " token in query: " + query
            + "; splits may not partition data.");
      }
    }

    conf.set(DBConfiguration.INPUT_BOUNDING_QUERY, query);
  }

  protected RecordReader<LongWritable, T> createDBRecordReader(DBInputSplit split,
      Configuration conf) throws IOException {

    DBConfiguration dbConf = getDBConf();
    @SuppressWarnings("unchecked")
    Class<T> inputClass = (Class<T>) (dbConf.getInputClass());
    String dbProductName = getDBProductName();

    LOG.debug("Creating db record reader for db product: " + dbProductName);

    try {
      // use database product name to determine appropriate record reader.
      if (dbProductName.startsWith("MYSQL")) {
        // use MySQL-specific db reader.
        return new MySQLDataDrivenDBRecordReader<T>(split, inputClass,
            conf, getConnection(), dbConf, dbConf.getInputConditions(),
            dbConf.getInputFieldNames(), dbConf.getInputTableName());
      } else {
        // Generic reader.
        return new DataDrivenDBRecordReader<T>(split, inputClass,
            conf, getConnection(), dbConf, dbConf.getInputConditions(),
            dbConf.getInputFieldNames(), dbConf.getInputTableName(),
            dbProductName);
      }
    } catch (SQLException ex) {
      throw new IOException(ex.getMessage());
    }
  }

  // Configuration methods override superclass to ensure that the proper
  // DataDrivenDBInputFormat gets used.

  /** Note that the "orderBy" column is called the "splitBy" in this version.
    * We reuse the same field, but it's not strictly ordering it -- just partitioning
    * the results.
    */
  public static void setInput(Job job, 
      Class<? extends DBWritable> inputClass,
      String tableName,String conditions, 
      String splitBy, String... fieldNames) {
    DBInputFormat.setInput(job, inputClass, tableName, conditions, splitBy, fieldNames);
    job.setInputFormatClass(DataDrivenDBInputFormat.class);
  }

  /** setInput() takes a custom query and a separate "bounding query" to use
      instead of the custom "count query" used by DBInputFormat.
    */
  public static void setInput(Job job,
      Class<? extends DBWritable> inputClass,
      String inputQuery, String inputBoundingQuery) {
    DBInputFormat.setInput(job, inputClass, inputQuery, "");
    job.getConfiguration().set(DBConfiguration.INPUT_BOUNDING_QUERY, inputBoundingQuery);
    job.setInputFormatClass(DataDrivenDBInputFormat.class);
  }
}
