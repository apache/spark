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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;

/**
 * Objects that are read from/written to a database should implement
 * <code>DBWritable</code>. DBWritable, is similar to {@link Writable} 
 * except that the {@link #write(PreparedStatement)} method takes a 
 * {@link PreparedStatement}, and {@link #readFields(ResultSet)} 
 * takes a {@link ResultSet}. 
 * <p>
 * Implementations are responsible for writing the fields of the object 
 * to PreparedStatement, and reading the fields of the object from the 
 * ResultSet. 
 * 
 * <p>Example:</p>
 * If we have the following table in the database :
 * <pre>
 * CREATE TABLE MyTable (
 *   counter        INTEGER NOT NULL,
 *   timestamp      BIGINT  NOT NULL,
 * );
 * </pre>
 * then we can read/write the tuples from/to the table with :
 * <p><pre>
 * public class MyWritable implements Writable, DBWritable {
 *   // Some data     
 *   private int counter;
 *   private long timestamp;
 *       
 *   //Writable#write() implementation
 *   public void write(DataOutput out) throws IOException {
 *     out.writeInt(counter);
 *     out.writeLong(timestamp);
 *   }
 *       
 *   //Writable#readFields() implementation
 *   public void readFields(DataInput in) throws IOException {
 *     counter = in.readInt();
 *     timestamp = in.readLong();
 *   }
 *       
 *   public void write(PreparedStatement statement) throws SQLException {
 *     statement.setInt(1, counter);
 *     statement.setLong(2, timestamp);
 *   }
 *       
 *   public void readFields(ResultSet resultSet) throws SQLException {
 *     counter = resultSet.getInt(1);
 *     timestamp = resultSet.getLong(2);
 *   } 
 * }
 * </pre></p>
 */
public interface DBWritable {

  /**
   * Sets the fields of the object in the {@link PreparedStatement}.
   * @param statement the statement that the fields are put into.
   * @throws SQLException
   */
	public void write(PreparedStatement statement) throws SQLException;
	
	/**
	 * Reads the fields of the object from the {@link ResultSet}. 
	 * @param resultSet the {@link ResultSet} to get the fields from.
	 * @throws SQLException
	 */
	public void readFields(ResultSet resultSet) throws SQLException ; 
}
