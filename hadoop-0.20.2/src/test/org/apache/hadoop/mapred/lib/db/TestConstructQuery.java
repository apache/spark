package org.apache.hadoop.mapred.lib.db;

import org.apache.hadoop.io.NullWritable;

import junit.framework.TestCase;

public class TestConstructQuery extends TestCase {
  public void testConstructQuery() {
    DBOutputFormat<DBWritable, NullWritable> format = new DBOutputFormat<DBWritable, NullWritable>();
    String expected = "INSERT INTO hadoop_output (id,name,value) VALUES (?,?,?);";
    String[] fieldNames = new String[] { "id", "name", "value" };
    String actual = format.constructQuery("hadoop_output", fieldNames);
    assertEquals(expected, actual);
    expected = "INSERT INTO hadoop_output VALUES (?,?,?);";
    fieldNames = new String[] { null, null, null };
    actual = format.constructQuery("hadoop_output", fieldNames);
    assertEquals(expected, actual);
  }
}
