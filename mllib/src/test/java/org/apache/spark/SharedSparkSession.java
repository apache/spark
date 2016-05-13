package org.apache.spark;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SharedSparkSession {

  public transient SparkSession spark;
  public transient JavaSparkContext jsc;

  @Before
  public void setUp() throws IOException {
    spark = SparkSession.builder()
      .master("local")
      .appName("shared-spark-session")
      .getOrCreate();
    jsc = new JavaSparkContext(spark.sparkContext());

    customSetUp();
    customSetUpWithException();
  }

  public void customSetUp() {}

  // TODO: Remove this once we have a way to use customSetUp that Exception
  public void customSetUpWithException() throws IOException {}

  @After
  public void tearDown() {
    spark.stop();
    spark = null;

    customTearDown();
  }

  public void customTearDown() {}
}
