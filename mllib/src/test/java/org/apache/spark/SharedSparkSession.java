package org.apache.spark;

import org.junit.After;
import org.junit.Before;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SharedSparkSession {

  public transient SparkSession spark;
  public transient JavaSparkContext jsc;

  @Before
  public void setUp() {
    spark = SparkSession.builder()
      .master("local")
      .appName("shared-spark-session")
      .getOrCreate();
    jsc = new JavaSparkContext(spark.sparkContext());

    customSetUp();
  }

  public void customSetUp() {}

  @After
  public void tearDown() {
    spark.stop();
    spark = null;
  }
}
