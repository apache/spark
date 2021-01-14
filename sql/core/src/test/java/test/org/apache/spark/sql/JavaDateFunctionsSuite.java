package test.org.apache.spark.sql;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.test.TestSparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Date;
import java.util.*;

import static org.apache.spark.sql.types.DataTypes.*;
import static org.apache.spark.sql.functions.*;

public class JavaDateFunctionsSuite {
  private transient TestSparkSession spark;

  @Before
  public void setUp() {
        spark = new TestSparkSession();
    }

  @After
  public void tearDown() {
    spark.stop();
    spark = null;
  }

  @Test
  public void makeIntervalWorksWithJava() {
    Column twoYears = make_interval(lit(2), lit(0), lit(0), lit(0), lit(0), lit(0), lit(0));
    List<Row> rows = Arrays.asList(
      RowFactory.create(Date.valueOf("2014-06-30"), Date.valueOf("2016-06-30")),
      RowFactory.create(Date.valueOf("2015-05-01"), Date.valueOf("2017-05-01")),
      RowFactory.create(Date.valueOf("2018-12-30"), Date.valueOf("2020-12-30")));
    StructType schema = createStructType(Arrays.asList(
      createStructField("some_date", DateType, false),
      createStructField("expected", DateType, false)));
    Dataset<Row> df = spark.createDataFrame(rows, schema).withColumn("plus_two_years", col("some_date").plus(twoYears));
    Assert.assertTrue(Arrays.equals(
      (Row[]) df.select(df.col("plus_two_years")).collect(),
      (Row[]) df.select(df.col("expected")).collect()));
  }

}
