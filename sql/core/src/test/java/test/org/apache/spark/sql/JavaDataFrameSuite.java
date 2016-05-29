/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package test.org.apache.spark.sql;

import java.io.Serializable;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;
import java.math.BigInteger;
import java.math.BigDecimal;

import scala.collection.JavaConverters;
import scala.collection.Seq;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import org.junit.*;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.test.TestSparkSession;
import org.apache.spark.sql.types.*;
import org.apache.spark.util.sketch.BloomFilter;
import org.apache.spark.util.sketch.CountMinSketch;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;

public class JavaDataFrameSuite {
  private transient TestSparkSession spark;
  private transient JavaSparkContext jsc;

  @Before
  public void setUp() {
    // Trigger static initializer of TestData
    spark = new TestSparkSession();
    jsc = new JavaSparkContext(spark.sparkContext());
    spark.loadTestData();
  }

  @After
  public void tearDown() {
    spark.stop();
    spark = null;
  }

  @Test
  public void testExecution() {
    Dataset<Row> df = spark.table("testData").filter("key = 1");
    Assert.assertEquals(1, df.select("key").collectAsList().get(0).get(0));
  }

  @Test
  public void testCollectAndTake() {
    Dataset<Row> df = spark.table("testData").filter("key = 1 or key = 2 or key = 3");
    Assert.assertEquals(3, df.select("key").collectAsList().size());
    Assert.assertEquals(2, df.select("key").takeAsList(2).size());
  }

  /**
   * See SPARK-5904. Abstract vararg methods defined in Scala do not work in Java.
   */
  @Test
  public void testVarargMethods() {
    Dataset<Row> df = spark.table("testData");

    df.toDF("key1", "value1");

    df.select("key", "value");
    df.select(col("key"), col("value"));
    df.selectExpr("key", "value + 1");

    df.sort("key", "value");
    df.sort(col("key"), col("value"));
    df.orderBy("key", "value");
    df.orderBy(col("key"), col("value"));

    df.groupBy("key", "value").agg(col("key"), col("value"), sum("value"));
    df.groupBy(col("key"), col("value")).agg(col("key"), col("value"), sum("value"));
    df.agg(first("key"), sum("value"));

    df.groupBy().avg("key");
    df.groupBy().mean("key");
    df.groupBy().max("key");
    df.groupBy().min("key");
    df.groupBy().sum("key");

    // Varargs in column expressions
    df.groupBy().agg(countDistinct("key", "value"));
    df.groupBy().agg(countDistinct(col("key"), col("value")));
    df.select(coalesce(col("key")));

    // Varargs with mathfunctions
    Dataset<Row> df2 = spark.table("testData2");
    df2.select(exp("a"), exp("b"));
    df2.select(exp(log("a")));
    df2.select(pow("a", "a"), pow("b", 2.0));
    df2.select(pow(col("a"), col("b")), exp("b"));
    df2.select(sin("a"), acos("b"));

    df2.select(rand(), acos("b"));
    df2.select(col("*"), randn(5L));
  }

  @Ignore
  public void testShow() {
    // This test case is intended ignored, but to make sure it compiles correctly
    Dataset<Row> df = spark.table("testData");
    df.show();
    df.show(1000);
  }

  public static class Bean implements Serializable {
    private double a = 0.0;
    private Integer[] b = { 0, 1 };
    private Map<String, int[]> c = ImmutableMap.of("hello", new int[] { 1, 2 });
    private List<String> d = Arrays.asList("floppy", "disk");
    private BigInteger e = new BigInteger("1234567");

    public double getA() {
      return a;
    }

    public Integer[] getB() {
      return b;
    }

    public Map<String, int[]> getC() {
      return c;
    }

    public List<String> getD() {
      return d;
    }

    public BigInteger getE() { return e; }
  }

  void validateDataFrameWithBeans(Bean bean, Dataset<Row> df) {
    StructType schema = df.schema();
    Assert.assertEquals(new StructField("a", DoubleType$.MODULE$, false, Metadata.empty()),
      schema.apply("a"));
    Assert.assertEquals(
      new StructField("b", new ArrayType(IntegerType$.MODULE$, true), true, Metadata.empty()),
      schema.apply("b"));
    ArrayType valueType = new ArrayType(DataTypes.IntegerType, false);
    MapType mapType = new MapType(DataTypes.StringType, valueType, true);
    Assert.assertEquals(
      new StructField("c", mapType, true, Metadata.empty()),
      schema.apply("c"));
    Assert.assertEquals(
      new StructField("d", new ArrayType(DataTypes.StringType, true), true, Metadata.empty()),
      schema.apply("d"));
    Assert.assertEquals(new StructField("e", DataTypes.createDecimalType(38,0), true,
      Metadata.empty()), schema.apply("e"));
    Row first = df.select("a", "b", "c", "d", "e").first();
    Assert.assertEquals(bean.getA(), first.getDouble(0), 0.0);
    // Now Java lists and maps are converted to Scala Seq's and Map's. Once we get a Seq below,
    // verify that it has the expected length, and contains expected elements.
    Seq<Integer> result = first.getAs(1);
    Assert.assertEquals(bean.getB().length, result.length());
    for (int i = 0; i < result.length(); i++) {
      Assert.assertEquals(bean.getB()[i], result.apply(i));
    }
    @SuppressWarnings("unchecked")
    Seq<Integer> outputBuffer = (Seq<Integer>) first.getJavaMap(2).get("hello");
    Assert.assertArrayEquals(
      bean.getC().get("hello"),
      Ints.toArray(JavaConverters.seqAsJavaListConverter(outputBuffer).asJava()));
    Seq<String> d = first.getAs(3);
    Assert.assertEquals(bean.getD().size(), d.length());
    for (int i = 0; i < d.length(); i++) {
      Assert.assertEquals(bean.getD().get(i), d.apply(i));
    }
    // Java.math.BigInteger is equavient to Spark Decimal(38,0)
    Assert.assertEquals(new BigDecimal(bean.getE()), first.getDecimal(4));
  }

  @Test
  public void testCreateDataFrameFromLocalJavaBeans() {
    Bean bean = new Bean();
    List<Bean> data = Arrays.asList(bean);
    Dataset<Row> df = spark.createDataFrame(data, Bean.class);
    validateDataFrameWithBeans(bean, df);
  }

  @Test
  public void testCreateDataFrameFromJavaBeans() {
    Bean bean = new Bean();
    JavaRDD<Bean> rdd = jsc.parallelize(Arrays.asList(bean));
    Dataset<Row> df = spark.createDataFrame(rdd, Bean.class);
    validateDataFrameWithBeans(bean, df);
  }

  @Test
  public void testCreateDataFromFromList() {
    StructType schema = createStructType(Arrays.asList(createStructField("i", IntegerType, true)));
    List<Row> rows = Arrays.asList(RowFactory.create(0));
    Dataset<Row> df = spark.createDataFrame(rows, schema);
    List<Row> result = df.collectAsList();
    Assert.assertEquals(1, result.size());
  }

  @Test
  public void testCreateStructTypeFromList(){
    List<StructField> fields1 = new ArrayList<>();
    fields1.add(new StructField("id", DataTypes.StringType, true, Metadata.empty()));
    StructType schema1 = StructType$.MODULE$.apply(fields1);
    Assert.assertEquals(0, schema1.fieldIndex("id"));

    List<StructField> fields2 =
        Arrays.asList(new StructField("id", DataTypes.StringType, true, Metadata.empty()));
    StructType schema2 = StructType$.MODULE$.apply(fields2);
    Assert.assertEquals(0, schema2.fieldIndex("id"));
  }

  private static final Comparator<Row> crosstabRowComparator = new Comparator<Row>() {
    @Override
    public int compare(Row row1, Row row2) {
      String item1 = row1.getString(0);
      String item2 = row2.getString(0);
      return item1.compareTo(item2);
    }
  };

  @Test
  public void testCrosstab() {
    Dataset<Row> df = spark.table("testData2");
    Dataset<Row> crosstab = df.stat().crosstab("a", "b");
    String[] columnNames = crosstab.schema().fieldNames();
    Assert.assertEquals("a_b", columnNames[0]);
    Assert.assertEquals("2", columnNames[1]);
    Assert.assertEquals("1", columnNames[2]);
    List<Row> rows = crosstab.collectAsList();
    Collections.sort(rows, crosstabRowComparator);
    Integer count = 1;
    for (Row row : rows) {
      Assert.assertEquals(row.get(0).toString(), count.toString());
      Assert.assertEquals(1L, row.getLong(1));
      Assert.assertEquals(1L, row.getLong(2));
      count++;
    }
  }

  @Test
  public void testFrequentItems() {
    Dataset<Row> df = spark.table("testData2");
    String[] cols = {"a"};
    Dataset<Row> results = df.stat().freqItems(cols, 0.2);
    Assert.assertTrue(results.collectAsList().get(0).getSeq(0).contains(1));
  }

  @Test
  public void testCorrelation() {
    Dataset<Row> df = spark.table("testData2");
    Double pearsonCorr = df.stat().corr("a", "b", "pearson");
    Assert.assertTrue(Math.abs(pearsonCorr) < 1.0e-6);
  }

  @Test
  public void testCovariance() {
    Dataset<Row> df = spark.table("testData2");
    Double result = df.stat().cov("a", "b");
    Assert.assertTrue(Math.abs(result) < 1.0e-6);
  }

  @Test
  public void testSampleBy() {
    Dataset<Row> df = spark.range(0, 100, 1, 2).select(col("id").mod(3).as("key"));
    Dataset<Row> sampled = df.stat().<Integer>sampleBy("key", ImmutableMap.of(0, 0.1, 1, 0.2), 0L);
    List<Row> actual = sampled.groupBy("key").count().orderBy("key").collectAsList();
    Assert.assertEquals(0, actual.get(0).getLong(0));
    Assert.assertTrue(0 <= actual.get(0).getLong(1) && actual.get(0).getLong(1) <= 8);
    Assert.assertEquals(1, actual.get(1).getLong(0));
    Assert.assertTrue(2 <= actual.get(1).getLong(1) && actual.get(1).getLong(1) <= 13);
  }

  @Test
  public void pivot() {
    Dataset<Row> df = spark.table("courseSales");
    List<Row> actual = df.groupBy("year")
      .pivot("course", Arrays.<Object>asList("dotNET", "Java"))
      .agg(sum("earnings")).orderBy("year").collectAsList();

    Assert.assertEquals(2012, actual.get(0).getInt(0));
    Assert.assertEquals(15000.0, actual.get(0).getDouble(1), 0.01);
    Assert.assertEquals(20000.0, actual.get(0).getDouble(2), 0.01);

    Assert.assertEquals(2013, actual.get(1).getInt(0));
    Assert.assertEquals(48000.0, actual.get(1).getDouble(1), 0.01);
    Assert.assertEquals(30000.0, actual.get(1).getDouble(2), 0.01);
  }

  private String getResource(String resource) {
    try {
      // The following "getResource" has different behaviors in SBT and Maven.
      // When running in Jenkins, the file path may contain "@" when there are multiple
      // SparkPullRequestBuilders running in the same worker
      // (e.g., /home/jenkins/workspace/SparkPullRequestBuilder@2)
      // When running in SBT, "@" in the file path will be returned as "@", however,
      // when running in Maven, "@" will be encoded as "%40".
      // Therefore, we convert it to URI then call "getPath" to decode it back so that it can both
      // work both in SBT and Maven.
      URL url = Thread.currentThread().getContextClassLoader().getResource(resource);
      return url.toURI().getPath();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testGenericLoad() {
    Dataset<Row> df1 = spark.read().format("text").load(getResource("text-suite.txt"));
    Assert.assertEquals(4L, df1.count());

    Dataset<Row> df2 = spark.read().format("text").load(
      getResource("text-suite.txt"),
      getResource("text-suite2.txt"));
    Assert.assertEquals(5L, df2.count());
  }

  @Test
  public void testTextLoad() {
    Dataset<String> ds1 = spark.read().text(getResource("text-suite.txt"));
    Assert.assertEquals(4L, ds1.count());

    Dataset<String> ds2 = spark.read().text(
      getResource("text-suite.txt"),
      getResource("text-suite2.txt"));
    Assert.assertEquals(5L, ds2.count());
  }

  @Test
  public void testCountMinSketch() {
    Dataset<Long> df = spark.range(1000);

    CountMinSketch sketch1 = df.stat().countMinSketch("id", 10, 20, 42);
    Assert.assertEquals(sketch1.totalCount(), 1000);
    Assert.assertEquals(sketch1.depth(), 10);
    Assert.assertEquals(sketch1.width(), 20);

    CountMinSketch sketch2 = df.stat().countMinSketch(col("id"), 10, 20, 42);
    Assert.assertEquals(sketch2.totalCount(), 1000);
    Assert.assertEquals(sketch2.depth(), 10);
    Assert.assertEquals(sketch2.width(), 20);

    CountMinSketch sketch3 = df.stat().countMinSketch("id", 0.001, 0.99, 42);
    Assert.assertEquals(sketch3.totalCount(), 1000);
    Assert.assertEquals(sketch3.relativeError(), 0.001, 1e-4);
    Assert.assertEquals(sketch3.confidence(), 0.99, 5e-3);

    CountMinSketch sketch4 = df.stat().countMinSketch(col("id"), 0.001, 0.99, 42);
    Assert.assertEquals(sketch4.totalCount(), 1000);
    Assert.assertEquals(sketch4.relativeError(), 0.001, 1e-4);
    Assert.assertEquals(sketch4.confidence(), 0.99, 5e-3);
  }

  @Test
  public void testBloomFilter() {
    Dataset<Long> df = spark.range(1000);

    BloomFilter filter1 = df.stat().bloomFilter("id", 1000, 0.03);
    Assert.assertTrue(filter1.expectedFpp() - 0.03 < 1e-3);
    for (int i = 0; i < 1000; i++) {
      Assert.assertTrue(filter1.mightContain(i));
    }

    BloomFilter filter2 = df.stat().bloomFilter(col("id").multiply(3), 1000, 0.03);
    Assert.assertTrue(filter2.expectedFpp() - 0.03 < 1e-3);
    for (int i = 0; i < 1000; i++) {
      Assert.assertTrue(filter2.mightContain(i * 3));
    }

    BloomFilter filter3 = df.stat().bloomFilter("id", 1000, 64 * 5);
    Assert.assertTrue(filter3.bitSize() == 64 * 5);
    for (int i = 0; i < 1000; i++) {
      Assert.assertTrue(filter3.mightContain(i));
    }

    BloomFilter filter4 = df.stat().bloomFilter(col("id").multiply(3), 1000, 64 * 5);
    Assert.assertTrue(filter4.bitSize() == 64 * 5);
    for (int i = 0; i < 1000; i++) {
      Assert.assertTrue(filter4.mightContain(i * 3));
    }
  }
}
