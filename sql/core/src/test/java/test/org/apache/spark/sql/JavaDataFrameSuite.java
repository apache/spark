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
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

import scala.collection.Seq;
import scala.jdk.javaapi.CollectionConverters;

import com.google.common.primitives.Ints;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.function.Executable;

import org.apache.spark.SparkArithmeticException;
import org.apache.spark.SparkRuntimeException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.LogicalRDD;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.test.TestSparkSession;
import org.apache.spark.sql.types.*;
import org.apache.spark.util.sketch.BloomFilter;
import org.apache.spark.util.sketch.CountMinSketch;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.*;

public class JavaDataFrameSuite {
  private transient TestSparkSession spark;
  private transient JavaSparkContext jsc;

  @BeforeEach
  public void setUp() {
    // Trigger static initializer of TestData
    spark = new TestSparkSession();
    jsc = new JavaSparkContext(spark.sparkContext());
    spark.loadTestData();
  }

  @AfterEach
  public void tearDown() {
    spark.stop();
    spark = null;
  }

  @Test
  public void testExecution() {
    Dataset<Row> df = spark.table("testData").filter("key = 1");
    Assertions.assertEquals(1, df.select("key").collectAsList().get(0).get(0));
  }

  @Test
  public void testCollectAndTake() {
    Dataset<Row> df = spark.table("testData").filter("key = 1 or key = 2 or key = 3");
    Assertions.assertEquals(3, df.select("key").collectAsList().size());
    Assertions.assertEquals(2, df.select("key").takeAsList(2).size());
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
    df.groupBy().agg(count_distinct(col("key"), col("value")));
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

  @Disabled
  public void testShow() {
    // This test case is intended ignored, but to make sure it compiles correctly
    Dataset<Row> df = spark.table("testData");
    df.show();
    df.show(1000);
  }

  public static class Bean implements Serializable {
    private double a = 0.0;
    private Integer[] b = { 0, 1 };
    private Map<String, int[]> c = Map.of("hello", new int[] { 1, 2 });
    private List<String> d = Arrays.asList("floppy", "disk");
    private BigInteger e = new BigInteger("1234567");
    private NestedBean f = new NestedBean();
    private NestedBean g = null;

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

    public NestedBean getF() {
      return f;
    }

    public NestedBean getG() {
      return g;
    }

    public static class NestedBean implements Serializable {
      private int a = 1;

      public int getA() {
        return a;
      }
    }
  }

  void validateDataFrameWithBeans(Bean bean, Dataset<Row> df) {
    StructType schema = df.schema();
    Assertions.assertEquals(new StructField("a", DoubleType$.MODULE$, false, Metadata.empty()),
      schema.apply("a"));
    Assertions.assertEquals(
      new StructField("b", new ArrayType(IntegerType$.MODULE$, true), true, Metadata.empty()),
      schema.apply("b"));
    ArrayType valueType = new ArrayType(DataTypes.IntegerType, false);
    MapType mapType = new MapType(DataTypes.StringType, valueType, true);
    Assertions.assertEquals(
      new StructField("c", mapType, true, Metadata.empty()),
      schema.apply("c"));
    Assertions.assertEquals(
      new StructField("d", new ArrayType(DataTypes.StringType, true), true, Metadata.empty()),
      schema.apply("d"));
    Assertions.assertEquals(new StructField("e", DataTypes.createDecimalType(38,0), true,
      Metadata.empty()), schema.apply("e"));
    StructType nestedBeanType =
      DataTypes.createStructType(Collections.singletonList(new StructField(
        "a", IntegerType$.MODULE$, false, Metadata.empty())));
    Assertions.assertEquals(new StructField("f", nestedBeanType, true, Metadata.empty()),
      schema.apply("f"));
    Assertions.assertEquals(new StructField("g", nestedBeanType, true, Metadata.empty()),
      schema.apply("g"));
    Row first = df.select("a", "b", "c", "d", "e", "f", "g").first();
    Assertions.assertEquals(bean.getA(), first.getDouble(0), 0.0);
    // Now Java lists and maps are converted to Scala Seq's and Map's. Once we get a Seq below,
    // verify that it has the expected length, and contains expected elements.
    Seq<Integer> result = first.getAs(1);
    Assertions.assertEquals(bean.getB().length, result.length());
    for (int i = 0; i < result.length(); i++) {
      Assertions.assertEquals(bean.getB()[i], result.apply(i));
    }
    @SuppressWarnings("unchecked")
    Seq<Integer> outputBuffer = (Seq<Integer>) first.getJavaMap(2).get("hello");
    Assertions.assertArrayEquals(
      bean.getC().get("hello"),
      Ints.toArray(CollectionConverters.asJava(outputBuffer)));
    Seq<String> d = first.getAs(3);
    Assertions.assertEquals(bean.getD().size(), d.length());
    for (int i = 0; i < d.length(); i++) {
      Assertions.assertEquals(bean.getD().get(i), d.apply(i));
    }
    // Java.math.BigInteger is equivalent to Spark Decimal(38,0)
    Assertions.assertEquals(new BigDecimal(bean.getE()), first.getDecimal(4));
    Row nested = first.getStruct(5);
    Assertions.assertEquals(bean.getF().getA(), nested.getInt(0));
    Assertions.assertTrue(first.isNullAt(6));
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
    Assertions.assertEquals(1, result.size());
  }

  @Test
  public void testCreateStructTypeFromList(){
    List<StructField> fields1 = new ArrayList<>();
    fields1.add(new StructField("id", DataTypes.StringType, true, Metadata.empty()));
    StructType schema1 = StructType$.MODULE$.apply(fields1);
    Assertions.assertEquals(0, schema1.fieldIndex("id"));

    List<StructField> fields2 =
        Arrays.asList(new StructField("id", DataTypes.StringType, true, Metadata.empty()));
    StructType schema2 = StructType$.MODULE$.apply(fields2);
    Assertions.assertEquals(0, schema2.fieldIndex("id"));
  }

  private static final Comparator<Row> crosstabRowComparator = (row1, row2) -> {
    String item1 = row1.getString(0);
    String item2 = row2.getString(0);
    return item1.compareTo(item2);
  };

  @Test
  public void testCrosstab() {
    Dataset<Row> df = spark.table("testData2");
    Dataset<Row> crosstab = df.stat().crosstab("a", "b");
    String[] columnNames = crosstab.schema().fieldNames();
    Assertions.assertEquals("a_b", columnNames[0]);
    Assertions.assertEquals("1", columnNames[1]);
    Assertions.assertEquals("2", columnNames[2]);
    List<Row> rows = crosstab.collectAsList();
    rows.sort(crosstabRowComparator);
    Integer count = 1;
    for (Row row : rows) {
      Assertions.assertEquals(row.get(0).toString(), count.toString());
      Assertions.assertEquals(1L, row.getLong(1));
      Assertions.assertEquals(1L, row.getLong(2));
      count++;
    }
  }

  @Test
  public void testFrequentItems() {
    Dataset<Row> df = spark.table("testData2");
    String[] cols = {"a"};
    Dataset<Row> results = df.stat().freqItems(cols, 0.2);
    Assertions.assertTrue(results.collectAsList().get(0).getSeq(0).contains(1));
  }

  @Test
  public void testCorrelation() {
    Dataset<Row> df = spark.table("testData2");
    Double pearsonCorr = df.stat().corr("a", "b", "pearson");
    Assertions.assertTrue(Math.abs(pearsonCorr) < 1.0e-6);
  }

  @Test
  public void testCovariance() {
    Dataset<Row> df = spark.table("testData2");
    Double result = df.stat().cov("a", "b");
    Assertions.assertTrue(Math.abs(result) < 1.0e-6);
  }

  @Test
  public void testSampleBy() {
    Dataset<Row> df = spark.range(0, 100, 1, 2).select(col("id").mod(3).as("key"));
    Dataset<Row> sampled = df.stat().sampleBy("key", Map.of(0, 0.1, 1, 0.2), 0L);
    List<Row> actual = sampled.groupBy("key").count().orderBy("key").collectAsList();
    Assertions.assertEquals(0, actual.get(0).getLong(0));
    Assertions.assertTrue(0 <= actual.get(0).getLong(1) && actual.get(0).getLong(1) <= 8);
    Assertions.assertEquals(1, actual.get(1).getLong(0));
    Assertions.assertTrue(2 <= actual.get(1).getLong(1) && actual.get(1).getLong(1) <= 13);
  }

  @Test
  public void testwithColumns() {
    Dataset<Row> df = spark.table("testData2");
    Map<String, Column> colMaps = new HashMap<>();
    colMaps.put("a1", col("a"));
    colMaps.put("b1", col("b"));

    StructType expected = df.withColumn("a1", col("a")).withColumn("b1", col("b")).schema();
    StructType actual = df.withColumns(colMaps).schema();
    // Validate geting same result with withColumn loop call
    Assertions.assertEquals(expected, actual);
    // Validate the col names
    Assertions.assertArrayEquals(actual.fieldNames(), new String[] {"a", "b", "a1", "b1"});
  }

  @Test
  public void testSampleByColumn() {
    Dataset<Row> df = spark.range(0, 100, 1, 2).select(col("id").mod(3).as("key"));
    Dataset<Row> sampled = df.stat().sampleBy(col("key"), Map.of(0, 0.1, 1, 0.2), 0L);
    List<Row> actual = sampled.groupBy("key").count().orderBy("key").collectAsList();
    Assertions.assertEquals(0, actual.get(0).getLong(0));
    Assertions.assertTrue(0 <= actual.get(0).getLong(1) && actual.get(0).getLong(1) <= 8);
    Assertions.assertEquals(1, actual.get(1).getLong(0));
    Assertions.assertTrue(2 <= actual.get(1).getLong(1) && actual.get(1).getLong(1) <= 13);
  }

  @Test
  public void pivot() {
    Dataset<Row> df = spark.table("courseSales");
    List<Row> actual = df.groupBy("year")
      .pivot("course", Arrays.asList("dotNET", "Java"))
      .agg(sum("earnings")).orderBy("year").collectAsList();

    Assertions.assertEquals(2012, actual.get(0).getInt(0));
    Assertions.assertEquals(15000.0, actual.get(0).getDouble(1), 0.01);
    Assertions.assertEquals(20000.0, actual.get(0).getDouble(2), 0.01);

    Assertions.assertEquals(2013, actual.get(1).getInt(0));
    Assertions.assertEquals(48000.0, actual.get(1).getDouble(1), 0.01);
    Assertions.assertEquals(30000.0, actual.get(1).getDouble(2), 0.01);
  }

  @Test
  public void pivotColumnValues() {
    Dataset<Row> df = spark.table("courseSales");
    List<Row> actual = df.groupBy("year")
      .pivot(col("course"), Arrays.asList(lit("dotNET"), lit("Java")))
      .agg(sum("earnings")).orderBy("year").collectAsList();

    Assertions.assertEquals(2012, actual.get(0).getInt(0));
    Assertions.assertEquals(15000.0, actual.get(0).getDouble(1), 0.01);
    Assertions.assertEquals(20000.0, actual.get(0).getDouble(2), 0.01);

    Assertions.assertEquals(2013, actual.get(1).getInt(0));
    Assertions.assertEquals(48000.0, actual.get(1).getDouble(1), 0.01);
    Assertions.assertEquals(30000.0, actual.get(1).getDouble(2), 0.01);
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
    Dataset<Row> df1 = spark.read().format("text").load(getResource("test-data/text-suite.txt"));
    Assertions.assertEquals(4L, df1.count());

    Dataset<Row> df2 = spark.read().format("text").load(
      getResource("test-data/text-suite.txt"),
      getResource("test-data/text-suite2.txt"));
    Assertions.assertEquals(5L, df2.count());
  }

  @Test
  public void testTextLoad() {
    Dataset<String> ds1 = spark.read().textFile(getResource("test-data/text-suite.txt"));
    Assertions.assertEquals(4L, ds1.count());

    Dataset<String> ds2 = spark.read().textFile(
      getResource("test-data/text-suite.txt"),
      getResource("test-data/text-suite2.txt"));
    Assertions.assertEquals(5L, ds2.count());
  }

  @Test
  public void testCountMinSketch() {
    Dataset<Long> df = spark.range(1000);

    CountMinSketch sketch1 = df.stat().countMinSketch("id", 10, 20, 42);
    Assertions.assertEquals(1000, sketch1.totalCount());
    Assertions.assertEquals(10, sketch1.depth());
    Assertions.assertEquals(20, sketch1.width());

    CountMinSketch sketch2 = df.stat().countMinSketch(col("id"), 10, 20, 42);
    Assertions.assertEquals(1000, sketch2.totalCount());
    Assertions.assertEquals(10, sketch2.depth());
    Assertions.assertEquals(20, sketch2.width());

    CountMinSketch sketch3 = df.stat().countMinSketch("id", 0.001, 0.99, 42);
    Assertions.assertEquals(1000, sketch3.totalCount());
    Assertions.assertEquals(0.001, sketch3.relativeError(), 1.0e-4);
    Assertions.assertEquals(0.99, sketch3.confidence(), 5.0e-3);

    CountMinSketch sketch4 = df.stat().countMinSketch(col("id"), 0.001, 0.99, 42);
    Assertions.assertEquals(1000, sketch4.totalCount());
    Assertions.assertEquals(0.001, sketch4.relativeError(), 1.0e-4);
    Assertions.assertEquals(0.99, sketch4.confidence(), 5.0e-3);
  }

  @Test
  public void testBloomFilter() {
    Dataset<Long> df = spark.range(1000);

    BloomFilter filter1 = df.stat().bloomFilter("id", 1000, 0.03);
    Assertions.assertTrue(filter1.expectedFpp() - 0.03 < 1e-3);
    for (int i = 0; i < 1000; i++) {
      Assertions.assertTrue(filter1.mightContain(i));
    }

    BloomFilter filter2 = df.stat().bloomFilter(col("id").multiply(3), 1000, 0.03);
    Assertions.assertTrue(filter2.expectedFpp() - 0.03 < 1e-3);
    for (int i = 0; i < 1000; i++) {
      Assertions.assertTrue(filter2.mightContain(i * 3));
    }

    BloomFilter filter3 = df.stat().bloomFilter("id", 1000, 64 * 5);
    Assertions.assertEquals(64 * 5, filter3.bitSize());
    for (int i = 0; i < 1000; i++) {
      Assertions.assertTrue(filter3.mightContain(i));
    }

    BloomFilter filter4 = df.stat().bloomFilter(col("id").multiply(3), 1000, 64 * 5);
    Assertions.assertEquals(64 * 5, filter4.bitSize());
    for (int i = 0; i < 1000; i++) {
      Assertions.assertTrue(filter4.mightContain(i * 3));
    }
  }

  public static class BeanWithoutGetter implements Serializable {
    private String a;

    public void setA(String a) {
      this.a = a;
    }
  }

  @Test
  public void testBeanWithoutGetter() {
    BeanWithoutGetter bean = new BeanWithoutGetter();
    List<BeanWithoutGetter> data = Arrays.asList(bean);
    Dataset<Row> df = spark.createDataFrame(data, BeanWithoutGetter.class);
    Assertions.assertEquals(0, df.schema().length());
    Assertions.assertEquals(1, df.collectAsList().size());
  }

  public static class WideBean implements Serializable {
    public int getPrimitiveInt() { return 1; }
    public boolean isPrimitiveBoolean() { return true; }
    public Long getBoxedLong() { return 2L; }
    public Double getNullBoxedDouble() { return null; }
    @Nonnull public Integer getNonNullBoxedInt() { return 3; }
    @Nonnull public String getNonNullString() { return "a"; }
    public String getNullString() { return null; }
    public Bean.NestedBean getNested() { return new Bean.NestedBean(); }
    public List<String> getList() { return Arrays.asList("b", null); }
    public Map<String, Integer> getMap() { return Map.of("k", 4); }
    public String[] getStringArray() { return new String[] { "c", null }; }
    public int[] getIntArray() { return new int[] { 5, 6 }; }
    public byte[] getBinary() { return new byte[] { 7, 8 }; }
    public BigDecimal getDecimal() { return new BigDecimal("12.34"); }
    public Date getSqlDate() { return Date.valueOf("2024-01-02"); }
    public Timestamp getSqlTimestamp() { return Timestamp.valueOf("2024-01-02 03:04:05.123456"); }
    public LocalDate getLocalDate() { return LocalDate.of(2024, 1, 3); }
    public Instant getInstant() { return Instant.parse("2024-01-03T03:04:05.123456Z"); }
  }

  @Test
  public void testCreateDataFrameSchemaAndRowsForBeans() {
    // SPARK-55396: createDataFrame takes its schema from the bean encoder and converts
    // values with the encoder's serializer.
    Map<String, Object> expected = new HashMap<>();
    expected.put("primitiveInt", 1);
    expected.put("primitiveBoolean", true);
    expected.put("boxedLong", 2L);
    expected.put("nullBoxedDouble", null);
    expected.put("nonNullBoxedInt", 3);
    expected.put("nonNullString", "a");
    expected.put("nullString", null);
    expected.put("nested", RowFactory.create(1));
    expected.put("list", toSeq(Arrays.asList("b", null)));
    expected.put("map", CollectionConverters.asScala(Map.of("k", 4)));
    expected.put("stringArray", toSeq(Arrays.asList("c", null)));
    expected.put("intArray", toSeq(Arrays.asList(5, 6)));
    expected.put("binary", new byte[] { 7, 8 });
    expected.put("decimal", new BigDecimal("12.34").setScale(18));
    expected.put("sqlDate", Date.valueOf("2024-01-02"));
    expected.put("sqlTimestamp", Timestamp.valueOf("2024-01-02 03:04:05.123456"));
    expected.put("localDate", Date.valueOf(LocalDate.of(2024, 1, 3)));
    expected.put("instant", Timestamp.from(Instant.parse("2024-01-03T03:04:05.123456Z")));

    StructType schema = Encoders.bean(WideBean.class).schema();
    Assertions.assertEquals(expected.keySet(), new HashSet<>(Arrays.asList(schema.fieldNames())));
    Assertions.assertFalse(schema.apply("nonNullBoxedInt").nullable());
    Row expectedRow =
      RowFactory.create(Arrays.stream(schema.fieldNames()).map(expected::get).toArray());

    List<WideBean> data = Arrays.asList(new WideBean());
    for (Dataset<Row> df : Arrays.asList(
        spark.createDataFrame(data, WideBean.class),
        spark.createDataFrame(jsc.parallelize(data), WideBean.class))) {
      Assertions.assertEquals(schema, df.schema());
      Assertions.assertEquals(Arrays.asList(expectedRow), df.collectAsList());
    }
  }

  private static <T> scala.collection.immutable.Seq<T> toSeq(List<T> list) {
    return CollectionConverters.asScala(list).toSeq();
  }

  public static class BeanWithEnum implements Serializable {
    public TimeUnit getUnit() { return TimeUnit.SECONDS; }
  }

  @Test
  public void testCreateDataFrameWithEnumProperty() {
    // SPARK-55396: enum properties are stored as their names.
    Dataset<Row> df = spark.createDataFrame(Arrays.asList(new BeanWithEnum()), BeanWithEnum.class);
    Assertions.assertEquals(StringType, df.schema().apply("unit").dataType());
    Assertions.assertEquals(Arrays.asList(RowFactory.create("SECONDS")), df.collectAsList());
  }

  public static class BeanWithNestedBeanCollections implements Serializable {
    public List<Bean.NestedBean> getList() { return Arrays.asList(new Bean.NestedBean()); }
    public Map<String, Bean.NestedBean> getMap() { return Map.of("k", new Bean.NestedBean()); }
    public Bean.NestedBean[] getArray() { return new Bean.NestedBean[] { new Bean.NestedBean() }; }
  }

  @Test
  public void testCreateDataFrameWithNestedBeansInCollections() {
    // SPARK-55396: nested beans inside arrays, lists and maps (see also SPARK-25654).
    Dataset<Row> df = spark.createDataFrame(
      Arrays.asList(new BeanWithNestedBeanCollections()), BeanWithNestedBeanCollections.class);
    Row nested = RowFactory.create(1);
    Row expected = RowFactory.create(
      toSeq(Arrays.asList(nested)),
      toSeq(Arrays.asList(nested)),
      CollectionConverters.asScala(Map.of("k", nested)));
    Assertions.assertEquals(
      Arrays.asList(expected), df.select("array", "list", "map").collectAsList());
  }

  @Test
  public void testCreateDataFramePlanForBeans() {
    // SPARK-55396: the List overload builds a LocalRelation, and the RDD overload a LogicalRDD
    // named after the source RDD.
    List<Bean.NestedBean> data = Arrays.asList(new Bean.NestedBean());
    org.apache.spark.sql.classic.Dataset<Row> fromList =
      spark.createDataFrame(data, Bean.NestedBean.class);
    Assertions.assertInstanceOf(LocalRelation.class, fromList.queryExecution().analyzed());
    JavaRDD<Bean.NestedBean> rdd = jsc.parallelize(data).setName("beans");
    LogicalPlan fromRdd =
      spark.createDataFrame(rdd, Bean.NestedBean.class).queryExecution().analyzed();
    Assertions.assertInstanceOf(LogicalRDD.class, fromRdd);
    Assertions.assertEquals("beans", ((LogicalRDD) fromRdd).rdd().name());
  }

  public static class GetterOnlyBean implements Serializable {
    private final int id;
    private final String name;

    public GetterOnlyBean(int id, String name) {
      this.id = id;
      this.name = name;
    }

    public int getId() { return id; }
    public String getName() { return name; }
  }

  @Test
  public void testCreateDataFrameWithGetterOnlyBean() {
    // SPARK-55396: beans without setters or a no-arg constructor are supported, since only
    // the serializer is used.
    List<GetterOnlyBean> data = Arrays.asList(new GetterOnlyBean(1, "a"));
    for (Dataset<Row> df : Arrays.asList(
        spark.createDataFrame(data, GetterOnlyBean.class),
        spark.createDataFrame(jsc.parallelize(data), GetterOnlyBean.class))) {
      Assertions.assertEquals(Encoders.bean(GetterOnlyBean.class).schema(), df.schema());
      Assertions.assertEquals(Arrays.asList(RowFactory.create(1, "a")), df.collectAsList());
    }
  }

  public static class DecimalBean implements Serializable {
    public BigDecimal getValue() { return new BigDecimal("1e30"); }
  }

  private List<Supplier<List<Row>>> createDecimalBeanDataFrames() {
    List<DecimalBean> data = Arrays.asList(new DecimalBean());
    return Arrays.asList(
      () -> spark.createDataFrame(data, DecimalBean.class).collectAsList(),
      () -> spark.createDataFrame(jsc.parallelize(data), DecimalBean.class).collectAsList());
  }

  private void withAnsi(boolean enabled, Runnable body) {
    String key = SQLConf.ANSI_ENABLED().key();
    String original = spark.conf().get(key);
    spark.conf().set(key, String.valueOf(enabled));
    try {
      body.run();
    } finally {
      spark.conf().set(key, original);
    }
  }

  @Test
  public void testCreateDataFrameWithDecimalOverflowNonAnsi() {
    // SPARK-55396: without ANSI mode, a BigDecimal that doesn't fit decimal(38, 18) is null.
    withAnsi(false, () -> {
      for (Supplier<List<Row>> call : createDecimalBeanDataFrames()) {
        Assertions.assertEquals(Arrays.asList(RowFactory.create((Object) null)), call.get());
      }
    });
  }

  @Test
  public void testCreateDataFrameWithDecimalOverflowAnsi() {
    // SPARK-55396: with ANSI mode, the overflow fails to encode, as in createDataset.
    withAnsi(true, () -> {
      for (Supplier<List<Row>> call : createDecimalBeanDataFrames()) {
        SparkRuntimeException e = Assertions.assertThrows(SparkRuntimeException.class, call::get);
        Assertions.assertEquals("EXPRESSION_ENCODING_FAILED", e.getCondition());
        Assertions.assertInstanceOf(SparkArithmeticException.class, e.getCause());
        Assertions.assertTrue(((SparkArithmeticException) e.getCause()).getCondition()
          .startsWith("NUMERIC_VALUE_OUT_OF_RANGE"));
      }
    });
  }

  @Test
  public void testCreateDataFrameWithNullBean() {
    // SPARK-55396: a null element fails with NOT_NULL_ASSERT_VIOLATION.
    List<Bean.NestedBean> data = Arrays.asList(new Bean.NestedBean(), null);
    List<Executable> calls = Arrays.asList(
      () -> spark.createDataFrame(data, Bean.NestedBean.class).collectAsList(),
      () -> spark.createDataFrame(jsc.parallelize(data), Bean.NestedBean.class).collectAsList());
    for (Executable call : calls) {
      SparkRuntimeException e = Assertions.assertThrows(SparkRuntimeException.class, call);
      Assertions.assertEquals("NOT_NULL_ASSERT_VIOLATION", e.getCondition());
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testJsonRDDToDataFrame() {
    // This is a test for the deprecated API in SPARK-15615.
    JavaRDD<String> rdd = jsc.parallelize(Arrays.asList("{\"a\": 2}"));
    Dataset<Row> df = spark.read().json(rdd);
    Assertions.assertEquals(1L, df.count());
    Assertions.assertEquals(2L, df.collectAsList().get(0).getLong(0));
  }

  public class CircularReference1Bean implements Serializable {
    private CircularReference2Bean child;

    public CircularReference2Bean getChild() {
      return child;
    }

    public void setChild(CircularReference2Bean child) {
      this.child = child;
    }
  }

  public class CircularReference2Bean implements Serializable {
    private CircularReference1Bean child;

    public CircularReference1Bean getChild() {
      return child;
    }

    public void setChild(CircularReference1Bean child) {
      this.child = child;
    }
  }

  // Checks a simple case for DataFrame here and put exhaustive tests for the issue
  // of circular references in `JavaDatasetSuite`.
  @Test
  public void testCircularReferenceBean() {
    CircularReference1Bean bean = new CircularReference1Bean();
    Assertions.assertThrows(UnsupportedOperationException.class,
      () -> spark.createDataFrame(Arrays.asList(bean), CircularReference1Bean.class));
  }

  @Test
  public void testUDF() {
    UserDefinedFunction foo = udf((Integer i, String s) -> i.toString() + s, DataTypes.StringType);
    Dataset<Row> df = spark.table("testData").select(foo.apply(col("key"), col("value")));
    String[] result = df.collectAsList().stream().map(row -> row.getString(0))
      .toArray(String[]::new);
    String[] expected = spark.table("testData").collectAsList().stream()
      .map(row -> row.get(0).toString() + row.getString(1)).toArray(String[]::new);
    Assertions.assertArrayEquals(expected, result);
  }

  @Test
  public void testTransformBase() {
    // SPARK-49961 - transform must have the correct type
    Dataset<Integer> ds = spark.createDataset(Arrays.asList(1,2), Encoders.INT());
    Dataset<Integer> transformed = ds.transform((Dataset<Integer> d) ->
            ds.selectExpr("(value + 1) value").as(Encoders.INT()));
    Integer[] expected = {2, 3};
    Integer[] got = transformed.collectAsList().toArray(new Integer[0]);
    Arrays.sort(got);
    Assertions.assertArrayEquals(expected, got);
  }

  @Test
  public void testTransformAsClassic() {
    // SPARK-49961 - transform must have the correct type
    org.apache.spark.sql.classic.Dataset<Integer> ds =
            spark.createDataset(Arrays.asList(1,2), Encoders.INT());
    org.apache.spark.sql.classic.Dataset<Integer> transformed =
            ds.transform((Dataset<Integer> d) ->
              ds.selectExpr("(value + 1) value").as(Encoders.INT()));
    Integer[] expected = {2, 3};
    Integer[] got = transformed.collectAsList().toArray(new Integer[0]);
    Arrays.sort(got);
    Assertions.assertArrayEquals(expected, got);
  }
}
