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
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.junit.*;

import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.catalyst.util.TimestampFormatter;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import org.apache.spark.sql.test.TestSparkSession;
import scala.Tuple2;

public class JavaBeanDeserializationSuite implements Serializable {

  private TestSparkSession spark;

  @Before
  public void setUp() {
    spark = new TestSparkSession();
  }

  @After
  public void tearDown() {
    spark.stop();
    spark = null;
  }

  private static final List<ArrayRecord> ARRAY_RECORDS = new ArrayList<>();

  static {
    ARRAY_RECORDS.add(
      new ArrayRecord(1, Arrays.asList(new Interval(111, 211), new Interval(121, 221)),
              new int[] { 11, 12, 13, 14 })
    );
    ARRAY_RECORDS.add(
      new ArrayRecord(2, Arrays.asList(new Interval(112, 212), new Interval(122, 222)),
              new int[] { 21, 22, 23, 24 })
    );
    ARRAY_RECORDS.add(
      new ArrayRecord(3, Arrays.asList(new Interval(113, 213), new Interval(123, 223)),
              new int[] { 31, 32, 33, 34 })
    );
  }

  @Test
  public void testBeanWithArrayFieldDeserialization() {
    Encoder<ArrayRecord> encoder = Encoders.bean(ArrayRecord.class);

    Dataset<ArrayRecord> dataset = spark
      .read()
      .format("json")
      .schema("id int, intervals array<struct<startTime: bigint, endTime: bigint>>, " +
          "ints array<int>")
      .load("src/test/resources/test-data/with-array-fields.json")
      .as(encoder);

    List<ArrayRecord> records = dataset.collectAsList();
    Assert.assertEquals(ARRAY_RECORDS, records);
  }

  private static final List<MapRecord> MAP_RECORDS = new ArrayList<>();

  static {
    MAP_RECORDS.add(new MapRecord(1,
      toMap(Arrays.asList("a", "b"), Arrays.asList(new Interval(111, 211), new Interval(121, 221)))
    ));
    MAP_RECORDS.add(new MapRecord(2,
      toMap(Arrays.asList("a", "b"), Arrays.asList(new Interval(112, 212), new Interval(122, 222)))
    ));
    MAP_RECORDS.add(new MapRecord(3,
      toMap(Arrays.asList("a", "b"), Arrays.asList(new Interval(113, 213), new Interval(123, 223)))
    ));
    MAP_RECORDS.add(new MapRecord(4, new HashMap<>()));
    MAP_RECORDS.add(new MapRecord(5, null));
  }

  private static <K, V> Map<K, V> toMap(Collection<K> keys, Collection<V> values) {
    Map<K, V> map = new HashMap<>();
    Iterator<K> keyI = keys.iterator();
    Iterator<V> valueI = values.iterator();
    while (keyI.hasNext() && valueI.hasNext()) {
      map.put(keyI.next(), valueI.next());
    }
    return map;
  }

  @Test
  public void testBeanWithMapFieldsDeserialization() {

    Encoder<MapRecord> encoder = Encoders.bean(MapRecord.class);

    Dataset<MapRecord> dataset = spark
      .read()
      .format("json")
      .schema("id int, intervals map<string, struct<startTime: bigint, endTime: bigint>>")
      .load("src/test/resources/test-data/with-map-fields.json")
      .as(encoder);

    List<MapRecord> records = dataset.collectAsList();

    Assert.assertEquals(MAP_RECORDS, records);
  }

  @Test
  public void testSpark22000() {
    List<Row> inputRows = new ArrayList<>();
    List<RecordSpark22000> expectedRecords = new ArrayList<>();

    for (long idx = 0 ; idx < 5 ; idx++) {
      Row row = createRecordSpark22000Row(idx);
      inputRows.add(row);
      expectedRecords.add(createRecordSpark22000(row));
    }

    // Here we try to convert the fields, from any types to string.
    // Before applying SPARK-22000, Spark called toString() against variable which type might
    // be primitive.
    // SPARK-22000 it calls String.valueOf() which finally calls toString() but handles boxing
    // if the type is primitive.
    Encoder<RecordSpark22000> encoder = Encoders.bean(RecordSpark22000.class);

    StructType schema = new StructType()
      .add("shortField", DataTypes.ShortType)
      .add("intField", DataTypes.IntegerType)
      .add("longField", DataTypes.LongType)
      .add("floatField", DataTypes.FloatType)
      .add("doubleField", DataTypes.DoubleType)
      .add("stringField", DataTypes.StringType)
      .add("booleanField", DataTypes.BooleanType)
      .add("timestampField", DataTypes.TimestampType)
      // explicitly setting nullable = true to make clear the intention
      .add("nullIntField", DataTypes.IntegerType, true);

    Dataset<Row> dataFrame = spark.createDataFrame(inputRows, schema);
    Dataset<RecordSpark22000> dataset = dataFrame.as(encoder);

    List<RecordSpark22000> records = dataset.collectAsList();

    Assert.assertEquals(expectedRecords, records);
  }

  @Test
  public void testSpark22000FailToUpcast() {
    List<Row> inputRows = new ArrayList<>();
    for (long idx = 0 ; idx < 5 ; idx++) {
      Row row = createRecordSpark22000FailToUpcastRow(idx);
      inputRows.add(row);
    }

    // Here we try to convert the fields, from string type to int, which upcast doesn't help.
    Encoder<RecordSpark22000FailToUpcast> encoder =
            Encoders.bean(RecordSpark22000FailToUpcast.class);

    StructType schema = new StructType().add("id", DataTypes.StringType);

    Dataset<Row> dataFrame = spark.createDataFrame(inputRows, schema);

    AnalysisException e = Assert.assertThrows(AnalysisException.class,
      () -> dataFrame.as(encoder).collect());
    Assert.assertTrue(e.getMessage().contains("Cannot up cast "));
  }

  private static Row createRecordSpark22000Row(Long index) {
    Object[] values = new Object[] {
            index.shortValue(),
            index.intValue(),
            index,
            index.floatValue(),
            index.doubleValue(),
            String.valueOf(index),
            index % 2 == 0,
            new java.sql.Timestamp(System.currentTimeMillis()),
            null
    };
    return new GenericRow(values);
  }

  private static String timestampToString(Timestamp ts) {
    String timestampString = String.valueOf(ts);
    String formatted = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ts);

    if (timestampString.length() > 19 && !timestampString.substring(19).equals(".0")) {
      return formatted + timestampString.substring(19);
    } else {
      return formatted;
    }
  }

  private static RecordSpark22000 createRecordSpark22000(Row recordRow) {
    RecordSpark22000 record = new RecordSpark22000();
    record.setShortField(String.valueOf(recordRow.getShort(0)));
    record.setIntField(String.valueOf(recordRow.getInt(1)));
    record.setLongField(String.valueOf(recordRow.getLong(2)));
    record.setFloatField(String.valueOf(recordRow.getFloat(3)));
    record.setDoubleField(String.valueOf(recordRow.getDouble(4)));
    record.setStringField(recordRow.getString(5));
    record.setBooleanField(String.valueOf(recordRow.getBoolean(6)));
    record.setTimestampField(timestampToString(recordRow.getTimestamp(7)));
    // This would figure out that null value will not become "null".
    record.setNullIntField(null);
    return record;
  }

  private static Row createRecordSpark22000FailToUpcastRow(Long index) {
    Object[] values = new Object[] { String.valueOf(index) };
    return new GenericRow(values);
  }

  public static class ArrayRecord {

    private int id;
    private List<Interval> intervals;
    private int[] ints;

    public ArrayRecord() { }

    ArrayRecord(int id, List<Interval> intervals, int[] ints) {
      this.id = id;
      this.intervals = intervals;
      this.ints = ints;
    }

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public List<Interval> getIntervals() {
      return intervals;
    }

    public void setIntervals(List<Interval> intervals) {
      this.intervals = intervals;
    }

    public int[] getInts() {
      return ints;
    }

    public void setInts(int[] ints) {
      this.ints = ints;
    }

    @Override
    public int hashCode() {
      return id ^ Objects.hashCode(intervals) ^ Objects.hashCode(ints);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof ArrayRecord)) return false;
      ArrayRecord other = (ArrayRecord) obj;
      return (other.id == this.id) && Objects.equals(other.intervals, this.intervals) &&
              Arrays.equals(other.ints, ints);
    }

    @Override
    public String toString() {
      return String.format("{ id: %d, intervals: %s, ints: %s }", id, intervals,
              Arrays.toString(ints));
    }
  }

  public static class MapRecord {

    private int id;
    private Map<String, Interval> intervals;

    public MapRecord() { }

    MapRecord(int id, Map<String, Interval> intervals) {
      this.id = id;
      this.intervals = intervals;
    }

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public Map<String, Interval> getIntervals() {
      return intervals;
    }

    public void setIntervals(Map<String, Interval> intervals) {
      this.intervals = intervals;
    }

    @Override
    public int hashCode() {
      return id ^ Objects.hashCode(intervals);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof MapRecord)) return false;
      MapRecord other = (MapRecord) obj;
      return (other.id == this.id) && Objects.equals(other.intervals, this.intervals);
    }

    @Override
    public String toString() {
      return String.format("{ id: %d, intervals: %s }", id, intervals);
    }
  }

  public static class Interval {

    private long startTime;
    private long endTime;

    public Interval() { }

    Interval(long startTime, long endTime) {
      this.startTime = startTime;
      this.endTime = endTime;
    }

    public long getStartTime() {
      return startTime;
    }

    public void setStartTime(long startTime) {
      this.startTime = startTime;
    }

    public long getEndTime() {
      return endTime;
    }

    public void setEndTime(long endTime) {
      this.endTime = endTime;
    }

    @Override
    public int hashCode() {
      return Long.hashCode(startTime) ^ Long.hashCode(endTime);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof Interval)) return false;
      Interval other = (Interval) obj;
      return (other.startTime == this.startTime) && (other.endTime == this.endTime);
    }

    @Override
    public String toString() {
      return String.format("[%d,%d]", startTime, endTime);
    }
  }

  public static final class RecordSpark22000 {
    private String shortField;
    private String intField;
    private String longField;
    private String floatField;
    private String doubleField;
    private String stringField;
    private String booleanField;
    private String timestampField;
    private String nullIntField;

    public RecordSpark22000() { }

    public String getShortField() {
      return shortField;
    }

    public void setShortField(String shortField) {
      this.shortField = shortField;
    }

    public String getIntField() {
      return intField;
    }

    public void setIntField(String intField) {
      this.intField = intField;
    }

    public String getLongField() {
      return longField;
    }

    public void setLongField(String longField) {
      this.longField = longField;
    }

    public String getFloatField() {
      return floatField;
    }

    public void setFloatField(String floatField) {
      this.floatField = floatField;
    }

    public String getDoubleField() {
      return doubleField;
    }

    public void setDoubleField(String doubleField) {
      this.doubleField = doubleField;
    }

    public String getStringField() {
      return stringField;
    }

    public void setStringField(String stringField) {
      this.stringField = stringField;
    }

    public String getBooleanField() {
      return booleanField;
    }

    public void setBooleanField(String booleanField) {
      this.booleanField = booleanField;
    }

    public String getTimestampField() {
      return timestampField;
    }

    public void setTimestampField(String timestampField) {
      this.timestampField = timestampField;
    }

    public String getNullIntField() {
      return nullIntField;
    }

    public void setNullIntField(String nullIntField) {
      this.nullIntField = nullIntField;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      RecordSpark22000 that = (RecordSpark22000) o;
      return Objects.equals(shortField, that.shortField) &&
              Objects.equals(intField, that.intField) &&
              Objects.equals(longField, that.longField) &&
              Objects.equals(floatField, that.floatField) &&
              Objects.equals(doubleField, that.doubleField) &&
              Objects.equals(stringField, that.stringField) &&
              Objects.equals(booleanField, that.booleanField) &&
              Objects.equals(timestampField, that.timestampField) &&
              Objects.equals(nullIntField, that.nullIntField);
    }

    @Override
    public int hashCode() {
      return Objects.hash(shortField, intField, longField, floatField, doubleField, stringField,
              booleanField, timestampField, nullIntField);
    }

    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
          .append("shortField", shortField)
          .append("intField", intField)
          .append("longField", longField)
          .append("floatField", floatField)
          .append("doubleField", doubleField)
          .append("stringField", stringField)
          .append("booleanField", booleanField)
          .append("timestampField", timestampField)
          .append("nullIntField", nullIntField)
          .toString();
    }
  }

  public static final class RecordSpark22000FailToUpcast {
    private Integer id;

    public RecordSpark22000FailToUpcast() {
    }

    public Integer getId() {
      return id;
    }

    public void setId(Integer id) {
      this.id = id;
    }
  }

  @Test
  public void testBeanWithLocalDateAndInstant() {
    String originConf = spark.conf().get(SQLConf.DATETIME_JAVA8API_ENABLED().key());
    try {
      spark.conf().set(SQLConf.DATETIME_JAVA8API_ENABLED().key(), "true");
      List<Row> inputRows = new ArrayList<>();
      List<LocalDateInstantRecord> expectedRecords = new ArrayList<>();

      for (long idx = 0 ; idx < 5 ; idx++) {
        Row row = createLocalDateInstantRow(idx);
        inputRows.add(row);
        expectedRecords.add(createLocalDateInstantRecord(row));
      }

      Encoder<LocalDateInstantRecord> encoder = Encoders.bean(LocalDateInstantRecord.class);

      StructType schema = new StructType()
        .add("localDateField", DataTypes.DateType)
        .add("instantField", DataTypes.TimestampType);

      Dataset<Row> dataFrame = spark.createDataFrame(inputRows, schema);
      Dataset<LocalDateInstantRecord> dataset = dataFrame.as(encoder);

      List<LocalDateInstantRecord> records = dataset.collectAsList();

      Assert.assertEquals(expectedRecords, records);
    } finally {
        spark.conf().set(SQLConf.DATETIME_JAVA8API_ENABLED().key(), originConf);
    }
  }

  @Test
  public void testSPARK38823NoBeanReuse() {
    List<Item> items = Arrays.asList(
            new Item("a", 1),
            new Item("b", 3),
            new Item("c", 2),
            new Item("a", 7));

    Encoder<Item> encoder = Encoders.bean(Item.class);

    Dataset<Item> ds = spark.createDataFrame(items, Item.class)
            .as(encoder)
            .coalesce(1);

    MapFunction<Item, String> mf = new MapFunction<Item, String>() {
      @Override
      public String call(Item item) throws Exception {
        return item.getK();
      }
    };

    ReduceFunction<Item> rf = new ReduceFunction<Item>() {
      @Override
      public Item call(Item item1, Item item2) throws Exception {
        Assert.assertNotSame(item1, item2);
        return item1.addValue(item2.getV());
      }
    };

    Dataset<Tuple2<String, Item>> finalDs = ds
            .groupByKey(mf, Encoders.STRING())
            .reduceGroups(rf);

    List<Tuple2<String, Item>> expectedRecords = Arrays.asList(
            new Tuple2("a", new Item("a", 8)),
            new Tuple2("b", new Item("b", 3)),
            new Tuple2("c", new Item("c", 2)));

    List<Tuple2<String, Item>> result = finalDs.collectAsList();

    Assert.assertEquals(expectedRecords, result);
  }

  public static class Item implements Serializable {
    private String k;
    private int v;

    public String getK() {
      return k;
    }

    public int getV() {
      return v;
    }

    public void setK(String k) {
      this.k = k;
    }

    public void setV(int v) {
      this.v = v;
    }

    public Item() { }

    public Item(String k, int v) {
      this.k = k;
      this.v = v;
    }

    public Item addValue(int inc) {
      return new Item(k, v + inc);
    }

    public String toString() {
      return "Item(" + k + "," + v + ")";
    }

    public boolean equals(Object o) {
      if (!(o instanceof Item)) {
        return false;
      }
      Item other = (Item) o;
      if (other.getK().equals(k) && other.getV() == v) {
        return true;
      }
      return false;
    }
  }

  public static final class LocalDateInstantRecord {
    private String localDateField;
    private String instantField;

    public LocalDateInstantRecord() { }

    public String getLocalDateField() {
      return localDateField;
    }

    public void setLocalDateField(String localDateField) {
      this.localDateField = localDateField;
    }

    public String getInstantField() {
      return instantField;
    }

    public void setInstantField(String instantField) {
      this.instantField = instantField;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      LocalDateInstantRecord that = (LocalDateInstantRecord) o;
      return Objects.equals(localDateField, that.localDateField) &&
        Objects.equals(instantField, that.instantField);
    }

    @Override
    public int hashCode() {
      return Objects.hash(localDateField, instantField);
    }

    @Override
    public String toString() {
      return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
          .append("localDateField", localDateField)
          .append("instantField", instantField)
          .toString();
    }

  }

  private static Row createLocalDateInstantRow(Long index) {
    Object[] values = new Object[] { LocalDate.ofEpochDay(42), Instant.ofEpochSecond(42) };
    return new GenericRow(values);
  }

  private static LocalDateInstantRecord createLocalDateInstantRecord(Row recordRow) {
    LocalDateInstantRecord record = new LocalDateInstantRecord();
    record.setLocalDateField(String.valueOf(recordRow.getLocalDate(0)));
    Instant instant = recordRow.getInstant(1);
    TimestampFormatter formatter = TimestampFormatter.getFractionFormatter(
      DateTimeUtils.getZoneId(SQLConf.get().sessionLocalTimeZone()));
    record.setInstantField(formatter.format(DateTimeUtils.instantToMicros(instant)));
    return record;
  }
}
