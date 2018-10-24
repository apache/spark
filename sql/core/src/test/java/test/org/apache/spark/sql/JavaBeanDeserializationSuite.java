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
import java.util.*;

import org.junit.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.test.TestSparkSession;

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
      new ArrayRecord(1, Arrays.asList(new Interval(111, 211), new Interval(121, 221)))
    );
    ARRAY_RECORDS.add(
      new ArrayRecord(2, Arrays.asList(new Interval(112, 212), new Interval(122, 222)))
    );
    ARRAY_RECORDS.add(
      new ArrayRecord(3, Arrays.asList(new Interval(113, 213), new Interval(123, 223)))
    );
  }

  @Test
  public void testBeanWithArrayFieldDeserialization() {

    Encoder<ArrayRecord> encoder = Encoders.bean(ArrayRecord.class);

    Dataset<ArrayRecord> dataset = spark
      .read()
      .format("json")
      .schema("id int, intervals array<struct<startTime: bigint, endTime: bigint>>")
      .load("src/test/resources/test-data/with-array-fields.json")
      .as(encoder);

    List<ArrayRecord> records = dataset.collectAsList();
    Assert.assertEquals(records, ARRAY_RECORDS);
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

    Assert.assertEquals(records, MAP_RECORDS);
  }

  public static class ArrayRecord {

    private int id;
    private List<Interval> intervals;

    public ArrayRecord() { }

    ArrayRecord(int id, List<Interval> intervals) {
      this.id = id;
      this.intervals = intervals;
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

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof ArrayRecord)) return false;
      ArrayRecord other = (ArrayRecord) obj;
      return (other.id == this.id) && other.intervals.equals(this.intervals);
    }

    @Override
    public String toString() {
      return String.format("{ id: %d, intervals: %s }", id, intervals);
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
}
