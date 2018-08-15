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

package org.apache.spark.sql.catalog.v2;

/**
 * A standard set of transformations that are passed to data sources during table creation.
 *
 * @see PartitionTransform
 */
public class PartitionTransforms {
  private PartitionTransforms() {
  }

  /**
   * Create a transform for a column with the given name.
   * <p>
   * This transform is used to pass named transforms that are not known to Spark.
   *
   * @param transform a name of the transform to apply to the column
   * @param colName a column name
   * @return an Apply transform for the column
   */
  public static PartitionTransform apply(String transform, String colName) {
    if ("identity".equals(transform)) {
      return identity(colName);
    } else if ("year".equals(transform)) {
      return year(colName);
    } else if ("month".equals(transform)) {
      return month(colName);
    } else if ("date".equals(transform)) {
      return date(colName);
    } else if ("hour".equals(transform)) {
      return hour(colName);
    }

    // unknown transform names are passed to sources with Apply
    return new Apply(transform, colName);
  }

  /**
   * Create an identity transform for a column name.
   *
   * @param colName a column name
   * @return an Identity transform for the column
   */
  public static Identity identity(String colName) {
    return new Identity(colName);
  }

  /**
   * Create a bucket transform with the given number of buckets for the named columns.
   *
   * @param colNames a column name
   * @param numBuckets the number of buckets
   * @return a BucketBy transform for the column
   */
  public static Bucket bucket(int numBuckets, String... colNames) {
    return new Bucket(numBuckets, colNames);
  }

  /**
   * Create a year transform for a column name.
   * <p>
   * The corresponding column should be a timestamp or date column.
   *
   * @param colName a column name
   * @return a Year transform for the column
   */
  public static Year year(String colName) {
    return new Year(colName);
  }

  /**
   * Create a month transform for a column name.
   * <p>
   * The corresponding column should be a timestamp or date column.
   *
   * @param colName a column name
   * @return a Month transform for the column
   */
  public static Month month(String colName) {
    return new Month(colName);
  }

  /**
   * Create a date transform for a column name.
   * <p>
   * The corresponding column should be a timestamp or date column.
   *
   * @param colName a column name
   * @return a Date transform for the column
   */
  public static Date date(String colName) {
    return new Date(colName);
  }

  /**
   * Create a date and hour transform for a column name.
   * <p>
   * The corresponding column should be a timestamp column.
   *
   * @param colName a column name
   * @return a DateAndHour transform for the column
   */
  public static DateAndHour hour(String colName) {
    return new DateAndHour(colName);
  }

  private abstract static class SingleColumnTransform implements PartitionTransform {
    private final String[] colNames;

    private SingleColumnTransform(String colName) {
      this.colNames = new String[] { colName };
    }

    @Override
    public String[] references() {
      return colNames;
    }
  }

  public static final class Identity extends SingleColumnTransform {
    private Identity(String colName) {
      super(colName);
    }

    @Override
    public String name() {
      return "identity";
    }
  }

  public static final class Bucket implements PartitionTransform {
    private final int numBuckets;
    private final String[] colNames;

    private Bucket(int numBuckets, String[] colNames) {
      this.numBuckets = numBuckets;
      this.colNames = colNames;
    }

    @Override
    public String name() {
      return "bucket";
    }

    public int numBuckets() {
      return numBuckets;
    }

    @Override
    public String[] references() {
      return colNames;
    }
  }

  public static final class Year extends SingleColumnTransform {
    private Year(String colName) {
      super(colName);
    }

    @Override
    public String name() {
      return "year";
    }
  }

  public static final class Month extends SingleColumnTransform {
    private Month(String colName) {
      super(colName);
    }

    @Override
    public String name() {
      return "month";
    }
  }

  public static final class Date extends SingleColumnTransform {
    private Date(String colName) {
      super(colName);
    }

    @Override
    public String name() {
      return "date";
    }
  }

  public static final class DateAndHour extends SingleColumnTransform {
    private DateAndHour(String colName) {
      super(colName);
    }

    @Override
    public String name() {
      return "hour";
    }
  }

  public static final class Apply extends SingleColumnTransform {
    private final String transformName;

    private Apply(String transformName, String colName) {
      super(colName);
      this.transformName = transformName;
    }

    @Override
    public String name() {
      return transformName;
    }
  }
}
