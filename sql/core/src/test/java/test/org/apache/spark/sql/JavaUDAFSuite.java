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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class JavaUDAFSuite {

  private transient SparkSession spark;

  @Before
  public void setUp() {
    spark = SparkSession.builder()
        .master("local[*]")
        .appName("testing")
        .getOrCreate();
  }

  @After
  public void tearDown() {
    spark.stop();
    spark = null;
  }

  @SuppressWarnings("unchecked")
  @Test
  public void udf1Test() {
    spark.range(1, 10).toDF("value").createOrReplaceTempView("df");
    spark.udf().registerJavaUDAF("myDoubleAvg", MyDoubleAvg.class.getName());
    Row result = spark.sql("SELECT myDoubleAvg(value) as my_avg from df").head();
    Assert.assertEquals(105.0, result.getDouble(0), 1.0e-6);
  }

}
