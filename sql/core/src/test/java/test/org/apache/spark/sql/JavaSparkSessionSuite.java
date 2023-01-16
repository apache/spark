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

import org.apache.spark.sql.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class JavaSparkSessionSuite {
  private SparkSession spark;

  @After
  public void tearDown() {
    spark.stop();
    spark = null;
  }

  @Test
  public void config() {
    // SPARK-40163: SparkSession.config(Map)
    Map<String, Object> map = new HashMap<String, Object>() {{
      put("string", "");
      put("boolean", true);
      put("double", 0.0);
      put("long", 0L);
    }};

    spark = SparkSession.builder()
      .master("local[*]")
      .appName("testing")
      .config(map)
      .getOrCreate();

    for (Map.Entry<String, Object> e : map.entrySet()) {
      Assert.assertEquals(spark.conf().get(e.getKey()), e.getValue().toString());
    }
  }
}
