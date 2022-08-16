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
import org.junit.Test;

public class JavaSparkSessionSuite {

  @Test
  public void conf() {
    SparkSession session = SparkSession.builder()
      .master("local")
      .conf("string", "")
      .conf("boolean", true)
      .conf("double", 0.0)
      .conf("long", 0L)
      .getOrCreate();


    assert(session.conf().get("string").equals(""));
    assert(session.conf().get("boolean").equals("true"));
    assert(session.conf().get("double").equals("0.0"));
    assert(session.conf().get("long").equals("0"));
  }
}
