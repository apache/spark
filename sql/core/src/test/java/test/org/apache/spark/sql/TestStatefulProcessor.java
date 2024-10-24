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

import java.util.*;

import scala.jdk.javaapi.CollectionConverters;

import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.streaming.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A test stateful processor used with transformWithState arbitrary stateful operator in
 * Structured Streaming. The processor primarily aims to test various functionality of the Java API
 * based on various composite types.
 */
public class TestStatefulProcessor extends StatefulProcessor<Integer, String, String> {

  private transient ValueState<Long> countState;
  private transient MapState<String, Long> keyCountMap;
  private transient ListState<String> keysList;

  @Override
  public void init(
      OutputMode outputMode,
      TimeMode timeMode) {
    countState = this.getHandle().getValueState("countState",
      Encoders.LONG());

    keyCountMap = this.getHandle().getMapState("keyCountMap",
      Encoders.STRING(), Encoders.LONG());

    keysList = this.getHandle().getListState("keyList",
      Encoders.STRING());
  }

  @Override
  public scala.collection.Iterator<String> handleInputRows(
      Integer key,
      scala.collection.Iterator<String> rows,
      TimerValues timerValues) {

    java.util.List<String> result = new ArrayList<>();
    long count = 0;
    // Perform various operations on composite types to verify compatibility for the Java API
    if (countState.exists()) {
      count = countState.get();
    }

    long numRows = 0;
    StringBuilder sb = new StringBuilder(key.toString());
    while (rows.hasNext()) {
      numRows++;
      String value = rows.next();
      if (keyCountMap.containsKey(value)) {
        keyCountMap.updateValue(value, keyCountMap.getValue(value) + 1);
      } else {
        keyCountMap.updateValue(value, 1L);
      }
      assertTrue(keyCountMap.containsKey(value));
      keysList.appendValue(value);
      sb.append(value);
    }

    scala.collection.Iterator<String> keys = keysList.get();
    while (keys.hasNext()) {
      String keyVal = keys.next();
      assertTrue(keyCountMap.containsKey(keyVal));
      assertTrue(keyCountMap.getValue(keyVal) > 0);
    }

    count += numRows;
    countState.update(count);
    assertEquals(count, (long) countState.get());

    result.add(sb.toString());
    return CollectionConverters.asScala(result).iterator();
  }
}
