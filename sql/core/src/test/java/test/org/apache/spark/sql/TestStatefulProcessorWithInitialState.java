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

/**
 * A test stateful processor concatenates all input rows for a key and emits the result.
 * Primarily used for testing the Java API for arbitrary stateful operator in structured streaming
 * using provided initial state.
 */
public class TestStatefulProcessorWithInitialState
  extends StatefulProcessorWithInitialState<Integer, String, String, String> {

  private transient ValueState<String> testState;

  @Override
  public void init(
      OutputMode outputMode,
      TimeMode timeMode) {
    testState = this.getHandle().getValueState("testState",
      Encoders.STRING());
  }

  @Override
  public void handleInitialState(Integer key, String initialState, TimerValues timerValues) {
    testState.update(initialState);
  }

  @Override
  public scala.collection.Iterator<String> handleInputRows(
      Integer key,
      scala.collection.Iterator<String> rows,
      TimerValues timerValues,
      ExpiredTimerInfo expiredTimerInfo) {

    java.util.List<String> result = new ArrayList<>();
    if (!expiredTimerInfo.isValid()) {
      String existingValue = "";
      if (testState.exists()) {
        existingValue = testState.get();
      }

      StringBuilder sb = new StringBuilder(key.toString());
      if (!existingValue.isEmpty()) {
        sb.append(existingValue);
      }

      while (rows.hasNext()) {
        sb.append(rows.next());
      }

      testState.clear();
      assert(testState.exists() == false);

      result.add(sb.toString());
    }
    return CollectionConverters.asScala(result).iterator();
  }
}
