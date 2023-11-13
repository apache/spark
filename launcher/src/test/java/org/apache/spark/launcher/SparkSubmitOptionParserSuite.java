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

package org.apache.spark.launcher;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.*;

public class SparkSubmitOptionParserSuite extends BaseSuite {

  private SparkSubmitOptionParser parser;

  @BeforeEach
  public void setUp() {
    parser = spy(new DummyParser());
  }

  @Test
  public void testAllOptions() {
    int count = 0;
    for (String[] optNames : parser.opts) {
      for (String optName : optNames) {
        String value = optName + "-value";
        parser.parse(Arrays.asList(optName, value));
        count++;
        verify(parser).handle(eq(optNames[0]), eq(value));
        verify(parser, times(count)).handle(anyString(), anyString());
        verify(parser, times(count)).handleExtraArgs(eq(Collections.emptyList()));
      }
    }

    int nullCount = 0;
    for (String[] switchNames : parser.switches) {
      int switchCount = 0;
      for (String name : switchNames) {
        parser.parse(Arrays.asList(name));
        count++;
        nullCount++;
        switchCount++;
        verify(parser, times(switchCount)).handle(eq(switchNames[0]), same(null));
        verify(parser, times(nullCount)).handle(anyString(), isNull());
        verify(parser, times(count - nullCount)).handle(anyString(), any(String.class));
        verify(parser, times(count)).handleExtraArgs(eq(Collections.emptyList()));
      }
    }
  }

  @Test
  public void testExtraOptions() {
    List<String> args = Arrays.asList(parser.MASTER, parser.MASTER, "foo", "bar");
    parser.parse(args);
    verify(parser).handle(eq(parser.MASTER), eq(parser.MASTER));
    verify(parser).handleUnknown(eq("foo"));
    verify(parser).handleExtraArgs(eq(Arrays.asList("bar")));
  }

  @Test
  public void testMissingArg() {
    assertThrows(IllegalArgumentException.class,
      () -> parser.parse(Arrays.asList(parser.MASTER)));
  }

  @Test
  public void testEqualSeparatedOption() {
    List<String> args = Arrays.asList(parser.MASTER + "=" + parser.MASTER);
    parser.parse(args);
    verify(parser).handle(eq(parser.MASTER), eq(parser.MASTER));
    verify(parser).handleExtraArgs(eq(Collections.emptyList()));
  }

  private static class DummyParser extends SparkSubmitOptionParser {

    @Override
    protected boolean handle(String opt, String value) {
      return true;
    }

    @Override
    protected boolean handleUnknown(String opt) {
      return false;
    }

    @Override
    protected void handleExtraArgs(List<String> extra) {

    }

  }

}
