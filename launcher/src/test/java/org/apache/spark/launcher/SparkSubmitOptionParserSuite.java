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

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import static org.apache.spark.launcher.SparkSubmitOptionParser.*;

public class SparkSubmitOptionParserSuite {

  private SparkSubmitOptionParser parser;

  @Before
  public void setUp() {
    parser = spy(new DummyParser());
  }

  @Test
  public void testAllOptions() {
    List<String> args = Arrays.asList(
      ARCHIVES, ARCHIVES,
      CLASS, CLASS,
      CONF, CONF,
      DEPLOY_MODE, DEPLOY_MODE,
      DRIVER_CLASS_PATH, DRIVER_CLASS_PATH,
      DRIVER_CORES, DRIVER_CORES,
      DRIVER_JAVA_OPTIONS, DRIVER_JAVA_OPTIONS,
      DRIVER_LIBRARY_PATH, DRIVER_LIBRARY_PATH,
      DRIVER_MEMORY, DRIVER_MEMORY,
      EXECUTOR_CORES, EXECUTOR_CORES,
      EXECUTOR_MEMORY, EXECUTOR_MEMORY,
      FILES, FILES,
      JARS, JARS,
      MASTER, MASTER,
      NAME, NAME,
      NUM_EXECUTORS, NUM_EXECUTORS,
      PACKAGES, PACKAGES,
      PROPERTIES_FILE, PROPERTIES_FILE,
      PY_FILES, PY_FILES,
      QUEUE, QUEUE,
      TOTAL_EXECUTOR_CORES, TOTAL_EXECUTOR_CORES,
      REPOSITORIES, REPOSITORIES,
      HELP,
      SUPERVISE,
      VERBOSE);

    parser.parse(args);
    verify(parser).handle(eq(ARCHIVES), eq(ARCHIVES));
    verify(parser).handle(eq(CLASS), eq(CLASS));
    verify(parser).handle(eq(CONF), eq(CONF));
    verify(parser).handle(eq(DEPLOY_MODE), eq(DEPLOY_MODE));
    verify(parser).handle(eq(DRIVER_CLASS_PATH), eq(DRIVER_CLASS_PATH));
    verify(parser).handle(eq(DRIVER_CORES), eq(DRIVER_CORES));
    verify(parser).handle(eq(DRIVER_JAVA_OPTIONS), eq(DRIVER_JAVA_OPTIONS));
    verify(parser).handle(eq(DRIVER_LIBRARY_PATH), eq(DRIVER_LIBRARY_PATH));
    verify(parser).handle(eq(DRIVER_MEMORY), eq(DRIVER_MEMORY));
    verify(parser).handle(eq(EXECUTOR_CORES), eq(EXECUTOR_CORES));
    verify(parser).handle(eq(EXECUTOR_MEMORY), eq(EXECUTOR_MEMORY));
    verify(parser).handle(eq(FILES), eq(FILES));
    verify(parser).handle(eq(JARS), eq(JARS));
    verify(parser).handle(eq(MASTER), eq(MASTER));
    verify(parser).handle(eq(NAME), eq(NAME));
    verify(parser).handle(eq(NUM_EXECUTORS), eq(NUM_EXECUTORS));
    verify(parser).handle(eq(PACKAGES), eq(PACKAGES));
    verify(parser).handle(eq(PROPERTIES_FILE), eq(PROPERTIES_FILE));
    verify(parser).handle(eq(PY_FILES), eq(PY_FILES));
    verify(parser).handle(eq(QUEUE), eq(QUEUE));
    verify(parser).handle(eq(REPOSITORIES), eq(REPOSITORIES));
    verify(parser).handle(eq(TOTAL_EXECUTOR_CORES), eq(TOTAL_EXECUTOR_CORES));
    verify(parser).handle(eq(HELP), same((String) null));
    verify(parser).handle(eq(SUPERVISE), same((String) null));
    verify(parser).handle(eq(VERBOSE), same((String) null));
    verify(parser).handleExtraArgs(eq(Collections.<String>emptyList()));
  }

  @Test
  public void testExtraOptions() {
    List<String> args = Arrays.asList(MASTER, MASTER, "foo", "bar");
    parser.parse(args);
    verify(parser).handle(eq(MASTER), eq(MASTER));
    verify(parser).handleUnknown(eq("foo"));
    verify(parser).handleExtraArgs(eq(Arrays.asList("bar")));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testMissingArg() {
    parser.parse(Arrays.asList(MASTER));
  }

  @Test
  public void testEqualSeparatedOption() {
    List<String> args = Arrays.asList(MASTER + "=" + MASTER);
    parser.parse(args);
    verify(parser).handle(eq(MASTER), eq(MASTER));
    verify(parser).handleExtraArgs(eq(Collections.<String>emptyList()));
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
