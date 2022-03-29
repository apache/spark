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

import java.io.File;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;
import static java.nio.file.attribute.PosixFilePermission.*;

import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.*;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

import static org.apache.spark.launcher.CommandBuilderUtils.*;

public class ChildProcAppHandleSuite extends BaseSuite {

  private static final List<String> MESSAGES = new ArrayList<>();

  private static final List<String> TEST_SCRIPT = Arrays.asList(
    "#!/bin/sh",
    "echo \"output\"",
    "echo \"error\" 1>&2",
    "while [ -n \"$1\" ]; do EC=$1; shift; done",
    "exit $EC");

  private static File TEST_SCRIPT_PATH;

  @AfterClass
  public static void cleanupClass() throws Exception {
    if (TEST_SCRIPT_PATH != null) {
      TEST_SCRIPT_PATH.delete();
      TEST_SCRIPT_PATH = null;
    }
  }

  @BeforeClass
  public static void setupClass() throws Exception {
    TEST_SCRIPT_PATH = File.createTempFile("output-redir-test", ".sh");
    Files.setPosixFilePermissions(TEST_SCRIPT_PATH.toPath(),
      EnumSet.of(OWNER_READ, OWNER_EXECUTE, OWNER_WRITE));
    Files.write(TEST_SCRIPT_PATH.toPath(), TEST_SCRIPT);
  }

  @Before
  public void cleanupLog() {
    MESSAGES.clear();
  }

  @Test
  public void testRedirectsSimple() throws Exception {
    SparkLauncher launcher = new SparkLauncher();
    launcher.redirectError(ProcessBuilder.Redirect.PIPE);
    assertNotNull(launcher.errorStream);
    assertEquals(ProcessBuilder.Redirect.Type.PIPE, launcher.errorStream.type());

    launcher.redirectOutput(ProcessBuilder.Redirect.PIPE);
    assertNotNull(launcher.outputStream);
    assertEquals(ProcessBuilder.Redirect.Type.PIPE, launcher.outputStream.type());
  }

  @Test
  public void testRedirectLastWins() throws Exception {
    SparkLauncher launcher = new SparkLauncher();
    launcher.redirectError(ProcessBuilder.Redirect.PIPE)
      .redirectError(ProcessBuilder.Redirect.INHERIT);
    assertEquals(ProcessBuilder.Redirect.Type.INHERIT, launcher.errorStream.type());

    launcher.redirectOutput(ProcessBuilder.Redirect.PIPE)
      .redirectOutput(ProcessBuilder.Redirect.INHERIT);
    assertEquals(ProcessBuilder.Redirect.Type.INHERIT, launcher.outputStream.type());
  }

  @Test
  public void testRedirectToLog() throws Exception {
    assumeFalse(isWindows());

    SparkAppHandle handle = (ChildProcAppHandle) new TestSparkLauncher()
      .startApplication();
    waitFor(handle);

    assertTrue(MESSAGES.contains("output"));
    assertTrue(MESSAGES.contains("error"));
  }

  @Test
  public void testRedirectErrorToLog() throws Exception {
    assumeFalse(isWindows());

    Path err = Files.createTempFile("stderr", "txt");
    err.toFile().deleteOnExit();

    SparkAppHandle handle = (ChildProcAppHandle) new TestSparkLauncher()
      .redirectError(err.toFile())
      .startApplication();
    waitFor(handle);

    assertTrue(MESSAGES.contains("output"));
    assertEquals(Arrays.asList("error"), Files.lines(err).collect(Collectors.toList()));
  }

  @Test
  public void testRedirectOutputToLog() throws Exception {
    assumeFalse(isWindows());

    Path out = Files.createTempFile("stdout", "txt");
    out.toFile().deleteOnExit();

    SparkAppHandle handle = (ChildProcAppHandle) new TestSparkLauncher()
      .redirectOutput(out.toFile())
      .startApplication();
    waitFor(handle);

    assertTrue(MESSAGES.contains("error"));
    assertEquals(Arrays.asList("output"), Files.lines(out).collect(Collectors.toList()));
  }

  @Test
  public void testNoRedirectToLog() throws Exception {
    assumeFalse(isWindows());

    Path out = Files.createTempFile("stdout", "txt");
    Path err = Files.createTempFile("stderr", "txt");
    out.toFile().deleteOnExit();
    err.toFile().deleteOnExit();

    ChildProcAppHandle handle = (ChildProcAppHandle) new TestSparkLauncher()
      .redirectError(err.toFile())
      .redirectOutput(out.toFile())
      .startApplication();
    waitFor(handle);

    assertTrue(MESSAGES.isEmpty());
    assertEquals(Arrays.asList("error"), Files.lines(err).collect(Collectors.toList()));
    assertEquals(Arrays.asList("output"), Files.lines(out).collect(Collectors.toList()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadLogRedirect() throws Exception {
    File out = Files.createTempFile("stdout", "txt").toFile();
    out.deleteOnExit();
    new SparkLauncher()
      .redirectError()
      .redirectOutput(out)
      .redirectToLog("foo")
      .launch()
      .waitFor();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRedirectErrorTwiceFails() throws Exception {
    File err = Files.createTempFile("stderr", "txt").toFile();
    err.deleteOnExit();
    new SparkLauncher()
      .redirectError()
      .redirectError(err)
      .launch()
      .waitFor();
  }

  @Test
  public void testProcMonitorWithOutputRedirection() throws Exception {
    assumeFalse(isWindows());
    File err = Files.createTempFile("out", "txt").toFile();
    err.deleteOnExit();
    SparkAppHandle handle = new TestSparkLauncher()
      .redirectError()
      .redirectOutput(err)
      .startApplication();
    waitFor(handle);
    assertEquals(SparkAppHandle.State.LOST, handle.getState());
  }

  @Test
  public void testProcMonitorWithLogRedirection() throws Exception {
    assumeFalse(isWindows());
    SparkAppHandle handle = new TestSparkLauncher()
      .redirectToLog(getClass().getName())
      .startApplication();
    waitFor(handle);
    assertEquals(SparkAppHandle.State.LOST, handle.getState());
  }

  @Test
  public void testFailedChildProc() throws Exception {
    assumeFalse(isWindows());
    SparkAppHandle handle = new TestSparkLauncher(1)
      .redirectToLog(getClass().getName())
      .startApplication();
    waitFor(handle);
    assertEquals(SparkAppHandle.State.FAILED, handle.getState());
  }

  private static class TestSparkLauncher extends SparkLauncher {

    TestSparkLauncher() {
      this(0);
    }

    TestSparkLauncher(int ec) {
      setAppResource("outputredirtest");
      addAppArgs(String.valueOf(ec));
    }

    @Override
    String findSparkSubmit() {
      return TEST_SCRIPT_PATH.getAbsolutePath();
    }

  }

  /**
   * A log4j appender used by child apps of this test. It records all messages logged through it in
   * memory so the test can check them.
   */
  @Plugin(name="LogAppender", category="Core", elementType="appender", printObject=true)
  public static class LogAppender extends AbstractAppender {

    protected LogAppender(String name,
                          Filter filter,
                          Layout<? extends Serializable> layout,
                          boolean ignoreExceptions) {
      super(name, filter, layout, ignoreExceptions, Property.EMPTY_ARRAY);
    }

    @Override
    public void append(LogEvent event) {
      MESSAGES.add(event.getMessage().toString());
    }


    @PluginFactory
    public static LogAppender createAppender(
            @PluginAttribute("name") String name,
            @PluginElement("Layout") Layout<? extends Serializable> layout,
            @PluginElement("Filter") final Filter filter,
            @PluginAttribute("otherAttribute") String otherAttribute) {
      return new LogAppender(name, filter, layout, false);
    }
  }
}
