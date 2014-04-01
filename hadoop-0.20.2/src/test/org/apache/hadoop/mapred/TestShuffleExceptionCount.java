/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.mapred.TaskTracker.ShuffleServerMetrics;
import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.spi.OutputRecord;
import org.junit.Test;

public class TestShuffleExceptionCount {

  public static class TestMapOutputServlet extends TaskTracker.MapOutputServlet {

    public void checkException(IOException ie, String exceptionMsgRegex,
        String exceptionStackRegex, ShuffleServerMetrics shuffleMetrics) {
      super.checkException(ie, exceptionMsgRegex, exceptionStackRegex,
          shuffleMetrics);
    }

  }

  @Test
  public void testCheckException() throws IOException, InterruptedException,
      ClassNotFoundException, InstantiationException, IllegalAccessException {
    TestMapOutputServlet testServlet = new TestMapOutputServlet();
    JobConf conf = new JobConf();
    conf.setUser("testuser");
    conf.setJobName("testJob");
    conf.setSessionId("testSession");

    // setup metrics context factory
    ContextFactory factory = ContextFactory.getFactory();
    factory.setAttribute("mapred.class",
        "org.apache.hadoop.metrics.spi.NoEmitMetricsContext");

    TaskTracker tt = new TaskTracker();
    tt.setConf(conf);
    ShuffleServerMetrics shuffleMetrics = tt.new ShuffleServerMetrics(conf);

    // first test with only MsgRegex set but doesn't match
    String exceptionMsgRegex = "Broken pipe";
    String exceptionStackRegex = null;
    IOException ie = new IOException("EOFException");
    testServlet.checkException(ie, exceptionMsgRegex, exceptionStackRegex,
        shuffleMetrics);

    MetricsContext context = factory.getContext("mapred");
    shuffleMetrics.doUpdates(context);
    Map<String, Collection<OutputRecord>> records = context.getAllRecords();
    Collection<OutputRecord> col = records.get("shuffleOutput");
    OutputRecord outputRecord = col.iterator().next();
    assertEquals(0, outputRecord.getMetric("shuffle_exceptions_caught")
        .intValue());

    // test with only MsgRegex set that does match
    ie = new IOException("Broken pipe");
    testServlet.checkException(ie, exceptionMsgRegex, exceptionStackRegex,
        shuffleMetrics);

    shuffleMetrics.doUpdates(context);
    assertEquals(1, outputRecord.getMetric("shuffle_exceptions_caught")
        .intValue());

    // test with neither set, make sure incremented
    exceptionStackRegex = null;
    exceptionMsgRegex = null;
    testServlet.checkException(ie, exceptionMsgRegex, exceptionStackRegex,
        shuffleMetrics);
    shuffleMetrics.doUpdates(context);
    assertEquals(2, outputRecord.getMetric("shuffle_exceptions_caught")
        .intValue());

    // test with only StackRegex set doesn't match
    exceptionStackRegex = ".*\\.doesnt\\$SelectSet\\.wakeup.*";
    exceptionMsgRegex = null;
    ie.setStackTrace(constructStackTrace());
    testServlet.checkException(ie, exceptionMsgRegex, exceptionStackRegex,
        shuffleMetrics);
    shuffleMetrics.doUpdates(context);
    assertEquals(2, outputRecord.getMetric("shuffle_exceptions_caught")
        .intValue());

    // test with only StackRegex set does match
    exceptionStackRegex = ".*\\.SelectorManager\\$SelectSet\\.wakeup.*";
    testServlet.checkException(ie, exceptionMsgRegex, exceptionStackRegex,
        shuffleMetrics);
    shuffleMetrics.doUpdates(context);
    assertEquals(3, outputRecord.getMetric("shuffle_exceptions_caught")
        .intValue());

    // test with both regex set and matches
    exceptionMsgRegex = "Broken pipe";
    ie.setStackTrace(constructStackTraceTwo());
    testServlet.checkException(ie, exceptionMsgRegex, exceptionStackRegex,
        shuffleMetrics);
    shuffleMetrics.doUpdates(context);
    assertEquals(4, outputRecord.getMetric("shuffle_exceptions_caught")
        .intValue());

    // test with both regex set and only msg matches
    exceptionStackRegex = ".*[1-9]+BOGUSREGEX";
    testServlet.checkException(ie, exceptionMsgRegex, exceptionStackRegex,
        shuffleMetrics);
    shuffleMetrics.doUpdates(context);
    assertEquals(4, outputRecord.getMetric("shuffle_exceptions_caught")
        .intValue());

    // test with both regex set and only stack matches
    exceptionStackRegex = ".*\\.SelectorManager\\$SelectSet\\.wakeup.*";
    exceptionMsgRegex = "EOFException";
    testServlet.checkException(ie, exceptionMsgRegex, exceptionStackRegex,
        shuffleMetrics);
    shuffleMetrics.doUpdates(context);
    assertEquals(4, outputRecord.getMetric("shuffle_exceptions_caught")
        .intValue());
  }

  /*
   * Construction exception like: java.io.IOException: Broken pipe at
   * sun.nio.ch.EPollArrayWrapper.interrupt(Native Method) at
   * sun.nio.ch.EPollArrayWrapper.interrupt(EPollArrayWrapper.java:256) at
   * sun.nio.ch.EPollSelectorImpl.wakeup(EPollSelectorImpl.java:175) at
   * org.mortbay
   * .io.nio.SelectorManager$SelectSet.wakeup(SelectorManager.java:831) at
   * org.mortbay
   * .io.nio.SelectorManager$SelectSet.doSelect(SelectorManager.java:709) at
   * org.mortbay.io.nio.SelectorManager.doSelect(SelectorManager.java:192) at
   * org
   * .mortbay.jetty.nio.SelectChannelConnector.accept(SelectChannelConnector.java
   * :124) at
   * org.mortbay.jetty.AbstractConnector$Acceptor.run(AbstractConnector.
   * java:708) at
   * org.mortbay.thread.QueuedThreadPool$PoolThread.run(QueuedThreadPool
   * .java:582)
   */
  private StackTraceElement[] constructStackTrace() {
    StackTraceElement[] stack = new StackTraceElement[9];
    stack[0] = new StackTraceElement("sun.nio.ch.EPollArrayWrapper",
        "interrupt", "", -2);
    stack[1] = new StackTraceElement("sun.nio.ch.EPollArrayWrapper",
        "interrupt", "EPollArrayWrapper.java", 256);
    stack[2] = new StackTraceElement("sun.nio.ch.EPollSelectorImpl", "wakeup",
        "EPollSelectorImpl.java", 175);
    stack[3] = new StackTraceElement(
        "org.mortbay.io.nio.SelectorManager$SelectSet", "wakeup",
        "SelectorManager.java", 831);
    stack[4] = new StackTraceElement(
        "org.mortbay.io.nio.SelectorManager$SelectSet", "doSelect",
        "SelectorManager.java", 709);
    stack[5] = new StackTraceElement("org.mortbay.io.nio.SelectorManager",
        "doSelect", "SelectorManager.java", 192);
    stack[6] = new StackTraceElement(
        "org.mortbay.jetty.nio.SelectChannelConnector", "accept",
        "SelectChannelConnector.java", 124);
    stack[7] = new StackTraceElement(
        "org.mortbay.jetty.AbstractConnector$Acceptor", "run",
        "AbstractConnector.java", 708);
    stack[8] = new StackTraceElement(
        "org.mortbay.thread.QueuedThreadPool$PoolThread", "run",
        "QueuedThreadPool.java", 582);

    return stack;
  }

  /*
   * java.io.IOException: Broken pipe at
   * sun.nio.ch.EPollArrayWrapper.interrupt(Native Method) at
   * sun.nio.ch.EPollArrayWrapper.interrupt(EPollArrayWrapper.java:256) at
   * sun.nio.ch.EPollSelectorImpl.wakeup(EPollSelectorImpl.java:175) at
   * org.mortbay
   * .io.nio.SelectorManager$SelectSet.wakeup(SelectorManager.java:831) at
   * org.mortbay
   * .io.nio.SelectChannelEndPoint.updateKey(SelectChannelEndPoint.java:335) at
   * org
   * .mortbay.io.nio.SelectChannelEndPoint.blockWritable(SelectChannelEndPoint
   * .java:278) at
   * org.mortbay.jetty.AbstractGenerator$Output.blockForOutput(AbstractGenerator
   * .java:545) at
   * org.mortbay.jetty.AbstractGenerator$Output.flush(AbstractGenerator
   * .java:572) at
   * org.mortbay.jetty.HttpConnection$Output.flush(HttpConnection.java:1012) at
   * org
   * .mortbay.jetty.AbstractGenerator$Output.write(AbstractGenerator.java:651)at
   * org
   * .mortbay.jetty.AbstractGenerator$Output.write(AbstractGenerator.java:580)
   * at
   */
  private StackTraceElement[] constructStackTraceTwo() {
    StackTraceElement[] stack = new StackTraceElement[11];
    stack[0] = new StackTraceElement("sun.nio.ch.EPollArrayWrapper",
        "interrupt", "", -2);
    stack[1] = new StackTraceElement("sun.nio.ch.EPollArrayWrapper",
        "interrupt", "EPollArrayWrapper.java", 256);
    stack[2] = new StackTraceElement("sun.nio.ch.EPollSelectorImpl", "wakeup",
        "EPollSelectorImpl.java", 175);
    stack[3] = new StackTraceElement(
        "org.mortbay.io.nio.SelectorManager$SelectSet", "wakeup",
        "SelectorManager.java", 831);
    stack[4] = new StackTraceElement(
        "org.mortbay.io.nio.SelectChannelEndPoint", "updateKey",
        "SelectChannelEndPoint.java", 335);
    stack[5] = new StackTraceElement(
        "org.mortbay.io.nio.SelectChannelEndPoint", "blockWritable",
        "SelectChannelEndPoint.java", 278);
    stack[6] = new StackTraceElement(
        "org.mortbay.jetty.AbstractGenerator$Output", "blockForOutput",
        "AbstractGenerator.java", 545);
    stack[7] = new StackTraceElement(
        "org.mortbay.jetty.AbstractGenerator$Output", "flush",
        "AbstractGenerator.java", 572);
    stack[8] = new StackTraceElement("org.mortbay.jetty.HttpConnection$Output",
        "flush", "HttpConnection.java", 1012);
    stack[9] = new StackTraceElement(
        "org.mortbay.jetty.AbstractGenerator$Output", "write",
        "AbstractGenerator.java", 651);
    stack[10] = new StackTraceElement(
        "org.mortbay.jetty.AbstractGenerator$Output", "write",
        "AbstractGenerator.java", 580);

    return stack;
  }

}
