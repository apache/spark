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

package org.apache.hadoop.util;


import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Executor;

import junit.framework.TestCase;

public class TestPluginDispatcher extends TestCase {

  /**
   * Ensure that dispatch works in general
   */
  public void testDispatch() {
    AtomicInteger runCount = new AtomicInteger(0);

    List<TestPlugin> plugins = Arrays.asList(
      new TestPlugin[] {
        new TestPlugin(runCount)
      });

    PluginDispatcher<TestPlugin> dispatcher =
      new PluginDispatcher<TestPlugin>(plugins, new SameThreadExecutor());

    dispatcher.dispatchCall(new RunMethodRunner());
    assertEquals(runCount.get(), 1);
  }

  /**
   * Ensure that, if a plugin is faulty during startup, it is removed
   * from the plugin dispatcher.
   */
  public void testRemovalOnStartError() {
    AtomicInteger runCount = new AtomicInteger(0);

    List<TestPlugin> plugins = Arrays.asList(
      new TestPlugin[] {
        new TestPlugin(runCount),
        new FaultyPlugin(runCount),
        new TestPlugin(runCount)
      });

    PluginDispatcher<TestPlugin> dispatcher =
      new PluginDispatcher<TestPlugin>(plugins, new SameThreadExecutor());

    // Before we start the plugins, we can dispatch a call
    // and it goes to all 3 plugins
    dispatcher.dispatchCall(new RunMethodRunner());
    assertEquals(runCount.get(), 3);

    // When we dispatch the start, it should kill the faulty plugin
    runCount.set(0);
    dispatcher.dispatchStart(this);
    dispatcher.dispatchCall(new RunMethodRunner());
    assertEquals(runCount.get(), 2);
  }

  /**
   * SingleArgumentRunnable that just calls plugin.run()
   */
  static class RunMethodRunner implements SingleArgumentRunnable<TestPlugin> {
    public void run(TestPlugin p) {
      p.run();
    }
  }

  /**
   * Plugin which increments a counter when its run method is called.
   */
  public static class TestPlugin implements ServicePlugin {
    final AtomicInteger ai;

    public TestPlugin(AtomicInteger ai) {
      this.ai = ai;
    }
    public void start(Object service) {}
    public void stop() {}
    public void close() {}
    public void run() {
      ai.getAndIncrement();
    }
  }

  /**
   * Plugin which throws a RuntimeException on start.
   */
  public static class FaultyPlugin extends TestPlugin {
    public FaultyPlugin(AtomicInteger ai) {
      super(ai);
    }
    public void start(Object service) {
      throw new RuntimeException("Kaboom!");
    }
  }

  /**
   * Executor which runs Runnables in the same thread that submits them.
   */
  public static class SameThreadExecutor implements Executor {
    public void execute(Runnable r) {
      r.run();
    }
  }
}