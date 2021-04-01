/*
 * This file is copied from Uber Remote Shuffle Service
(https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.remoteshuffle.metrics;

import com.uber.m3.tally.Gauge;
import com.uber.m3.tally.Scope;
import com.uber.m3.tally.ScopeBuilder;
import com.uber.m3.tally.StatsReporter;
import com.uber.m3.util.Duration;
import org.apache.spark.remoteshuffle.exceptions.RssException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class M3Stats {
  private static final Logger logger = LoggerFactory.getLogger(M3Stats.class);

  public static final String TAG_NAME_SOURCE = "source";
  public static final String TAG_NAME_CLIENT = "client";
  public static final String TAG_NAME_OPERATION = "operation";
  public static final String TAG_NAME_REMOTE = "remote";

  public static final String TAG_NAME_USER = "user";
  public static final String TAG_NAME_QUEUE = "queue";
  public static final String TAG_NAME_ATTEMPT_ID = "attemptId";
  public static final String TAG_NAME_JOB_STATUS = "jobStatus";

  private static final AtomicInteger numM3ScopesAtomicInteger;

  private static final Scope defaultScope;
  private static boolean defaultScopeClosed = false;
  private static Object defaultScopeClosedLock = new Object();

  private static final Gauge numM3Scopes;

  private static final ConcurrentLinkedQueue<StatsReporter> reporters =
      new ConcurrentLinkedQueue<>();

  private static final ExceptionMetricGroupContainer exceptionMetricGroupContainer;

  static {
    numM3ScopesAtomicInteger = new AtomicInteger();

    defaultScope = createScopeHelper();
    numM3Scopes = defaultScope.gauge("numM3Scopes");

    exceptionMetricGroupContainer = new ExceptionMetricGroupContainer();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        closeDefaultScope();
      }
    });
  }

  public static Scope getDefaultScope() {
    return defaultScope;
  }

  public static Scope createSubScope(Map<String, String> tags) {
    Scope scope = defaultScope.tagged(tags);

    int numM3ScopesValue = numM3ScopesAtomicInteger.incrementAndGet();
    numM3Scopes.update(numM3ScopesValue);

    return scope;
  }

  public static void decreaseNumM3Scopes() {
    int numM3ScopesValue = numM3ScopesAtomicInteger.decrementAndGet();
    numM3Scopes.update(numM3ScopesValue);
  }

  public static void addException(Throwable ex, String exceptionSource) {
    exceptionMetricGroupContainer.getMetricGroup(ex, exceptionSource).getNumExceptions().inc(1);
  }

  public static void closeDefaultScope() {
    synchronized (defaultScopeClosedLock) {
      if (defaultScopeClosed) {
        logger.info("M3 scope already closed, do not close again");
        return;
      }

      logger.info("Closing M3 reporters");
      for (StatsReporter reporter : reporters) {
        try {
          reporter.close();
        } catch (Throwable e) {
          logger.warn("Failed to close one M3 reporter", e);
        }
      }
      logger.info("Closed M3 reporters");

      logger.info("Closing M3 scope");
      try {
        defaultScope.close();
      } catch (Throwable e) {
        logger.warn("Failed to close one M3 scope", e);
      }
      logger.info("Closed M3 scope");

      defaultScopeClosed = true;
    }
  }

  private static Scope createScopeHelper() {
    String scopeBuilderClassName = System.getProperty("rss.scopeBuilder");
    if (scopeBuilderClassName == null || scopeBuilderClassName.isEmpty()) {
      scopeBuilderClassName = M3DummyScopeBuilder.class.getName();
    }
    logger.info(String.format("Using scope builder: %s", scopeBuilderClassName));

    ScopeBuilder scopeBuilder;
    try {
      Class<? extends ScopeBuilder> scopeBuilderClass =
          Class.forName(scopeBuilderClassName).asSubclass(ScopeBuilder.class);
      scopeBuilder = scopeBuilderClass.getConstructor().newInstance();
    } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
      throw new RssException(String
          .format("Failed to create ScopeBuilder instance from class name %s",
              scopeBuilderClassName), e);
    }

    Scope scope = scopeBuilder.reportEvery(Duration.ofSeconds(30));
    return scope;
  }
}
