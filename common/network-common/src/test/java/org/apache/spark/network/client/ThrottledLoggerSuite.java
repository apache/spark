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

package org.apache.spark.network.client;

import java.util.concurrent.atomic.AtomicInteger;
import java.lang.Thread;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Test;
import org.junit.Before;
import org.apache.log4j.Logger;

public class ThrottledLoggerSuite {
    private ThrottledLogger tlogger;

    @Before
    public void setup() {
        tlogger = new ThrottledLogger(ThrottledLogger.class.getName());
        ThrottledLoggerAppender appender = new ThrottledLoggerAppender();
        Logger logger = Logger.getLogger(ThrottledLogger.class.getName());
        logger.addAppender(appender);
    }

    @Test
    public void test1() {
        ThrottledLoggerAppender appender = (ThrottledLoggerAppender) Logger.getLogger(ThrottledLogger.class.getName()).getAppender("ThrottledLoggerAppender");
        appender.resetCounter();
        tlogger.registerPrefix("msg1", 2);
        tlogger.registerPrefix("msg2", 2);
        try {
            tlogger.info("msg1"); // print
            tlogger.info("msg2"); // print
            tlogger.info("msg1");
        } catch (Exception e) {
            System.out.println(e);
        }

        assert (appender.getLogCount() == 2);
    }

    @Test
    public void test2() {
        ThrottledLoggerAppender appender = (ThrottledLoggerAppender) Logger.getLogger(ThrottledLogger.class.getName()).getAppender("ThrottledLoggerAppender");
        appender.resetCounter();
        tlogger.registerPrefix("msg1", 2);
        try {
            tlogger.info("msg1"); // print
            tlogger.info("msg1");
            tlogger.info("msg1");
        } catch (Exception e) {
            System.out.println(e);
        }

        assert (appender.getLogCount() == 1);
    }

    @Test
    public void test3() {
        ThrottledLoggerAppender appender = (ThrottledLoggerAppender) Logger.getLogger(ThrottledLogger.class.getName()).getAppender("ThrottledLoggerAppender");
        appender.resetCounter();
        tlogger.registerPrefix("msg1", 2);
        try {
            tlogger.info("msg1");  // print
            tlogger.info("msg1");
            tlogger.info("msg1");
            Thread.sleep(2000); // This value depends on the value of `ThrottlingSeconds`.
            tlogger.info("msg1");  // print
        } catch (Exception e) {
            System.out.println(e);
        }

        assert (appender.getLogCount() == 2);
        assert (appender.getLastString().equals("msg1 [3 occurrences]"));
    }


    @Test
    public void test4() {
        ThrottledLoggerAppender appender = (ThrottledLoggerAppender) Logger.getLogger(ThrottledLogger.class.getName()).getAppender("ThrottledLoggerAppender");
        appender.resetCounter();
        tlogger.registerPrefix("msg1", 2);
        try {
            tlogger.info("msg1"); // print
            Thread.sleep(1000);
            tlogger.info("msg1");
        } catch (Exception e) {
            System.out.println(e);
        }

        assert (appender.getLogCount() == 1);
        assert (appender.getLastString().equals("msg1 [1 occurrences]"));

        tlogger.info("msg2");
        tlogger.info("msg2");
        tlogger.info("msg2");
        assert (appender.getLogCount() == 4);
        assert (appender.getLastString().equals("msg2 [1 occurrences]"));
    }

    @Test
    public void test5() {
        ThrottledLoggerAppender appender = (ThrottledLoggerAppender) Logger.getLogger(ThrottledLogger.class.getName()).getAppender("ThrottledLoggerAppender");
        appender.resetCounter();
        tlogger.registerPrefix("msg1", 1);
        try {
            tlogger.info("msg1"); // print
            Thread.sleep(1000);
            tlogger.info("msg1");
        } catch (Exception e) {
            System.out.println(e);
        }

        assert (appender.getLogCount() == 2);
        assert (appender.getLastString().equals("msg1 [1 occurrences]"));
    }

    @Test
    public void test6() {
        ThrottledLoggerAppender appender = (ThrottledLoggerAppender) Logger.getLogger(ThrottledLogger.class.getName()).getAppender("ThrottledLoggerAppender");
        appender.resetCounter();
        tlogger.registerPrefix("msg1", 1);
        try {
            tlogger.info("msg1"); // print
            Thread.sleep(1000);
            tlogger.info("msg1");
            tlogger.registerPrefix("msg2", 2);
            tlogger.info("msg2");
            Thread.sleep(1000);
            tlogger.info("msg2");
        } catch (Exception e) {
            System.out.println(e);
        }

        assert (appender.getLogCount() == 3);
        assert (appender.getLastString().equals("msg2 [1 occurrences]"));
    }

    @Test
    public void test7() {
        ThrottledLoggerAppender appender = (ThrottledLoggerAppender) Logger.getLogger(ThrottledLogger.class.getName()).getAppender("ThrottledLoggerAppender");
        appender.resetCounter();
        tlogger.registerPrefix("msg", 2);
        tlogger.info("msg1"); // print
        tlogger.info("msg2");
        tlogger.info("msg3");

        assert (appender.getLogCount() == 1);
        assert (appender.getLastString().equals("msg1 [1 occurrences]"));
    }

    @Test
    public void test8() {
        ThrottledLoggerAppender appender = (ThrottledLoggerAppender) Logger.getLogger(ThrottledLogger.class.getName()).getAppender("ThrottledLoggerAppender");
        appender.resetCounter();
        tlogger.registerPrefix("msg", 2);

        try {
            tlogger.info("msg1"); // print
            tlogger.info("msg2");
            tlogger.info("msg3");
            Thread.sleep(2000);
            tlogger.info("msg4"); // print
        } catch (Exception e) {
            System.out.println(e);
        }

        assert (appender.getLogCount() == 2);
        assert (appender.getLastString().equals("msg4 [3 occurrences]"));
        // It does not mean that the number of "msg4" occurrences is 3, but the messages with prefix "msg" is 3.

        tlogger.info("abc");
        tlogger.info("abc");

        assert (appender.getLogCount() == 4);
        assert (appender.getLastString().equals("abc [1 occurrences]"));
    }

    @Test
    public void test9() {
        ThrottledLoggerAppender appender = (ThrottledLoggerAppender) Logger.getLogger(ThrottledLogger.class.getName()).getAppender("ThrottledLoggerAppender");
        appender.resetCounter();
        tlogger.registerPrefix("msg1");
        try {
            tlogger.info("msg1"); // print
            Thread.sleep(2000);
            tlogger.info("msg1");
        } catch (Exception e) {
            System.out.println(e);
        }

        assert (appender.getLogCount() == 2);
        assert (appender.getLastString().equals("msg1 [1 occurrences]"));
    }

    @Test
    public void test10() {
        ThrottledLoggerAppender appender = (ThrottledLoggerAppender) Logger.getLogger(ThrottledLogger.class.getName()).getAppender("ThrottledLoggerAppender");
        appender.resetCounter();
        tlogger.registerPrefix("msg", 2);
        try {
            tlogger.info("msg1"); // print
            tlogger.info("msg2");
            tlogger.info("abc");  // print => Does not match with registered prefixes.
            Thread.sleep(2000);
            tlogger.info("msg3"); // print => The RateLimiter of the prefix "msg" has enough permits.
        } catch (Exception e) {
            System.out.println(e);
        }

        assert (appender.getLogCount() == 3);
        assert (appender.getLastString().equals("msg3 [2 occurrences]"));
    }

    @Test
    public void test11() {
        assert (tlogger.getName().equals("org.apache.spark.network.client.ThrottledLogger"));
    }

}

class ThrottledLoggerAppender extends AppenderSkeleton {
    private AtomicInteger logCount = new AtomicInteger(0);
    private String lastString;

    public ThrottledLoggerAppender() {
        setName("ThrottledLoggerAppender");
    }

    @Override
    synchronized protected void append(LoggingEvent event) {
        logCount.getAndAdd(1);
        lastString = event.getRenderedMessage();
    }

    @Override
    public void close() {
    }

    @Override
    public boolean requiresLayout() {
        return false;
    }

    public void resetCounter() {
        logCount.set(0);
    }

    public int getLogCount() {
        return logCount.get();
    }

    public String getLastString() {
        return lastString;
    }
}
