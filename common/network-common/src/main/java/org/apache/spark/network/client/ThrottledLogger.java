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

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.slf4j.Logger;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import scala.Tuple2;
import scala.Tuple3;

import org.apache.spark.network.util.MapConfigProvider;
import org.apache.spark.network.util.TransportConf;

public class ThrottledLogger implements Logger {
    private class ThrottlingGroup {
        RateLimiter limiter;
        AtomicLong count;
        public ThrottlingGroup(RateLimiter limiter, AtomicLong count) {
            this.limiter = limiter;
            this.count = count;
        }
    }

    private LoadingCache<String, ThrottlingGroup> rateLimiters = CacheBuilder.newBuilder().maximumSize(1000).build(new CacheLoader<String, ThrottlingGroup>() {
        @Override
        public ThrottlingGroup load(String msg) throws Exception {
            return new ThrottlingGroup(RateLimiter.create(1.0), new AtomicLong());
        }
    });

    private Logger logger;
    private int defaultThrottlingSeconds = (new TransportConf("shuffle", MapConfigProvider.EMPTY)).throttlingDelaySeconds(); // Same messages can only print 1 time per 2 seconds
    private ConcurrentHashMap<String, Integer> prefixMap = new ConcurrentHashMap<>(); // key: prefix, value: throttlingSeconds

    /**
     * Provides throttled logging. For example, one may not want to print out an error from a polling
     * loop every second, but still want to know when the error happens.
     *
     * The usage is the same as org.slf4j.Logger.
     *
     * WARNING: throttling occurs on a per-prefix basis, so users need to register prefixes to limit the
     * rate for messages with the same prefix.
     *
     * For example:
     *
     * ThrottledLogger tlogger = new ThrottledLogger("ThrottledLogger");
     * tlogger.registerPrefix("msg", 2); // Printing a message with the prefix "msg" consumes 2 permits.
     * tlogger.info("msg1"); // print
     * tlogger.info("msg2");
     * tlogger.info("abc");  // print => Does not match with registered prefixes.
     * Thread.sleep(2000);
     * tlogger.info("msg3"); // print => The RateLimiter of the prefix "msg" has enough permits.
     *
     * @param name Variable to create a named logger
     */
    public ThrottledLogger(String name) {
        this.logger = LoggerFactory.getLogger(name);
    }

    /**
     * The logger needs $throttlingSeconds permits to print a message. If the RateLimiter for the prefix
     * does not have enough permits, the function `tryAcquire(throttlingSeconds)` will return false.
     *
     * All messages with the same prefix will share a RateLimiter. In addition, every prefix has its
     * $throttlingSeconds, that is, each message can consume different number of permits.
     *
     * If $msg does not match with any registered prefix, the message will not be limited by RateLimiter.
     * In other words, the message will always be printed.
     *
     * @param msg The message wants to be printed.
     * @return Tuple2<Boolean, Long><isPrinted/count>
     * If $isPrinted is true, $msg will be printed. Otherwise, $msg will not be printed. The return
     * variable $count is the number of messages with same prefix which have not been printed.
     */
    public Tuple2<Boolean, Long> logThrottled(String msg) {
        Tuple3<Boolean, Integer, String> tuple = checkPrefix(msg);
        Boolean registered = tuple._1();
        Integer throttlingSeconds = tuple._2();
        String prefix = tuple._3();

        if (registered) {
            ThrottlingGroup group = rateLimiters.getUnchecked(prefix);
            if (throttlingSeconds <= 0 || group.limiter.tryAcquire(throttlingSeconds)) {
                long count = group.count.getAndSet(0L);
                return new Tuple2<Boolean, Long>(true, count);
            } else {
                long count = group.count.getAndIncrement();
                return new Tuple2<Boolean, Long>(false, count);
            }
        } else {
            return new Tuple2<Boolean, Long>(true, 0L);
        }
    }

    public void registerPrefix(String prefix) {
        registerPrefix(prefix, defaultThrottlingSeconds);
    }

    /**
     * Create a RateLimiter for the $prefix, and put a prefix-throttlingSeconds pair into a HashMap.
     *
     * @param prefix
     * @param throttlingSeconds: Printing a message starting with $prefix consumes $throttlingSeconds permits.
     */
    public void registerPrefix(String prefix, int throttlingSeconds) {
        rateLimiters.getUnchecked(prefix);
        prefixMap.put(prefix, throttlingSeconds);
    }

    /**
     * Check whether the message starting with any registered prefix or not.
     *
     * @param msg
     */
    private Tuple3<Boolean, Integer, String> checkPrefix(String msg) {
        Iterator<ConcurrentHashMap.Entry<String, Integer>> itr = prefixMap.entrySet().iterator();
        while (itr.hasNext()) {
            ConcurrentHashMap.Entry<String, Integer> entry = itr.next();
            String prefix = entry.getKey();
            int throttlingSeconds = entry.getValue();
            if (msg.startsWith(prefix)) {
                return new Tuple3<Boolean, Integer, String>(true, throttlingSeconds, prefix);
            }
        }
        return new Tuple3<Boolean, Integer, String>(false, 0, "");
    }

    @Override
    public String getName() {
        return logger.getName();
    }

    @Override
    public boolean isTraceEnabled() {
        return logger.isTraceEnabled();
    }

    @Override
    public void trace(String s) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.trace(String.format("%s [%d occurrences]", s, tuple._2() + 1));
        }
    }

    @Override
    public void trace(String s, Object o) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.trace(String.format("%s [%d occurrences]", s, tuple._2() + 1), o);
        }
    }

    @Override
    public void trace(String s, Object o, Object o1) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.trace(String.format("%s [%d occurrences]", s, tuple._2() + 1), o, o1);
        }
    }

    @Override
    public void trace(String s, Object... objects) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.trace(String.format("%s [%d occurrences]", s, tuple._2() + 1), objects);
        }
    }

    @Override
    public void trace(String s, Throwable throwable) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.trace(String.format("%s [%d occurrences]", s, tuple._2() + 1), throwable);
        }
    }

    @Override
    public boolean isTraceEnabled(Marker marker) {
        return logger.isTraceEnabled(marker);
    }

    @Override
    public void trace(Marker marker, String s) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.trace(marker, String.format("%s [%d occurrences]", s, tuple._2() + 1));
        }
    }

    @Override
    public void trace(Marker marker, String s, Object o) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.trace(marker, String.format("%s [%d occurrences]", s, tuple._2() + 1), o);
        }
    }

    @Override
    public void trace(Marker marker, String s, Object o, Object o1) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.trace(marker, String.format("%s [%d occurrences]", s, tuple._2() + 1), o, o1);
        }
    }

    @Override
    public void trace(Marker marker, String s, Object... objects) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.trace(marker, String.format("%s [%d occurrences]", s, tuple._2() + 1), objects);
        }
    }

    @Override
    public void trace(Marker marker, String s, Throwable throwable) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.trace(marker, String.format("%s [%d occurrences]", s, tuple._2() + 1), throwable);
        }
    }

    @Override
    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    @Override
    public void debug(String s) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.debug(String.format("%s [%d occurrences]", s, tuple._2() + 1));
        }
    }

    @Override
    public void debug(String s, Object o) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.debug(String.format("%s [%d occurrences]", s, tuple._2() + 1), o);
        }
    }

    @Override
    public void debug(String s, Object o, Object o1) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.debug(String.format("%s [%d occurrences]", s, tuple._2() + 1), o, o1);
        }
    }

    @Override
    public void debug(String s, Object... objects) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.debug(String.format("%s [%d occurrences]", s, tuple._2() + 1), objects);
        }
    }

    @Override
    public void debug(String s, Throwable throwable) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.debug(String.format("%s [%d occurrences]", s, tuple._2() + 1), throwable);
        }
    }

    @Override
    public boolean isDebugEnabled(Marker marker) {
        return logger.isDebugEnabled(marker);
    }

    @Override
    public void debug(Marker marker, String s) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.debug(marker, String.format("%s [%d occurrences]", s, tuple._2() + 1));
        }
    }

    @Override
    public void debug(Marker marker, String s, Object o) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.debug(marker, String.format("%s [%d occurrences]", s, tuple._2() + 1), o);
        }
    }

    @Override
    public void debug(Marker marker, String s, Object o, Object o1) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.debug(marker, String.format("%s [%d occurrences]", s, tuple._2() + 1), o, o1);
        }
    }

    @Override
    public void debug(Marker marker, String s, Object... objects) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.debug(marker, String.format("%s [%d occurrences]", s, tuple._2() + 1), objects);
        }
    }

    @Override
    public void debug(Marker marker, String s, Throwable throwable) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.debug(marker, String.format("%s [%d occurrences]", s, tuple._2() + 1), throwable);
        }
    }

    @Override
    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    @Override
    public void info(String s) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.info(String.format("%s [%d occurrences]", s, tuple._2() + 1));
        }
    }

    @Override
    public void info(String s, Object o) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.info(String.format("%s [%d occurrences]", s, tuple._2() + 1), o);
        }
    }

    @Override
    public void info(String s, Object o, Object o1) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.info(String.format("%s [%d occurrences]", s, tuple._2() + 1), o, o1);
        }
    }

    @Override
    public void info(String s, Object... objects) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.info(String.format("%s [%d occurrences]", s, tuple._2() + 1), objects);
        }
    }

    @Override
    public void info(String s, Throwable throwable) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.info(String.format("%s [%d occurrences]", s, tuple._2() + 1), throwable);
        }
    }

    @Override
    public boolean isInfoEnabled(Marker marker) {
        return logger.isInfoEnabled(marker);
    }

    @Override
    public void info(Marker marker, String s) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.info(marker, String.format("%s [%d occurrences]", s, tuple._2() + 1));
        }
    }

    @Override
    public void info(Marker marker, String s, Object o) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.info(marker, String.format("%s [%d occurrences]", s, tuple._2() + 1), o);
        }
    }

    @Override
    public void info(Marker marker, String s, Object o, Object o1) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.info(marker, String.format("%s [%d occurrences]", s, tuple._2() + 1), o, o1);
        }
    }

    @Override
    public void info(Marker marker, String s, Object... objects) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.info(marker, String.format("%s [%d occurrences]", s, tuple._2() + 1), objects);
        }
    }

    @Override
    public void info(Marker marker, String s, Throwable throwable) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.info(marker, String.format("%s [%d occurrences]", s, tuple._2() + 1), throwable);
        }
    }

    @Override
    public boolean isWarnEnabled() {
        return logger.isWarnEnabled();
    }

    @Override
    public void warn(String s) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.warn(String.format("%s [%d occurrences]", s, tuple._2() + 1));
        }
    }

    @Override
    public void warn(String s, Object o) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.warn(String.format("%s [%d occurrences]", s, tuple._2() + 1), o);
        }
    }

    @Override
    public void warn(String s, Object... objects) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.warn(String.format("%s [%d occurrences]", s, tuple._2() + 1), objects);
        }
    }

    @Override
    public void warn(String s, Object o, Object o1) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.warn(String.format("%s [%d occurrences]", s, tuple._2() + 1), o, o1);
        }
    }

    @Override
    public void warn(String s, Throwable throwable) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.warn(String.format("%s [%d occurrences]", s, tuple._2() + 1), throwable);
        }
    }

    @Override
    public boolean isWarnEnabled(Marker marker) {
        return logger.isWarnEnabled(marker);
    }

    @Override
    public void warn(Marker marker, String s) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.warn(marker, String.format("%s [%d occurrences]", s, tuple._2() + 1));
        }
    }

    @Override
    public void warn(Marker marker, String s, Object o) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.warn(marker, String.format("%s [%d occurrences]", s, tuple._2() + 1), o);
        }
    }

    @Override
    public void warn(Marker marker, String s, Object o, Object o1) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.warn(marker, String.format("%s [%d occurrences]", s, tuple._2() + 1), o, o1);
        }
    }

    @Override
    public void warn(Marker marker, String s, Object... objects) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.warn(marker, String.format("%s [%d occurrences]", s, tuple._2() + 1), objects);
        }
    }

    @Override
    public void warn(Marker marker, String s, Throwable throwable) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.warn(marker, String.format("%s [%d occurrences]", s, tuple._2() + 1), throwable);
        }
    }

    @Override
    public boolean isErrorEnabled() {
        return logger.isErrorEnabled();
    }

    @Override
    public void error(String s) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.error(String.format("%s [%d occurrences]", s, tuple._2() + 1));
        }
    }

    @Override
    public void error(String s, Object o) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.error(String.format("%s [%d occurrences]", s, tuple._2() + 1), o);
        }
    }

    @Override
    public void error(String s, Object o, Object o1) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.error(String.format("%s [%d occurrences]", s, tuple._2() + 1), o, o1);
        }
    }

    @Override
    public void error(String s, Object... objects) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.error(String.format("%s [%d occurrences]", s, tuple._2() + 1), objects);
        }
    }

    @Override
    public void error(String s, Throwable throwable) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.error(String.format("%s [%d occurrences]", s, tuple._2() + 1), throwable);
        }
    }

    @Override
    public boolean isErrorEnabled(Marker marker) {
        return logger.isErrorEnabled(marker);
    }

    @Override
    public void error(Marker marker, String s) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.error(marker, String.format("%s [%d occurrences]", s, tuple._2() + 1));
        }
    }

    @Override
    public void error(Marker marker, String s, Object o) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.error(marker, String.format("%s [%d occurrences]", s, tuple._2() + 1), o);
        }
    }

    @Override
    public void error(Marker marker, String s, Object o, Object o1) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.error(marker, String.format("%s [%d occurrences]", s, tuple._2() + 1), o, o1);
        }
    }

    @Override
    public void error(Marker marker, String s, Object... objects) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.error(marker, String.format("%s [%d occurrences]", s, tuple._2() + 1), objects);
        }
    }

    @Override
    public void error(Marker marker, String s, Throwable throwable) {
        Tuple2<Boolean, Long> tuple = logThrottled(s);
        if (tuple._1()) {
            logger.error(marker, String.format("%s [%d occurrences]", s, tuple._2() + 1), throwable);
        }
    }
}
