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

package org.apache.spark.network.util;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

public class BossThreadFactorySuite {

    @Test
    public void testBossThreadFactory() throws InterruptedException {
        AtomicBoolean wasCalled = new AtomicBoolean(false);

        Function<Throwable, Void> onUncaughtException = (throwable) -> {
            System.out.println("Uncaught exception handler invoked!");
            wasCalled.set(true);
            return null;
        };

        BossThreadFactory factory = new BossThreadFactory("test", false, onUncaughtException);

        Runnable faultyTask = () -> {
            throw new RuntimeException("Test exception from thread!");
        };

        Thread thread = factory.newThread(faultyTask);
        thread.start();
        thread.join();

        assertTrue(wasCalled.get(), "Test failed: onUncaughtException was not called.");
    }
}