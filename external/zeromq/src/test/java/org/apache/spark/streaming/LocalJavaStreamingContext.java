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

package org.apache.spark.streaming;

import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.Before;

public abstract class LocalJavaStreamingContext {

    protected transient JavaStreamingContext ssc;

    @Before
    public void setUp() {
        System.setProperty("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock");
        ssc = new JavaStreamingContext("local[2]", "test", new Duration(1000));
        ssc.checkpoint("checkpoint");
    }

    @After
    public void tearDown() {
        ssc.stop();
        ssc = null;
    }
}
