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

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public abstract class LocalJavaStreamingContext {

    protected transient JavaStreamingContext ssc;

    @BeforeEach
    public void setUp() {
        SparkConf conf = new SparkConf()
            .setMaster("local[2]")
            .setAppName("test")
            .set("spark.streaming.clock", "org.apache.spark.util.ManualClock");
        ssc = new JavaStreamingContext(conf, new Duration(1000));
        ssc.checkpoint("checkpoint");
    }

    @AfterEach
    public void tearDown() {
        ssc.stop();
        ssc = null;
    }
}
