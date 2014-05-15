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
package org.apache.spark;

import java.io.Closeable;
import java.io.IOException;

// copy from hadoop
public interface Service extends Closeable {

    /**
     * Service states
     */
    public enum State {
        /**
         * Constructed but not initialized
         */
        Uninitialized(0, "Uninitialized"),

        /**
         * Initialized but not started or stopped
         */
        Initialized(1, "Initialized"),

        /**
         * started and not stopped
         */
        Started(2, "Started"),

        /**
         * stopped. No further state transitions are permitted
         */
        Stopped(3, "Stopped");

        /**
         * An integer value for use in array lookup and JMX interfaces.
         * Although {@link Enum#ordinal()} could do this, explicitly
         * identify the numbers gives more stability guarantees over time.
         */
        private final int value;

        /**
         * A name of the state that can be used in messages
         */
        private final String stateName;

        private State(int value, String name) {
            this.value = value;
            this.stateName = name;
        }

        /**
         * Get the integer value of a state
         *
         * @return the numeric value of the state
         */
        public int getValue() {
            return value;
        }

        /**
         * Get the name of a state
         *
         * @return the state's name
         */
        @Override
        public String toString() {
            return stateName;
        }
    }

    void initialize();

    void start();

    void stop();

    void close() throws IOException;

    SparkConf conf();

    State state();
}
