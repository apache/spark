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

package org.apache.spark.network.shuffledb;

import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Used to identify the version of data stored in local shuffle state DB.
 */
public class StoreVersion {

    public static final byte[] KEY = "StoreVersion".getBytes(StandardCharsets.UTF_8);

    public final int major;
    public final int minor;

    @JsonCreator
    public StoreVersion(@JsonProperty("major") int major, @JsonProperty("minor") int minor) {
        this.major = major;
        this.minor = minor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StoreVersion that = (StoreVersion) o;

        return major == that.major && minor == that.minor;
    }

    @Override
    public int hashCode() {
        int result = major;
        result = 31 * result + minor;
        return result;
    }
}
