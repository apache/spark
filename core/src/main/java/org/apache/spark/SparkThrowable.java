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

import java.io.IOException;
import java.net.URL;
import java.util.SortedMap;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;

import org.apache.spark.util.Utils;

/**
 * Interface mixed into Throwables thrown from Spark.
 *
 * - For backwards compatibility, existing throwable types can be thrown with an arbitrary error
 *   message with no error class. See [[SparkException]].
 * - To promote standardization, throwables should be thrown with an error class and message
 *   parameters to construct an error message with SparkThrowableHelper.getMessage(). New throwable
 *   types should not accept arbitrary error messages. See [[SparkArithmeticException]].
 */
public interface SparkThrowable {
    String getErrorClass();
    String[] getMessageParameters();
    String getSqlState();

    class SparkThrowableHelper {
        protected final static URL errorClassesUrl =
                Utils.getSparkClassLoader().getResource("error/error-classes.json");

        protected final static SortedMap<String, ErrorInfo> errorClassToInfoMap = getErrorClassToInfoMap();

        public static String getMessage(String errorClass, String[] messageParameters) {
            if (errorClass != null && errorClassToInfoMap.containsKey(errorClass)) {
                ErrorInfo errorInfo = errorClassToInfoMap.get(errorClass);
                return String.format(errorInfo.messageFormat, (Object[]) messageParameters);
            }
            throw new IllegalArgumentException(String.format("Cannot find error class '%s'", errorClass));
        }

        public static String getSqlState(String errorClass) {
            if (errorClass != null && errorClassToInfoMap.containsKey(errorClass)) {
                return errorClassToInfoMap.get(errorClass).sqlState;
            }
            return null;
        }

        static SortedMap<String, ErrorInfo> getErrorClassToInfoMap() {
            try {
                ObjectMapper mapper = new ObjectMapper();
                return mapper.readValue(
                        errorClassesUrl, new TypeReference<SortedMap<String, ErrorInfo>>(){});
            } catch (IOException e) {
                throw new IllegalArgumentException(
                        String.format("Cannot load error classes from '%s'", errorClassesUrl), e);
            }
        }
    }
}

/**
 * Information associated with an error class.
 */
class ErrorInfo {
    // SQLSTATE associated with this class
    @JsonProperty String[] message;
    // C-style message format compatible with printf
    @JsonProperty String sqlState;
    // Error message constructed by concatenating the lines with newlines
    String messageFormat;

    @JsonCreator
    public ErrorInfo(
            @JsonProperty("message") String[] message,
            @JsonProperty("sqlState") String sqlState) {
        this.message = message;
        this.sqlState = sqlState;
        this.messageFormat = StringUtils.join(message, "\n");
    }
}
