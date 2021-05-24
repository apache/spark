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
package org.apache.spark.streaming.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;

import java.io.Serializable;
import java.util.Date;

/**
 * A java wrapper for exposing [[InitialPositionInStream]]
 * to the corresponding Kinesis readers.
 */
interface KinesisInitialPosition {
    InitialPositionInStream getPosition();
}

public class KinesisInitialPositions {
    public static class Latest implements KinesisInitialPosition, Serializable {
        public Latest() {}

        @Override
        public InitialPositionInStream getPosition() {
            return InitialPositionInStream.LATEST;
        }
    }

    public static class TrimHorizon implements KinesisInitialPosition, Serializable {
        public TrimHorizon() {}

        @Override
        public InitialPositionInStream getPosition() {
            return InitialPositionInStream.TRIM_HORIZON;
        }
    }

    public static class AtTimestamp implements KinesisInitialPosition, Serializable {
        private Date timestamp;

        public AtTimestamp(Date timestamp) {
            this.timestamp = timestamp;
        }

        @Override
        public InitialPositionInStream getPosition() {
            return InitialPositionInStream.AT_TIMESTAMP;
        }

        public Date getTimestamp() {
            return timestamp;
        }
    }


    /**
     * Returns instance of [[KinesisInitialPosition]] based on the passed
     * [[InitialPositionInStream]]. This method is used in KinesisUtils for translating the
     * InitialPositionInStream to InitialPosition. This function would be removed when we deprecate
     * the KinesisUtils.
     *
     * @return [[InitialPosition]]
     */
    public static KinesisInitialPosition fromKinesisInitialPosition(
            InitialPositionInStream initialPositionInStream) throws UnsupportedOperationException {
        if (initialPositionInStream == InitialPositionInStream.LATEST) {
            return new Latest();
        } else if (initialPositionInStream == InitialPositionInStream.TRIM_HORIZON) {
            return new TrimHorizon();
        } else {
            // InitialPositionInStream.AT_TIMESTAMP is not supported.
            // Use InitialPosition.atTimestamp(timestamp) instead.
            throw new UnsupportedOperationException(
                    "Only InitialPositionInStream.LATEST and InitialPositionInStream." +
                            "TRIM_HORIZON supported in initialPositionInStream(). Please use " +
                            "the initialPosition() from builder API in KinesisInputDStream for " +
                            "using InitialPositionInStream.AT_TIMESTAMP");
        }
    }
}
