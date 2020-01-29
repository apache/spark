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

package org.apache.spark.sql.connector.read.streaming;

import org.apache.spark.annotation.Evolving;

/**
 * A mix-in interface for {@link MicroBatchStream} streaming sources to signal that they can control
 * the rate of data ingested into the system per micro-batch. These rate limits can come implicitly
 * from the contract of triggers, e.g. Trigger.Once() requires that a micro-batch process all data
 * available to the system at the start of the micro-batch. Alternatively, sources can decide to
 * limit ingest through data source options.
 *
 * Through this interface, a MicroBatchStream should be able to return the next offset that it will
 * process until given a {@link ReadLimit}.
 *
 * @since 3.0.0
 */
@Evolving
public interface SupportsAdmissionControl extends SparkDataStream {

    /**
     * Returns the read limits potentially passed to the data source through options when creating
     * the data source.
     */
    default ReadLimit getDefaultReadLimit() {
        return ReadLimit.allAvailable();
    }

    /**
     * Returns the most recent offset available given a read limit. The start offset can be used
     * to figure out
     */
    Offset latestOffset(Offset startOffset, ReadLimit limit);
}
