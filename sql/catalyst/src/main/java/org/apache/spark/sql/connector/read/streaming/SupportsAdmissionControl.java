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
 * A mix-in interface for {@link SparkDataStream} streaming sources to signal that they can control
 * the rate of data ingested into the system. These rate limits can come implicitly from the
 * contract of triggers, e.g. Trigger.Once() requires that a micro-batch process all data
 * available to the system at the start of the micro-batch. Alternatively, sources can decide to
 * limit ingest through data source options.
 * <p>
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
  default ReadLimit getDefaultReadLimit() { return ReadLimit.allAvailable(); }

  /**
   * Returns the most recent offset available given a read limit. The start offset can be used
   * to figure out how much new data should be read given the limit. Users should implement this
   * method instead of latestOffset for a MicroBatchStream or getOffset for Source.
   * <p>
   * When this method is called on a `Source`, the source can return `null` if there is no
   * data to process. In addition, for the very first micro-batch, the `startOffset` will be
   * null as well.
   * <p>
   * When this method is called on a MicroBatchStream, the `startOffset` will be `initialOffset`
   * for the very first micro-batch. The source can return `null` if there is no data to process.
   */
  Offset latestOffset(Offset startOffset, ReadLimit limit);

  /**
   * Returns the most recent offset available.
   * <p>
   * The source can return `null`, if there is no data to process or the source does not support
   * to this method.
   */
  default Offset reportLatestOffset() { return null; }
}
