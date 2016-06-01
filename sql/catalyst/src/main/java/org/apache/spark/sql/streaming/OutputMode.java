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

package org.apache.spark.sql.streaming;

import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.InternalOutputModes;

/**
 * :: Experimental ::
 *
 * OutputMode is used to what data will be written to a streaming sink when there is
 * new data available in a streaming DataFrame/Dataset.
 *
 * @since 2.0.0
 */
@Experimental
public class OutputMode {

  /**
   * OutputMode in which only the new rows in the streaming DataFrame/Dataset will be
   * written to the sink. This output mode can be only be used in queries that do not
   * contain any aggregation.
   *
   * @since 2.0.0
   */
  public static OutputMode Append() {
    return InternalOutputModes.Append$.MODULE$;
  }

  /**
   * OutputMode in which all the rows in the streaming DataFrame/Dataset will be written
   * to the sink every time these is some updates. This output mode can only be used in queries
   * that contain aggregations.
   *
   * @since 2.0.0
   */
  public static OutputMode Complete() {
    return InternalOutputModes.Complete$.MODULE$;
  }
}
