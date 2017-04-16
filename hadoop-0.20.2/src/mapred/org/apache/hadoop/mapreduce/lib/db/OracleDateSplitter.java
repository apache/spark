/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.lib.db;

import java.util.Date;

/**
 * Implement DBSplitter over date/time values returned by an Oracle db.
 * Make use of logic from DateSplitter, since this just needs to use
 * some Oracle-specific functions on the formatting end when generating
 * InputSplits.
 */
public class OracleDateSplitter extends DateSplitter {

  @SuppressWarnings("unchecked")
  @Override
  protected String dateToString(Date d) {
    // Oracle Data objects are always actually Timestamps
    return "TO_TIMESTAMP('" + d.toString() + "', 'YYYY-MM-DD HH24:MI:SS.FF')";
  }
}
