
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
package org.apache.hadoop.mapred.gridmix;

import java.util.Random;

import org.junit.Test;
import static org.junit.Assert.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;

public class TestRecordFactory {
  private static final Log LOG = LogFactory.getLog(TestRecordFactory.class);

  public static void testFactory(long targetBytes, long targetRecs)
      throws Exception {
    final Configuration conf = new Configuration();
    final GridmixKey key = new GridmixKey();
    final GridmixRecord val = new GridmixRecord();
    LOG.info("Target bytes/records: " + targetBytes + "/" + targetRecs);
    final RecordFactory f = new AvgRecordFactory(targetBytes, targetRecs, conf);
    targetRecs = targetRecs <= 0 && targetBytes >= 0
      ? Math.max(1,
          targetBytes / conf.getInt("gridmix.missing.rec.size", 64 * 1024))
      : targetRecs;

    long records = 0L;
    final DataOutputBuffer out = new DataOutputBuffer();
    while (f.next(key, val)) {
      ++records;
      key.write(out);
      val.write(out);
    }
    assertEquals(targetRecs, records);
    assertEquals(targetBytes, out.getLength());
  }

  @Test
  public void testRandom() throws Exception {
    final Random r = new Random();
    final long targetBytes = r.nextInt(1 << 20) + 3 * (1 << 14);
    final long targetRecs = r.nextInt(1 << 14);
    testFactory(targetBytes, targetRecs);
  }

  @Test
  public void testAvg() throws Exception {
    final Random r = new Random();
    final long avgsize = r.nextInt(1 << 10) + 1;
    final long targetRecs = r.nextInt(1 << 14);
    testFactory(targetRecs * avgsize, targetRecs);
  }

  @Test
  public void testZero() throws Exception {
    final Random r = new Random();
    final long targetBytes = r.nextInt(1 << 20);
    testFactory(targetBytes, 0);
  }
}
