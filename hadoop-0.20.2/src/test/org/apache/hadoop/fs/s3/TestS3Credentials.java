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
package org.apache.hadoop.fs.s3;

import java.net.URI;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;

public class TestS3Credentials extends TestCase {
  public void testInvalidHostnameWithUnderscores() throws Exception {
    S3Credentials s3Credentials = new S3Credentials();
    try {
      s3Credentials.initialize(new URI("s3://a:b@c_d"), new Configuration());
      fail("Should throw IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertEquals("Invalid hostname in URI s3://a:b@c_d", e.getMessage());
    }
  }
}
