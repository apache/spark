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

package org.apache.hadoop.security;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestKerberosName {

  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    conf.set("hadoop.security.auth_to_local", 
             ("RULE:[1:$1@$0](.*@YAHOO\\.COM)s/@.*//\n" +
              "RULE:[2:$1](johndoe)s/^.*$/guest/\n" +
              "RULE:[2:$1;$2](^.*;admin$)s/;admin$//\n" +
              "RULE:[2:$2](root)\n" +
              "DEFAULT"));
    conf.set("hadoop.security.authentication", "kerberos");
    KerberosName.setConfiguration(conf);
    KerberosName.printRules();
  }

  private void checkTranslation(String from, String to) throws Exception {
    System.out.println("Translate " + from);
    KerberosName nm = new KerberosName(from);
    String simple = nm.getShortName();
    System.out.println("to " + simple);
    assertEquals("short name incorrect", to, simple);
  }

  @Test
  public void testRules() throws Exception {
    checkTranslation("omalley@APACHE.ORG", "omalley");
    checkTranslation("hdfs/10.0.0.1@APACHE.ORG", "hdfs");
    checkTranslation("oom@YAHOO.COM", "oom");
    checkTranslation("johndoe/zoo@FOO.COM", "guest");
    checkTranslation("joe/admin@FOO.COM", "joe");
    checkTranslation("joe/root@FOO.COM", "root");
  }
  
  private void checkBadName(String name) {
    System.out.println("Checking " + name + " to ensure it is bad.");
    try {
      new KerberosName(name);
      fail("didn't get exception for " + name);
    } catch (IllegalArgumentException iae) {
      // PASS
    }
  }
  
  private void checkBadTranslation(String from) {
    System.out.println("Checking bad translation for " + from);
    KerberosName nm = new KerberosName(from);
    try {
      nm.getShortName();
      fail("didn't get exception for " + from);
    } catch (IOException ie) {
      // PASS
    }
  }
  
  @Test
  public void testAntiPatterns() throws Exception {
    checkBadName("owen/owen/owen@FOO.COM");
    checkBadName("owen@foo/bar.com");
    checkBadTranslation("foo@ACME.COM");
    checkBadTranslation("root/joe@FOO.COM");
  }
}
