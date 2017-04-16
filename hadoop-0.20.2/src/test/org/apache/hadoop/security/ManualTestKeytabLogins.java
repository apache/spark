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

import org.apache.hadoop.security.UserGroupInformation;
import static org.junit.Assert.assertTrue;

/**
 * Regression test for HADOOP-6947 which can be run manually in
 * a kerberos environment.
 *
 * To run this test, set up two keytabs, each with a different principal.
 * Then run something like:
 *  <code>
 *  HADOOP_CLASSPATH=build/test/classes bin/hadoop \
 *     org.apache.hadoop.security.ManualTestKeytabLogins \
 *     usera/test@REALM  /path/to/usera-keytab \
 *     userb/test@REALM  /path/to/userb-keytab
 *  </code>
 */
public class ManualTestKeytabLogins {

  public static void main(String []args) throws Exception {
    if (args.length != 4) {
      System.err.println(
        "usage: ManualTestKeytabLogins <principal 1> <keytab 1> <principal 2> <keytab 2>");
      System.exit(1);
    }

    UserGroupInformation ugi1 =
      UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        args[0], args[1]);
    System.out.println("UGI 1 = " + ugi1);
    assertTrue(ugi1.getUserName().equals(args[0]));
    
    UserGroupInformation ugi2 =
      UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        args[2], args[3]);
    System.out.println("UGI 2 = " + ugi2);
    assertTrue(ugi2.getUserName().equals(args[2]));
  }
}
