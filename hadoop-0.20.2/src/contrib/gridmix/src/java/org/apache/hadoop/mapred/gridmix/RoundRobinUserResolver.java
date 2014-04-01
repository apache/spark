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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class RoundRobinUserResolver implements UserResolver {
  public static final Log LOG = LogFactory.getLog(RoundRobinUserResolver.class);

  private int uidx = 0;
  private List<UserGroupInformation> users = Collections.emptyList();
  private final HashMap<UserGroupInformation,UserGroupInformation> usercache =
    new HashMap<UserGroupInformation,UserGroupInformation>();
  
  /**
   * Userlist assumes one UGI per line, each UGI matching
   * &lt;username&gt;,&lt;group&gt;[,group]*
   */
  private List<UserGroupInformation> parseUserList(
      URI userUri, Configuration conf) throws IOException {
    if (null == userUri) {
      return Collections.emptyList();
    }
    
    final Path userloc = new Path(userUri.toString());
    final Text rawUgi = new Text();
    final FileSystem fs = userloc.getFileSystem(conf);
    final ArrayList<UserGroupInformation> ret = new ArrayList();

    LineReader in = null;
    try {
      final ArrayList<String> groups = new ArrayList();
      in = new LineReader(fs.open(userloc));
      while (in.readLine(rawUgi) > 0) {
        int e = rawUgi.find(",");
        if (e <= 0) {
          throw new IOException("Missing username: " + rawUgi);
        }
        final String username = Text.decode(rawUgi.getBytes(), 0, e);
        int s = e;
        while ((e = rawUgi.find(",", ++s)) != -1) {
          groups.add(Text.decode(rawUgi.getBytes(), s, e - s));
          s = e;
        }
        groups.add(Text.decode(rawUgi.getBytes(), s, rawUgi.getLength() - s));
        if (groups.size() == 0) {
          throw new IOException("Missing groups: " + rawUgi);
        }
        ret.add(UserGroupInformation.createRemoteUser(username));
      }
    } finally {
      if (in != null) {
        in.close();
      }
    }
    return ret;
  }

  @Override
  public synchronized boolean setTargetUsers(URI userloc, Configuration conf)
      throws IOException {
    users = parseUserList(userloc, conf);
    if (users.size() == 0) {
      throw new IOException("Empty user list");
    }
    usercache.keySet().retainAll(users);
    return true;
  }

  @Override
  public synchronized UserGroupInformation getTargetUgi(
      UserGroupInformation ugi) {
    UserGroupInformation ret = usercache.get(ugi);
    if (null == ret) {
      ret = users.get(uidx++ % users.size());
      usercache.put(ugi, ret);
    }
    UserGroupInformation val = null;
    try {
      val = UserGroupInformation.createProxyUser(
        ret.getUserName(), UserGroupInformation.getLoginUser());
    } catch (IOException e) {
      LOG.error("Error while creating the proxy user " ,e);
    }
    return val;
  }

}
