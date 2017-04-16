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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.LineReader;

/**
 * Maps users in the trace to a set of valid target users on the test cluster.
 */
public interface UserResolver {

  /**
   * Configure the user map given the URI and configuration. The resolver's
   * contract will define how the resource will be interpreted, but the default
   * will typically interpret the URI as a {@link org.apache.hadoop.fs.Path}
   * listing target users. 
   * @param userdesc URI (possibly null) from which user information may be
   * loaded per the subclass contract.
   * @param conf The tool configuration.
   * @return true if the resource provided was used in building the list of
   * target users
   */
  public boolean setTargetUsers(URI userdesc, Configuration conf)
    throws IOException;

  /**
   * Map the given UGI to another per the subclass contract.
   * @param ugi User information from the trace.
   */
  public UserGroupInformation getTargetUgi(UserGroupInformation ugi);

}
