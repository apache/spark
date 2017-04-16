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
package org.apache.hadoop.tools.rumen;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Reading JSON-encoded cluster topology and produce the parsed
 * {@link LoggedNetworkTopology} object.
 */
public class ClusterTopologyReader {
  private LoggedNetworkTopology topology;

  private void readTopology(JsonObjectMapperParser<LoggedNetworkTopology> parser)
      throws IOException {
    try {
      topology = parser.getNext();
      if (topology == null) {
        throw new IOException(
            "Input file does not contain valid topology data.");
      }
    } finally {
      parser.close();
    }
  }

  /**
   * Constructor.
   * 
   * @param path
   *          Path to the JSON-encoded topology file, possibly compressed.
   * @param conf
   * @throws IOException
   */
  public ClusterTopologyReader(Path path, Configuration conf)
      throws IOException {
    JsonObjectMapperParser<LoggedNetworkTopology> parser = new JsonObjectMapperParser<LoggedNetworkTopology>(
        path, LoggedNetworkTopology.class, conf);
    readTopology(parser);
  }

  /**
   * Constructor.
   * 
   * @param input
   *          The input stream for the JSON-encoded topology data.
   */
  public ClusterTopologyReader(InputStream input) throws IOException {
    JsonObjectMapperParser<LoggedNetworkTopology> parser = new JsonObjectMapperParser<LoggedNetworkTopology>(
        input, LoggedNetworkTopology.class);
    readTopology(parser);
  }

  /**
   * Get the {@link LoggedNetworkTopology} object.
   * 
   * @return The {@link LoggedNetworkTopology} object parsed from the input.
   */
  public LoggedNetworkTopology get() {
    return topology;
  }
}
