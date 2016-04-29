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

package org.apache.hive.service.server;

import java.util.Properties;

import org.apache.commons.cli.*;

public class HiveServerServerOptionsProcessor {
  private final Options options = new Options();
  private org.apache.commons.cli.CommandLine commandLine;
  private final String serverName;
  private final StringBuilder debugMessage = new StringBuilder();

  @SuppressWarnings("static-access")
  public HiveServerServerOptionsProcessor(String serverName) {
    this.serverName = serverName;
    // -hiveconf x=y
    options.addOption(OptionBuilder
      .withValueSeparator()
      .hasArgs(2)
      .withArgName("property=value")
      .withLongOpt("hiveconf")
      .withDescription("Use value for given property")
      .create());
    // -deregister <versionNumber>
    options.addOption(OptionBuilder
      .hasArgs(1)
      .withArgName("versionNumber")
      .withLongOpt("deregister")
      .withDescription("Deregister all instances of given version from dynamic service discovery")
      .create());
    options.addOption(new Option("H", "help", false, "Print help information"));
  }

  public HiveServer2.ServerOptionsProcessorResponse parse(String[] argv) {
    try {
      commandLine = new GnuParser().parse(options, argv);
      // Process --hiveconf
      // Get hiveconf param values and set the System property values
      Properties confProps = commandLine.getOptionProperties("hiveconf");
      for (String propKey : confProps.stringPropertyNames()) {
        // save logging message for log4j output latter after log4j initialize properly
        debugMessage.append("Setting " + propKey + "=" + confProps.getProperty(propKey) + ";\n");
        // System.setProperty("hivecli." + propKey, confProps.getProperty(propKey));
        System.setProperty(propKey, confProps.getProperty(propKey));
      }

      // Process --help
      if (commandLine.hasOption('H')) {
        return new HiveServer2.ServerOptionsProcessorResponse(
          new HiveServer2.HelpOptionExecutor(serverName, options));
      }

      // Process --deregister
      if (commandLine.hasOption("deregister")) {
        return new HiveServer2.ServerOptionsProcessorResponse(
          new HiveServer2.DeregisterOptionExecutor(
          commandLine.getOptionValue("deregister")));
      }
    } catch (ParseException e) {
      // Error out & exit - we were not able to parse the args successfully
      System.err.println("Error starting HiveServer2 with given arguments: ");
      System.err.println(e.getMessage());
      System.exit(-1);
    }
    // Default executor, when no option is specified
    return new HiveServer2.ServerOptionsProcessorResponse(new HiveServer2.StartOptionExecutor());
  }
}
