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

package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/** Context data for an ongoing NameNode recovery process. */
public final class MetaRecoveryContext  {
  public static final Log LOG = LogFactory.getLog(MetaRecoveryContext.class.getName());
  private int force;
  public static final int FORCE_NONE = 0;
  public static final int FORCE_FIRST_CHOICE = 1;
  public static final int FORCE_ALL = 2;

  public MetaRecoveryContext(int force) {
    this.force = force;
  }
  /** Display a prompt to the user and get his or her choice.
   *  
   * @param prompt      The prompt to display
   * @param c1          Choice 1
   * @param choices     Other choies
   *
   * @return            The choice that was taken
   * @throws IOException
   */
  public String ask(String prompt, String firstChoice, String... choices) 
      throws IOException {
    while (true) {
      LOG.error(prompt);
      if (force > FORCE_NONE) {
        LOG.info("Automatically choosing " + firstChoice);
        return firstChoice;
      }
      StringBuilder responseBuilder = new StringBuilder();
      while (true) {
        int c = System.in.read();
        if (c == -1 || c == '\r' || c == '\n') {
          break;
        }
        responseBuilder.append((char)c);
      }
      String response = responseBuilder.toString();
      if (response.equalsIgnoreCase(firstChoice)) {
        return firstChoice;
      }
      for (String c : choices) {
        if (response.equalsIgnoreCase(c)) {
          return c;
        }
      }
      LOG.error("I'm sorry, I cannot understand your response.\n");
    }
  }
  /** Log a message and quit */
  public void quit() {
    LOG.error("Exiting on user request.");
    System.exit(0);
  }

  static public void editLogLoaderPrompt(String prompt,
      MetaRecoveryContext recovery) throws IOException
  {
    if (recovery == null) {
      throw new IOException(prompt);
    }
    LOG.error(prompt);
    String answer = recovery.ask(
      "\nEnter 's' to stop reading the edit log here, abandoning any later " +
        "edits.\n" +
      "Enter 'q' to quit without saving.\n" +
      "(s/q)", "s", "q");
    if (answer.equals("s")) {
      LOG.error("We will stop reading the edits log here.  "
          + "NOTE: Some edits have been lost!");
      return;
    } else if (answer.equals("q")) {
      recovery.quit();
    }
  }
}
