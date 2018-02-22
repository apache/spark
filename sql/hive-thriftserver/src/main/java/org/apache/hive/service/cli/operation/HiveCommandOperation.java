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

package org.apache.hive.service.cli.operation;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.CharEncoding;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.OperationLog;
import org.apache.hadoop.hive.ql.session.OperationLog.LoggingLevel;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.ServiceUtils;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.session.HiveSession;

/**
 * Executes a HiveCommand
 */
public class HiveCommandOperation extends ExecuteStatementOperation {
  private final CommandProcessor commandProcessor;
  private TableSchema resultSchema = null;
  private boolean closeSessionStreams = true; // Only close file based streams, not System.out and System.err.

  /**
   * For processors other than Hive queries (Driver), they output to session.out (a temp file)
   * first and the fetchOne/fetchN/fetchAll functions get the output from pipeIn.
   */
  private BufferedReader resultReader;

  protected HiveCommandOperation(HiveSession parentSession, String statement,
      CommandProcessor commandProcessor, Map<String, String> confOverlay) {
    super(parentSession, statement, confOverlay, false);
    this.commandProcessor = commandProcessor;
    setupSessionIO(parentSession.getSessionState());
  }

  private void setupSessionIO(SessionState sessionState) {
    try {
      LOG.info("Putting temp output to file " + sessionState.getTmpOutputFile().toString()
          + " and error output to file " + sessionState.getTmpErrOutputFile().toString());
      sessionState.in = null; // hive server's session input stream is not used
      // open a per-session file in auto-flush mode for writing temp results and tmp error output
      sessionState.out =
          new PrintStream(new FileOutputStream(sessionState.getTmpOutputFile()), true, CharEncoding.UTF_8);
      sessionState.err =
          new PrintStream(new FileOutputStream(sessionState.getTmpErrOutputFile()), true, CharEncoding.UTF_8);
    } catch (IOException e) {
      LOG.error("Error in creating temp output file ", e);

      // Close file streams to avoid resource leaking
      ServiceUtils.cleanup(LOG, parentSession.getSessionState().out, parentSession.getSessionState().err);
      closeSessionStreams = false;

      try {
        sessionState.in = null;
        sessionState.out = new PrintStream(System.out, true, CharEncoding.UTF_8);
        sessionState.err = new PrintStream(System.err, true, CharEncoding.UTF_8);
      } catch (UnsupportedEncodingException ee) {
        LOG.error("Error creating PrintStream", e);
        ee.printStackTrace();
        sessionState.out = null;
        sessionState.err = null;
      }
    }
  }


  private void tearDownSessionIO() {
    if (closeSessionStreams) {
      ServiceUtils.cleanup(LOG, parentSession.getSessionState().out, parentSession.getSessionState().err);
    }
  }

  @Override
  public void runInternal() throws HiveSQLException {
    setState(OperationState.RUNNING);
    try {
      String command = getStatement().trim();
      String[] tokens = statement.split("\\s");
      String commandArgs = command.substring(tokens[0].length()).trim();

      CommandProcessorResponse response = commandProcessor.run(commandArgs);
      int returnCode = response.getResponseCode();
      if (returnCode != 0) {
        throw toSQLException("Error while processing statement", response);
      }
      Schema schema = response.getSchema();
      if (schema != null) {
        setHasResultSet(true);
        resultSchema = new TableSchema(schema);
      } else {
        setHasResultSet(false);
        resultSchema = new TableSchema();
      }
      if (response.getConsoleMessages() != null) {
        // Propagate processor messages (if any) to beeline or other client.
        OperationLog ol = OperationLog.getCurrentOperationLog();
        if (ol != null) {
          for (String consoleMsg : response.getConsoleMessages()) {
            ol.writeOperationLog(LoggingLevel.EXECUTION, consoleMsg + "\n");
          }
        }
      }
    } catch (HiveSQLException e) {
      setState(OperationState.ERROR);
      throw e;
    } catch (Exception e) {
      setState(OperationState.ERROR);
      throw new HiveSQLException("Error running query: " + e.toString(), e);
    }
    setState(OperationState.FINISHED);
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.operation.Operation#close()
   */
  @Override
  public void close() throws HiveSQLException {
    setState(OperationState.CLOSED);
    tearDownSessionIO();
    cleanTmpFile();
    cleanupOperationLog();
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.operation.Operation#getResultSetSchema()
   */
  @Override
  public TableSchema getResultSetSchema() throws HiveSQLException {
    return resultSchema;
  }

  /* (non-Javadoc)
   * @see org.apache.hive.service.cli.operation.Operation#getNextRowSet(org.apache.hive.service.cli.FetchOrientation, long)
   */
  @Override
  public RowSet getNextRowSet(FetchOrientation orientation, long maxRows) throws HiveSQLException {
    validateDefaultFetchOrientation(orientation);
    if (orientation.equals(FetchOrientation.FETCH_FIRST)) {
      resetResultReader();
    }
    List<String> rows = readResults((int) maxRows);
    RowSet rowSet = RowSetFactory.create(resultSchema, getProtocolVersion(), false);

    for (String row : rows) {
      rowSet.addRow(new String[] {row});
    }
    return rowSet;
  }

  /**
   * Reads the temporary results for non-Hive (non-Driver) commands to the
   * resulting List of strings.
   * @param nLines number of lines read at once. If it is <= 0, then read all lines.
   */
  private List<String> readResults(int nLines) throws HiveSQLException {
    if (resultReader == null) {
      SessionState sessionState = getParentSession().getSessionState();
      File tmp = sessionState.getTmpOutputFile();
      try {
        resultReader = new BufferedReader(new FileReader(tmp));
      } catch (FileNotFoundException e) {
        LOG.error("File " + tmp + " not found. ", e);
        throw new HiveSQLException(e);
      }
    }
    List<String> results = new ArrayList<String>();

    for (int i = 0; i < nLines || nLines <= 0; ++i) {
      try {
        String line = resultReader.readLine();
        if (line == null) {
          // reached the end of the result file
          break;
        } else {
          results.add(line);
        }
      } catch (IOException e) {
        LOG.error("Reading temp results encountered an exception: ", e);
        throw new HiveSQLException(e);
      }
    }
    return results;
  }

  private void cleanTmpFile() {
    resetResultReader();
    SessionState sessionState = getParentSession().getSessionState();
    sessionState.deleteTmpOutputFile();
    sessionState.deleteTmpErrOutputFile();
  }

  private void resetResultReader() {
    if (resultReader != null) {
      ServiceUtils.cleanup(LOG, resultReader);
      resultReader = null;
    }
  }

  @Override
  public void cancel(OperationState stateAfterCancel) throws HiveSQLException {
    throw new UnsupportedOperationException("HiveCommandOperation.cancel()");
  }
}
