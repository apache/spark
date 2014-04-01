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

package org.apache.hadoop.contrib.index.example;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.contrib.index.mapred.DocumentAndOp;
import org.apache.hadoop.contrib.index.mapred.DocumentID;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

/**
 * A simple RecordReader for LineDoc for plain text files where each line is a
 * doc. Each line is as follows: documentID<SPACE>op<SPACE>content<EOF>,
 * where op can be "i", "ins" or "insert" for insert, "d", "del" or "delete"
 * for delete, or "u", "upd" or "update" for update.
 */
public class LineDocRecordReader implements
    RecordReader<DocumentID, LineDocTextAndOp> {
  private static final char SPACE = ' ';
  private static final char EOL = '\n';

  private long start;
  private long pos;
  private long end;
  private BufferedInputStream in;
  private ByteArrayOutputStream buffer = new ByteArrayOutputStream(256);

  /**
   * Provide a bridge to get the bytes from the ByteArrayOutputStream without
   * creating a new byte array.
   */
  private static class TextStuffer extends OutputStream {
    public Text target;

    public void write(int b) {
      throw new UnsupportedOperationException("write(byte) not supported");
    }

    public void write(byte[] data, int offset, int len) throws IOException {
      target.set(data, offset, len);
    }
  }

  private TextStuffer bridge = new TextStuffer();

  /**
   * Constructor
   * @param job
   * @param split  
   * @throws IOException
   */
  public LineDocRecordReader(Configuration job, FileSplit split)
      throws IOException {
    long start = split.getStart();
    long end = start + split.getLength();
    final Path file = split.getPath();

    // open the file and seek to the start of the split
    FileSystem fs = file.getFileSystem(job);
    FSDataInputStream fileIn = fs.open(split.getPath());
    InputStream in = fileIn;
    boolean skipFirstLine = false;
    if (start != 0) {
      skipFirstLine = true; // wait till BufferedInputStream to skip
      --start;
      fileIn.seek(start);
    }

    this.in = new BufferedInputStream(in);
    if (skipFirstLine) { // skip first line and re-establish "start".
      start += LineDocRecordReader.readData(this.in, null, EOL);
    }
    this.start = start;
    this.pos = start;
    this.end = end;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapred.RecordReader#close()
   */
  public void close() throws IOException {
    in.close();
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapred.RecordReader#createKey()
   */
  public DocumentID createKey() {
    return new DocumentID();
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapred.RecordReader#createValue()
   */
  public LineDocTextAndOp createValue() {
    return new LineDocTextAndOp();
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapred.RecordReader#getPos()
   */
  public long getPos() throws IOException {
    return pos;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapred.RecordReader#getProgress()
   */
  public float getProgress() throws IOException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float) (end - start));
    }
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.mapred.RecordReader#next(java.lang.Object, java.lang.Object)
   */
  public synchronized boolean next(DocumentID key, LineDocTextAndOp value)
      throws IOException {
    if (pos >= end) {
      return false;
    }

    // key is document id, which are bytes until first space
    if (!readInto(key.getText(), SPACE)) {
      return false;
    }

    // read operation: i/d/u, or ins/del/upd, or insert/delete/update
    Text opText = new Text();
    if (!readInto(opText, SPACE)) {
      return false;
    }
    String opStr = opText.toString();
    DocumentAndOp.Op op;
    if (opStr.equals("i") || opStr.equals("ins") || opStr.equals("insert")) {
      op = DocumentAndOp.Op.INSERT;
    } else if (opStr.equals("d") || opStr.equals("del")
        || opStr.equals("delete")) {
      op = DocumentAndOp.Op.DELETE;
    } else if (opStr.equals("u") || opStr.equals("upd")
        || opStr.equals("update")) {
      op = DocumentAndOp.Op.UPDATE;
    } else {
      // default is insert
      op = DocumentAndOp.Op.INSERT;
    }
    value.setOp(op);

    if (op == DocumentAndOp.Op.DELETE) {
      return true;
    } else {
      // read rest of the line
      return readInto(value.getText(), EOL);
    }
  }

  private boolean readInto(Text text, char delimiter) throws IOException {
    buffer.reset();
    long bytesRead = readData(in, buffer, delimiter);
    if (bytesRead == 0) {
      return false;
    }
    pos += bytesRead;
    bridge.target = text;
    buffer.writeTo(bridge);
    return true;
  }

  private static long readData(InputStream in, OutputStream out, char delimiter)
      throws IOException {
    long bytes = 0;
    while (true) {

      int b = in.read();
      if (b == -1) {
        break;
      }
      bytes += 1;

      byte c = (byte) b;
      if (c == EOL || c == delimiter) {
        break;
      }

      if (c == '\r') {
        in.mark(1);
        byte nextC = (byte) in.read();
        if (nextC != EOL || c == delimiter) {
          in.reset();
        } else {
          bytes += 1;
        }
        break;
      }

      if (out != null) {
        out.write(c);
      }
    }
    return bytes;
  }
}
