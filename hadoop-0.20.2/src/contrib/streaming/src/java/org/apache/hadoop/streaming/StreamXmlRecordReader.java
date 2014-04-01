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

package org.apache.hadoop.streaming;

import java.io.*;
import java.util.regex.*;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;

/** A way to interpret XML fragments as Mapper input records.
 *  Values are XML subtrees delimited by configurable tags.
 *  Keys could be the value of a certain attribute in the XML subtree, 
 *  but this is left to the stream processor application.
 *
 *  The name-value properties that StreamXmlRecordReader understands are:
 *    String begin (chars marking beginning of record)
 *    String end   (chars marking end of record)
 *    int maxrec   (maximum record size)
 *    int lookahead(maximum lookahead to sync CDATA)
 *    boolean slowmatch
 */
public class StreamXmlRecordReader extends StreamBaseRecordReader {

  public StreamXmlRecordReader(FSDataInputStream in, FileSplit split, Reporter reporter,
                               JobConf job, FileSystem fs) throws IOException {
    super(in, split, reporter, job, fs);

    beginMark_ = checkJobGet(CONF_NS + "begin");
    endMark_ = checkJobGet(CONF_NS + "end");

    maxRecSize_ = job_.getInt(CONF_NS + "maxrec", 50 * 1000);
    lookAhead_ = job_.getInt(CONF_NS + "lookahead", 2 * maxRecSize_);
    synched_ = false;

    slowMatch_ = job_.getBoolean(CONF_NS + "slowmatch", false);
    if (slowMatch_) {
      beginPat_ = makePatternCDataOrMark(beginMark_);
      endPat_ = makePatternCDataOrMark(endMark_);
    }
    init();
  }

  public void init() throws IOException {
    LOG.info("StreamBaseRecordReader.init: " + " start_=" + start_ + " end_=" + end_ + " length_="
             + length_ + " start_ > in_.getPos() =" + (start_ > in_.getPos()) + " " + start_ + " > "
             + in_.getPos());
    if (start_ > in_.getPos()) {
      in_.seek(start_);
    }
    pos_ = start_;
    bin_ = new BufferedInputStream(in_);
    seekNextRecordBoundary();
  }
  
  int numNext = 0;

  public synchronized boolean next(Text key, Text value) throws IOException {
    numNext++;
    if (pos_ >= end_) {
      return false;
    }

    DataOutputBuffer buf = new DataOutputBuffer();
    if (!readUntilMatchBegin()) {
      return false;
    }
    if (!readUntilMatchEnd(buf)) {
      return false;
    }

    // There is only one elem..key/value splitting is not done here.
    byte[] record = new byte[buf.getLength()];
    System.arraycopy(buf.getData(), 0, record, 0, record.length);

    numRecStats(record, 0, record.length);

    key.set(record);
    value.set("");

    return true;
  }

  public void seekNextRecordBoundary() throws IOException {
    readUntilMatchBegin();
  }

  boolean readUntilMatchBegin() throws IOException {
    if (slowMatch_) {
      return slowReadUntilMatch(beginPat_, false, null);
    } else {
      return fastReadUntilMatch(beginMark_, false, null);
    }
  }

  private boolean readUntilMatchEnd(DataOutputBuffer buf) throws IOException {
    if (slowMatch_) {
      return slowReadUntilMatch(endPat_, true, buf);
    } else {
      return fastReadUntilMatch(endMark_, true, buf);
    }
  }

  private boolean slowReadUntilMatch(Pattern markPattern, boolean includePat,
                                     DataOutputBuffer outBufOrNull) throws IOException {
    byte[] buf = new byte[Math.max(lookAhead_, maxRecSize_)];
    int read = 0;
    bin_.mark(Math.max(lookAhead_, maxRecSize_) + 2); //mark to invalidate if we read more
    read = bin_.read(buf);
    if (read == -1) return false;

    String sbuf = new String(buf, 0, read, "UTF-8");
    Matcher match = markPattern.matcher(sbuf);

    firstMatchStart_ = NA;
    firstMatchEnd_ = NA;
    int bufPos = 0;
    int state = synched_ ? CDATA_OUT : CDATA_UNK;
    int s = 0;

    while (match.find(bufPos)) {
      int input;
      if (match.group(1) != null) {
        input = CDATA_BEGIN;
      } else if (match.group(2) != null) {
        input = CDATA_END;
        firstMatchStart_ = NA; // |<DOC CDATA[ </DOC> ]]> should keep it
      } else {
        input = RECORD_MAYBE;
      }
      if (input == RECORD_MAYBE) {
        if (firstMatchStart_ == NA) {
          firstMatchStart_ = match.start();
          firstMatchEnd_ = match.end();
        }
      }
      state = nextState(state, input, match.start());
      if (state == RECORD_ACCEPT) {
        break;
      }
      bufPos = match.end();
      s++;
    }
    if (state != CDATA_UNK) {
      synched_ = true;
    }
    boolean matched = (firstMatchStart_ != NA) && (state == RECORD_ACCEPT || state == CDATA_UNK);
    if (matched) {
      int endPos = includePat ? firstMatchEnd_ : firstMatchStart_;
      bin_.reset();

      for (long skiplen = endPos; skiplen > 0; ) {
        skiplen -= bin_.skip(skiplen); // Skip succeeds as we have read this buffer
      }

      pos_ += endPos;
      if (outBufOrNull != null) {
        outBufOrNull.writeBytes(sbuf.substring(0,endPos));
      }
    }
    return matched;
  }

  // states
  final static int CDATA_IN = 10;
  final static int CDATA_OUT = 11;
  final static int CDATA_UNK = 12;
  final static int RECORD_ACCEPT = 13;
  // inputs
  final static int CDATA_BEGIN = 20;
  final static int CDATA_END = 21;
  final static int RECORD_MAYBE = 22;

  /* also updates firstMatchStart_;*/
  int nextState(int state, int input, int bufPos) {
    switch (state) {
    case CDATA_UNK:
    case CDATA_OUT:
      switch (input) {
      case CDATA_BEGIN:
        return CDATA_IN;
      case CDATA_END:
        if (state == CDATA_OUT) {
          //System.out.println("buggy XML " + bufPos);
        }
        return CDATA_OUT;
      case RECORD_MAYBE:
        return (state == CDATA_UNK) ? CDATA_UNK : RECORD_ACCEPT;
      }
      break;
    case CDATA_IN:
      return (input == CDATA_END) ? CDATA_OUT : CDATA_IN;
    }
    throw new IllegalStateException(state + " " + input + " " + bufPos + " " + splitName_);
  }

  Pattern makePatternCDataOrMark(String escapedMark) {
    StringBuffer pat = new StringBuffer();
    addGroup(pat, StreamUtil.regexpEscape("CDATA[")); // CDATA_BEGIN
    addGroup(pat, StreamUtil.regexpEscape("]]>")); // CDATA_END
    addGroup(pat, escapedMark); // RECORD_MAYBE
    return Pattern.compile(pat.toString());
  }

  void addGroup(StringBuffer pat, String escapedGroup) {
    if (pat.length() > 0) {
      pat.append("|");
    }
    pat.append("(");
    pat.append(escapedGroup);
    pat.append(")");
  }

  boolean fastReadUntilMatch(String textPat, boolean includePat, DataOutputBuffer outBufOrNull) throws IOException {
    byte[] cpat = textPat.getBytes("UTF-8");
    int m = 0;
    boolean match = false;
    int msup = cpat.length;
    int LL = 120000 * 10;

    bin_.mark(LL); // large number to invalidate mark
    while (true) {
      int b = bin_.read();
      if (b == -1) break;

      byte c = (byte) b; // this assumes eight-bit matching. OK with UTF-8
      if (c == cpat[m]) {
        m++;
        if (m == msup) {
          match = true;
          break;
        }
      } else {
        bin_.mark(LL); // rest mark so we could jump back if we found a match
        if (outBufOrNull != null) {
          outBufOrNull.write(cpat, 0, m);
          outBufOrNull.write(c);
          pos_ += m;
        }
        m = 0;
      }
    }
    if (!includePat && match) {
      bin_.reset();
    } else if (outBufOrNull != null) {
      outBufOrNull.write(cpat);
      pos_ += msup;
    }
    return match;
  }

  String checkJobGet(String prop) throws IOException {
    String val = job_.get(prop);
    if (val == null) {
      throw new IOException("JobConf: missing required property: " + prop);
    }
    return val;
  }

  String beginMark_;
  String endMark_;

  Pattern beginPat_;
  Pattern endPat_;

  boolean slowMatch_;
  int lookAhead_; // bytes to read to try to synch CDATA/non-CDATA. Should be more than max record size
  int maxRecSize_;

  BufferedInputStream bin_; // Wrap FSDataInputStream for efficient backward seeks 
  long pos_; // Keep track on position with respect encapsulated FSDataInputStream  

  final static int NA = -1;
  int firstMatchStart_ = 0; // candidate record boundary. Might just be CDATA.
  int firstMatchEnd_ = 0;

  boolean synched_;
}
