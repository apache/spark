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

package org.apache.hadoop.mapred.pipes;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.crypto.SecretKey;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.security.SecureShuffleUtils;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.util.StringUtils;

/**
 * This protocol is a binary implementation of the Pipes protocol.
 */
class BinaryProtocol<K1 extends WritableComparable, V1 extends Writable,
                     K2 extends WritableComparable, V2 extends Writable>
  implements DownwardProtocol<K1, V1> {
  
  public static final int CURRENT_PROTOCOL_VERSION = 0;
  /**
   * The buffer size for the command socket
   */
  private static final int BUFFER_SIZE = 128*1024;

  private DataOutputStream stream;
  private DataOutputBuffer buffer = new DataOutputBuffer();
  private static final Log LOG = 
    LogFactory.getLog(BinaryProtocol.class.getName());
  private UplinkReaderThread uplink;

  /**
   * The integer codes to represent the different messages. These must match
   * the C++ codes or massive confusion will result.
   */
  private static enum MessageType { START(0),
                                    SET_JOB_CONF(1),
                                    SET_INPUT_TYPES(2),
                                    RUN_MAP(3),
                                    MAP_ITEM(4),
                                    RUN_REDUCE(5),
                                    REDUCE_KEY(6),
                                    REDUCE_VALUE(7),
                                    CLOSE(8),
                                    ABORT(9),
                                    AUTHENTICATION_REQ(10),
                                    OUTPUT(50),
                                    PARTITIONED_OUTPUT(51),
                                    STATUS(52),
                                    PROGRESS(53),
                                    DONE(54),
                                    REGISTER_COUNTER(55),
                                    INCREMENT_COUNTER(56),
                                    AUTHENTICATION_RESP(57);
    final int code;
    MessageType(int code) {
      this.code = code;
    }
  }

  private static class UplinkReaderThread<K2 extends WritableComparable,
                                          V2 extends Writable>  
    extends Thread {
    
    private DataInputStream inStream;
    private UpwardProtocol<K2, V2> handler;
    private K2 key;
    private V2 value;
    private boolean authPending = true;
    
    public UplinkReaderThread(InputStream stream,
                              UpwardProtocol<K2, V2> handler, 
                              K2 key, V2 value) throws IOException{
      inStream = new DataInputStream(new BufferedInputStream(stream, 
                                                             BUFFER_SIZE));
      this.handler = handler;
      this.key = key;
      this.value = value;
    }

    public void closeConnection() throws IOException {
      inStream.close();
    }

    public void run() {
      while (true) {
        try {
          if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
          }
          int cmd = WritableUtils.readVInt(inStream);
          LOG.debug("Handling uplink command " + cmd);
          if (cmd == MessageType.AUTHENTICATION_RESP.code) {
            String digest = Text.readString(inStream);
            authPending = !handler.authenticate(digest);
          } else if (authPending) {
            LOG.warn("Message " + cmd + " received before authentication is "
                + "complete. Ignoring");
            continue;
          } else if (cmd == MessageType.OUTPUT.code) {
            readObject(key);
            readObject(value);
            handler.output(key, value);
          } else if (cmd == MessageType.PARTITIONED_OUTPUT.code) {
            int part = WritableUtils.readVInt(inStream);
            readObject(key);
            readObject(value);
            handler.partitionedOutput(part, key, value);
          } else if (cmd == MessageType.STATUS.code) {
            handler.status(Text.readString(inStream));
          } else if (cmd == MessageType.PROGRESS.code) {
            handler.progress(inStream.readFloat());
          } else if (cmd == MessageType.REGISTER_COUNTER.code) {
            int id = WritableUtils.readVInt(inStream);
            String group = Text.readString(inStream);
            String name = Text.readString(inStream);
            handler.registerCounter(id, group, name);
          } else if (cmd == MessageType.INCREMENT_COUNTER.code) {
            int id = WritableUtils.readVInt(inStream);
            long amount = WritableUtils.readVLong(inStream);
            handler.incrementCounter(id, amount);
          } else if (cmd == MessageType.DONE.code) {
            LOG.debug("Pipe child done");
            handler.done();
            return;
          } else {
            throw new IOException("Bad command code: " + cmd);
          }
        } catch (InterruptedException e) {
          return;
        } catch (Throwable e) {
          LOG.error(StringUtils.stringifyException(e));
          handler.failed(e);
          return;
        }
      }
    }
    
    private void readObject(Writable obj) throws IOException {
      int numBytes = WritableUtils.readVInt(inStream);
      byte[] buffer;
      // For BytesWritable and Text, use the specified length to set the length
      // this causes the "obvious" translations to work. So that if you emit
      // a string "abc" from C++, it shows up as "abc".
      if (obj instanceof BytesWritable) {
        buffer = new byte[numBytes];
        inStream.readFully(buffer);
        ((BytesWritable) obj).set(buffer, 0, numBytes);
      } else if (obj instanceof Text) {
        buffer = new byte[numBytes];
        inStream.readFully(buffer);
        ((Text) obj).set(buffer);
      } else {
        obj.readFields(inStream);
      }
    }
  }

  /**
   * An output stream that will save a copy of the data into a file.
   */
  private static class TeeOutputStream extends FilterOutputStream {
    private OutputStream file;
    TeeOutputStream(String filename, OutputStream base) throws IOException {
      super(base);
      file = new FileOutputStream(filename);
    }
    public void write(byte b[], int off, int len) throws IOException {
      file.write(b,off,len);
      out.write(b,off,len);
    }

    public void write(int b) throws IOException {
      file.write(b);
      out.write(b);
    }

    public void flush() throws IOException {
      file.flush();
      out.flush();
    }

    public void close() throws IOException {
      flush();
      file.close();
      out.close();
    }
  }

  /**
   * Create a proxy object that will speak the binary protocol on a socket.
   * Upward messages are passed on the specified handler and downward
   * downward messages are public methods on this object.
   * @param sock The socket to communicate on.
   * @param handler The handler for the received messages.
   * @param key The object to read keys into.
   * @param value The object to read values into.
   * @param config The job's configuration
   * @throws IOException
   */
  public BinaryProtocol(Socket sock, 
                        UpwardProtocol<K2, V2> handler,
                        K2 key,
                        V2 value,
                        JobConf config) throws IOException {
    OutputStream raw = sock.getOutputStream();
    // If we are debugging, save a copy of the downlink commands to a file
    if (Submitter.getKeepCommandFile(config)) {
      raw = new TeeOutputStream("downlink.data", raw);
    }
    stream = new DataOutputStream(new BufferedOutputStream(raw, 
                                                           BUFFER_SIZE)) ;
    uplink = new UplinkReaderThread<K2, V2>(sock.getInputStream(),
                                            handler, key, value);
    uplink.setName("pipe-uplink-handler");
    uplink.start();
  }

  /**
   * Close the connection and shutdown the handler thread.
   * @throws IOException
   * @throws InterruptedException
   */
  public void close() throws IOException, InterruptedException {
    LOG.debug("closing connection");
    stream.close();
    uplink.closeConnection();
    uplink.interrupt();
    uplink.join();
  }
  
  public void authenticate(String digest, String challenge)
      throws IOException {
    LOG.debug("Sending AUTHENTICATION_REQ, digest=" + digest + ", challenge="
        + challenge);
    WritableUtils.writeVInt(stream, MessageType.AUTHENTICATION_REQ.code);
    Text.writeString(stream, digest);
    Text.writeString(stream, challenge);
  }

  public void start() throws IOException {
    LOG.debug("starting downlink");
    WritableUtils.writeVInt(stream, MessageType.START.code);
    WritableUtils.writeVInt(stream, CURRENT_PROTOCOL_VERSION);
  }

  public void setJobConf(JobConf job) throws IOException {
    WritableUtils.writeVInt(stream, MessageType.SET_JOB_CONF.code);
    List<String> list = new ArrayList<String>();
    for(Map.Entry<String, String> itm: job) {
      list.add(itm.getKey());
      list.add(itm.getValue());
    }
    WritableUtils.writeVInt(stream, list.size());
    for(String entry: list){
      Text.writeString(stream, entry);
    }
  }

  public void setInputTypes(String keyType, 
                            String valueType) throws IOException {
    WritableUtils.writeVInt(stream, MessageType.SET_INPUT_TYPES.code);
    Text.writeString(stream, keyType);
    Text.writeString(stream, valueType);
  }

  public void runMap(InputSplit split, int numReduces, 
                     boolean pipedInput) throws IOException {
    WritableUtils.writeVInt(stream, MessageType.RUN_MAP.code);
    writeObject(split);
    WritableUtils.writeVInt(stream, numReduces);
    WritableUtils.writeVInt(stream, pipedInput ? 1 : 0);
  }

  public void mapItem(WritableComparable key, 
                      Writable value) throws IOException {
    WritableUtils.writeVInt(stream, MessageType.MAP_ITEM.code);
    writeObject(key);
    writeObject(value);
  }

  public void runReduce(int reduce, boolean pipedOutput) throws IOException {
    WritableUtils.writeVInt(stream, MessageType.RUN_REDUCE.code);
    WritableUtils.writeVInt(stream, reduce);
    WritableUtils.writeVInt(stream, pipedOutput ? 1 : 0);
  }

  public void reduceKey(WritableComparable key) throws IOException {
    WritableUtils.writeVInt(stream, MessageType.REDUCE_KEY.code);
    writeObject(key);
  }

  public void reduceValue(Writable value) throws IOException {
    WritableUtils.writeVInt(stream, MessageType.REDUCE_VALUE.code);
    writeObject(value);
  }

  public void endOfInput() throws IOException {
    WritableUtils.writeVInt(stream, MessageType.CLOSE.code);
    LOG.debug("Sent close command");
  }
  
  public void abort() throws IOException {
    WritableUtils.writeVInt(stream, MessageType.ABORT.code);
    LOG.debug("Sent abort command");
  }

  public void flush() throws IOException {
    stream.flush();
  }

  /**
   * Write the given object to the stream. If it is a Text or BytesWritable,
   * write it directly. Otherwise, write it to a buffer and then write the
   * length and data to the stream.
   * @param obj the object to write
   * @throws IOException
   */
  private void writeObject(Writable obj) throws IOException {
    // For Text and BytesWritable, encode them directly, so that they end up
    // in C++ as the natural translations.
    if (obj instanceof Text) {
      Text t = (Text) obj;
      int len = t.getLength();
      WritableUtils.writeVInt(stream, len);
      stream.write(t.getBytes(), 0, len);
    } else if (obj instanceof BytesWritable) {
      BytesWritable b = (BytesWritable) obj;
      int len = b.getLength();
      WritableUtils.writeVInt(stream, len);
      stream.write(b.getBytes(), 0, len);
    } else {
      buffer.reset();
      obj.write(buffer);
      int length = buffer.getLength();
      WritableUtils.writeVInt(stream, length);
      stream.write(buffer.getData(), 0, length);
    }
  }
}
