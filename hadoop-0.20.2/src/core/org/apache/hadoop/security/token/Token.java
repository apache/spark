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

package org.apache.hadoop.security.token;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.codec.binary.Base64;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * The client-side form of the token.
 */
public class Token<T extends TokenIdentifier> implements Writable {
  private byte[] identifier;
  private byte[] password;
  private Text kind;
  private Text service;
  
  /**
   * Construct a token given a token identifier and a secret manager for the
   * type of the token identifier.
   * @param id the token identifier
   * @param mgr the secret manager
   */
  public Token(T id, SecretManager<T> mgr) {
    password = mgr.createPassword(id);
    identifier = id.getBytes();
    kind = id.getKind();
    service = new Text();
  }
 
  /**
   * Construct a token from the components.
   * @param identifier the token identifier
   * @param password the token's password
   * @param kind the kind of token
   * @param service the service for this token
   */
  public Token(byte[] identifier, byte[] password, Text kind, Text service) {
    this.identifier = identifier;
    this.password = password;
    this.kind = kind;
    this.service = service;
  }

  /**
   * Default constructor
   */
  public Token() {
    identifier = new byte[0];
    password = new byte[0];
    kind = new Text();
    service = new Text();
  }

  /**
   * Get the token identifier
   * @return the token identifier
   */
  public byte[] getIdentifier() {
    return identifier;
  }
  
  /**
   * Get the token password/secret
   * @return the token password/secret
   */
  public byte[] getPassword() {
    return password;
  }
  
  /**
   * Get the token kind
   * @return the kind of the token
   */
  public Text getKind() {
    return kind;
  }

  /**
   * Get the service on which the token is supposed to be used
   * @return the service name
   */
  public Text getService() {
    return service;
  }
  
  /**
   * Set the service on which the token is supposed to be used
   * @param newService the service name
   */
  public void setService(Text newService) {
    service = newService;
  }

  /** {@inheritDoc} */
  public void readFields(DataInput in) throws IOException {
    int len = WritableUtils.readVInt(in);
    if (identifier == null || identifier.length != len) {
      identifier = new byte[len];
    }
    in.readFully(identifier);
    len = WritableUtils.readVInt(in);
    if (password == null || password.length != len) {
      password = new byte[len];
    }
    in.readFully(password);
    kind.readFields(in);
    service.readFields(in);
  }

  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, identifier.length);
    out.write(identifier);
    WritableUtils.writeVInt(out, password.length);
    out.write(password);
    kind.write(out);
    service.write(out);
  }

  /**
   * Generate a string with the url-quoted base64 encoded serialized form
   * of the Writable.
   * @param obj the object to serialize
   * @return the encoded string
   * @throws IOException
   */
  private static String encodeWritable(Writable obj) throws IOException {
    DataOutputBuffer buf = new DataOutputBuffer();
    obj.write(buf);
    Base64 encoder = new Base64(0, null, true);
    byte[] raw = new byte[buf.getLength()];
    System.arraycopy(buf.getData(), 0, raw, 0, buf.getLength());
    return encoder.encodeToString(raw);
  }
  
  /**
   * Modify the writable to the value from the newValue
   * @param obj the object to read into
   * @param newValue the string with the url-safe base64 encoded bytes
   * @throws IOException
   */
  private static void decodeWritable(Writable obj, 
                                     String newValue) throws IOException {
    Base64 decoder = new Base64(0, null, true);
    DataInputBuffer buf = new DataInputBuffer();
    byte[] decoded = decoder.decode(newValue);
    buf.reset(decoded, decoded.length);
    obj.readFields(buf);
  }

  /**
   * Encode this token as a url safe string
   * @return the encoded string
   * @throws IOException
   */
  public String encodeToUrlString() throws IOException {
    return encodeWritable(this);
  }
  
  /**
   * Decode the given url safe string into this token.
   * @param newValue the encoded string
   * @throws IOException
   */
  public void decodeFromUrlString(String newValue) throws IOException {
    decodeWritable(this, newValue);
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public boolean equals(Object right) {
    if (this == right) {
      return true;
    } else if (right == null || getClass() != right.getClass()) {
      return false;
    } else {
      Token<T> r = (Token<T>) right;
      return Arrays.equals(identifier, r.identifier) &&
             Arrays.equals(password, r.password) &&
             kind.equals(r.kind) &&
             service.equals(r.service);
    }
  }
  
  @Override
  public int hashCode() {
    return WritableComparator.hashBytes(identifier, identifier.length);
  }
  
  private static void addBinaryBuffer(StringBuilder buffer, byte[] bytes) {
    for (int idx = 0; idx < bytes.length; idx++) {
      // if not the first, put a blank separator in
      if (idx != 0) {
        buffer.append(' ');
      }
      String num = Integer.toHexString(0xff & bytes[idx]);
      // if it is only one digit, add a leading 0.
      if (num.length() < 2) {
        buffer.append('0');
      }
      buffer.append(num);
    }
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("Ident: ");
    addBinaryBuffer(buffer, identifier);
    buffer.append(", Pass: ");
    addBinaryBuffer(buffer, password);
    buffer.append(", Kind: ");
    buffer.append(kind.toString());
    buffer.append(", Service: ");
    buffer.append(service.toString());
    return buffer.toString();
  }
}
