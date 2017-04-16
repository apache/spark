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
package org.apache.hadoop.fs.permission;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**
 * A class for file/directory permissions.
 */
public class FsPermission implements Writable {
  private static final Log LOG = LogFactory.getLog(FsPermission.class);
  
  static final WritableFactory FACTORY = new WritableFactory() {
    public Writable newInstance() { return new FsPermission(); }
  };
  static {                                      // register a ctor
    WritableFactories.setFactory(FsPermission.class, FACTORY);
    WritableFactories.setFactory(ImmutableFsPermission.class, FACTORY);
  }

  /** Create an immutable {@link FsPermission} object. */
  public static FsPermission createImmutable(short permission) {
    return new ImmutableFsPermission(permission);
  }

  //POSIX permission style
  private FsAction useraction = null;
  private FsAction groupaction = null;
  private FsAction otheraction = null;
  private boolean stickyBit = false;

  private FsPermission() {}

  /**
   * Construct by the given {@link FsAction}.
   * @param u user action
   * @param g group action
   * @param o other action
   */
  public FsPermission(FsAction u, FsAction g, FsAction o) {
    this(u, g, o, false);
  }

  public FsPermission(FsAction u, FsAction g, FsAction o, boolean sb) {
    set(u, g, o, sb);
  }

  /**
   * Construct by the given mode.
   * @param mode
   * @see #toShort()
   */
  public FsPermission(short mode) { fromShort(mode); }

  /**
   * Copy constructor
   * 
   * @param other other permission
   */
  public FsPermission(FsPermission other) {
    this.useraction = other.useraction;
    this.groupaction = other.groupaction;
    this.otheraction = other.otheraction;
  }
  
  /**
   * Construct by given mode, either in octal or symbolic format.
   * @param mode mode as a string, either in octal or symbolic format
   * @throws IllegalArgumentException if <code>mode</code> is invalid
   */
  public FsPermission(String mode) {
    this(new UmaskParser(mode).getUMask());
  }
  
  /** Return user {@link FsAction}. */
  public FsAction getUserAction() {return useraction;}

  /** Return group {@link FsAction}. */
  public FsAction getGroupAction() {return groupaction;}

  /** Return other {@link FsAction}. */
  public FsAction getOtherAction() {return otheraction;}

  private void set(FsAction u, FsAction g, FsAction o, boolean sb) {
    useraction = u;
    groupaction = g;
    otheraction = o;
    stickyBit = sb;
  }

  public void fromShort(short n) {
    FsAction[] v = FsAction.values();

    set(v[(n >>> 6) & 7], v[(n >>> 3) & 7], v[n & 7], (((n >>> 9) & 1) == 1) );
  }

  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    out.writeShort(toShort());
  }

  /** {@inheritDoc} */
  public void readFields(DataInput in) throws IOException {
    fromShort(in.readShort());
  }

  /**
   * Create and initialize a {@link FsPermission} from {@link DataInput}.
   */
  public static FsPermission read(DataInput in) throws IOException {
    FsPermission p = new FsPermission();
    p.readFields(in);
    return p;
  }

  /**
   * Encode the object to a short.
   */
  public short toShort() {
    int s =  (stickyBit ? 1 << 9 : 0)     |
             (useraction.ordinal() << 6)  |
             (groupaction.ordinal() << 3) |
             otheraction.ordinal();

    return (short)s;
  }

  /** {@inheritDoc} */
  public boolean equals(Object obj) {
    if (obj instanceof FsPermission) {
      FsPermission that = (FsPermission)obj;
      return this.useraction == that.useraction
          && this.groupaction == that.groupaction
          && this.otheraction == that.otheraction
          && this.stickyBit == that.stickyBit;
    }
    return false;
  }

  /** {@inheritDoc} */
  public int hashCode() {return toShort();}

  /** {@inheritDoc} */
  public String toString() {
    String str = useraction.SYMBOL + groupaction.SYMBOL + otheraction.SYMBOL;
    if(stickyBit) {
      StringBuilder str2 = new StringBuilder(str);
      str2.replace(str2.length() - 1, str2.length(),
           otheraction.implies(FsAction.EXECUTE) ? "t" : "T");
      str = str2.toString();
    }

    return str;
  }

  /** Apply a umask to this permission and return a new one */
  public FsPermission applyUMask(FsPermission umask) {
    return new FsPermission(useraction.and(umask.useraction.not()),
        groupaction.and(umask.groupaction.not()),
        otheraction.and(umask.otheraction.not()));
  }

  /** umask property label */
  public static final String DEPRECATED_UMASK_LABEL = "dfs.umask"; 
  public static final String UMASK_LABEL = "dfs.umaskmode";
  public static final int DEFAULT_UMASK = 0022;

  /** 
   * Get the user file creation mask (umask)
   * 
   * {@code UMASK_LABEL} config param has umask value that is either symbolic 
   * or octal.
   * 
   * Symbolic umask is applied relative to file mode creation mask; 
   * the permission op characters '+' clears the corresponding bit in the mask, 
   * '-' sets bits in the mask.
   * 
   * Octal umask, the specified bits are set in the file mode creation mask.
   * 
   * {@code DEPRECATED_UMASK_LABEL} config param has umask value set to decimal.
   */
  public static FsPermission getUMask(Configuration conf) {
    int umask = DEFAULT_UMASK;
    
    // To ensure backward compatibility first use the deprecated key.
    // If the deprecated key is not present then check for the new key
    if(conf != null) {
      int oldStyleValue = conf.getInt(DEPRECATED_UMASK_LABEL, Integer.MIN_VALUE);
      if(oldStyleValue != Integer.MIN_VALUE) { // Property was set with old key
        LOG.warn(DEPRECATED_UMASK_LABEL + " configuration key is deprecated. " +
            "Convert to " + UMASK_LABEL + ", using octal or symbolic umask " +
            "specifications.");
        umask = oldStyleValue;
      } else {
        String confUmask = conf.get(UMASK_LABEL);
        if(confUmask != null) { // UMASK_LABEL is set
          return new FsPermission(confUmask);
        }
      }
    }
    
    return new FsPermission((short)umask);
  }

  public boolean getStickyBit() {
    return stickyBit;
  }
  
  /** Set the user file creation mask (umask) */
  public static void setUMask(Configuration conf, FsPermission umask) {
    conf.set(UMASK_LABEL, String.format("%1$03o", umask.toShort()));
  }

  /** Get the default permission. */
  public static FsPermission getDefault() {
    return new FsPermission((short)00777);
  }

  /**
   * Create a FsPermission from a Unix symbolic permission string
   * @param unixSymbolicPermission e.g. "-rw-rw-rw-"
   */
  public static FsPermission valueOf(String unixSymbolicPermission) {
    if (unixSymbolicPermission == null) {
      return null;
    }
    else if (unixSymbolicPermission.length() != 10) {
      throw new IllegalArgumentException("length != 10(unixSymbolicPermission="
          + unixSymbolicPermission + ")");
    }

    int n = 0;
    for(int i = 1; i < unixSymbolicPermission.length(); i++) {
      n = n << 1;
      char c = unixSymbolicPermission.charAt(i);
      n += (c == '-' || c == 'T' || c == 'S') ? 0: 1;
    }

    // Add sticky bit value if set
    if(unixSymbolicPermission.charAt(9) == 't' ||
        unixSymbolicPermission.charAt(9) == 'T')
      n += 01000;

    return new FsPermission((short)n);
  }
  
  private static class ImmutableFsPermission extends FsPermission {
    public ImmutableFsPermission(short permission) {
      super(permission);
    }
    public FsPermission applyUMask(FsPermission umask) {
      throw new UnsupportedOperationException();
    }
    public void readFields(DataInput in) throws IOException {
      throw new UnsupportedOperationException();
    }    
  }
}
