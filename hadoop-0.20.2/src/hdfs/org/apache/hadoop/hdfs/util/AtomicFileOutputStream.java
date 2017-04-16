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
package org.apache.hadoop.hdfs.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IOUtils;

/**
 * A FileOutputStream that has the property that it will only show
 * up at its destination once it has been entirely written and flushed
 * to disk. While being written, it will use a .tmp suffix.
 * 
 * When the output stream is closed, it is flushed, fsynced, and
 * will be moved into place, overwriting any file that already
 * exists at that location.
 * 
 * <b>NOTE</b>: on Windows platforms, it will not atomically
 * replace the target file - instead the target file is deleted
 * before this one is moved into place.
 */
public class AtomicFileOutputStream extends FilterOutputStream {

  private static final String TMP_EXTENSION = ".tmp";
  
  private final static Log LOG = LogFactory.getLog(
      AtomicFileOutputStream.class);
  
  private final File origFile;
  private final File tmpFile;
  
  public AtomicFileOutputStream(File f) throws FileNotFoundException {
    // Code unfortunately must be duplicated below since we can't assign anything
    // before calling super
    super(new FileOutputStream(new File(f.getParentFile(), f.getName() + TMP_EXTENSION)));
    origFile = f.getAbsoluteFile();
    tmpFile = new File(f.getParentFile(), f.getName() + TMP_EXTENSION).getAbsoluteFile();
  }

  @Override
  public void close() throws IOException {
    boolean triedToClose = false, success = false;
    try {
      flush();
      ((FileOutputStream)out).getChannel().force(true);

      triedToClose = true;
      super.close();
      success = true;
    } finally {
      if (success) {
        boolean renamed = tmpFile.renameTo(origFile);
        if (!renamed) {
          // On windows, renameTo does not replace.
          if (!origFile.delete() || !tmpFile.renameTo(origFile)) {
            throw new IOException("Could not rename temporary file " +
                tmpFile + " to " + origFile);
          }
        }
      } else {
        if (!triedToClose) {
          // If we failed when flushing, try to close it to not leak an FD
          IOUtils.closeStream(out);
        }
        // close wasn't successful, try to delete the tmp file
        if (!tmpFile.delete()) {
          LOG.warn("Unable to delete tmp file " + tmpFile);
        }
      }
    }
  }

}
