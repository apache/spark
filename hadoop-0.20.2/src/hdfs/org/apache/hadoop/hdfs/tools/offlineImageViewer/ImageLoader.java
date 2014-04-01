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
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * An ImageLoader can accept a DataInputStream to an Hadoop FSImage file
 * and walk over its structure using the supplied ImageVisitor.
 *
 * Each implementation of ImageLoader is designed to rapidly process an
 * image file.  As long as minor changes are made from one layout version
 * to another, it is acceptable to tweak one implementation to read the next.
 * However, if the layout version changes enough that it would make a
 * processor slow or difficult to read, another processor should be created.
 * This allows each processor to quickly read an image without getting
 * bogged down in dealing with significant differences between layout versions.
 */
interface ImageLoader {

  /**
   * @param in DataInputStream pointing to an Hadoop FSImage file
   * @param v Visit to apply to the FSImage file
   * @param enumerateBlocks Should visitor visit each of the file blocks?
   */
  public void loadImage(DataInputStream in, ImageVisitor v,
      boolean enumerateBlocks) throws IOException;

  /**
   * Can this processor handle the specified version of FSImage file?
   *
   * @param version FSImage version file
   * @return True if this instance can process the file
   */
  public boolean canLoadVersion(int version);

  /**
   * Factory for obtaining version of image loader that can read
   * a particular image format.
   */
  public class LoaderFactory {
    // Java doesn't support static methods on interfaces, which necessitates
    // this factory class

    /**
     * Find an image loader capable of interpreting the specified
     * layout version number.  If none, return null;
     *
     * @param version fsimage layout version number to be processed
     * @return ImageLoader that can interpret specified version, or null
     */
    static public ImageLoader getLoader(int version) {
      // Easy to add more image processors as they are written
      ImageLoader[] loaders = { new ImageLoaderCurrent() };

      for (ImageLoader l : loaders) {
        if (l.canLoadVersion(version))
          return l;
      }

      return null;
    }
  }
}
